import glob
import json
import os
from argparse import ArgumentParser
from typing import Any
# import zipfile
import io
import openaq
import time
from datetime import datetime, timedelta, date



class TimeoutErrorExt(openaq.TimeoutError):
    """
    Extension to openaq.TimeoutError to push the expected timeout in seconds up the callstack
    """
    def __init__(self, e: openaq.TimeoutError, timeout_seconds: float):
        self.status_code = e.status_code
        super().__init__(str(e))
        self.timeout_seconds = timeout_seconds


class OpenAQConnectionManager:
    """
    Wrapper around OpenAQ class to easily restart the connection in case of an endless annoying service timeout loop
    """
    client: openaq.OpenAQ | None = None

    def __init__(self, api_key: str):
        self._api_key = api_key

    
    def __del__(self):
        if self.client is not None:
            self.client.close()


    def get_client(self):
        if self.client is None:
            self.client = openaq.OpenAQ(self._api_key)
        return self.client

    def recycle_client(self, timeout: float) -> openaq.OpenAQ:
        """
        Get a client object. If exists, cycle it by closing, sleep for {timeout} seconds, and opening anew

        :return: OpenAQ client object
        :rtype: openaq.OpenAQ
        """
        if self.client is not None:
            self.client.close()
            self.client = None
            time.sleep(timeout)
        
        return self.get_client()
    

def is_running_in_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def setup_environment(dbutils) -> dict[str, Any]:
    """
    Initialize and configure the environment for air pollution analytics.
    Retrieves API credentials from Databricks secrets or environment variables,
    parses command-line arguments for data fetching mode and parameters, and
    constructs a configuration dictionary.
    Args:
        dbutils: Databricks utilities object for secrets retrieval. If None,
                 assumes running in a local IDE environment.
    Returns:
        dict[str, Any]: Configuration dictionary containing:
            - mode: (str) - Execution mode ('locations', 'measurements', or 'test')
            - latitude: (float) - Latitude coordinate for location search (optional)
            - longitude: (float) - Longitude coordinate for location search (optional)
            - city: (str) - City name for location search (optional)
            - location_id: (int) - Location ID for measurements query (optional)
            - location_name: (str) - Location name for measurements query (optional)
            - sensors: (list[str]) - List of sensor IDs to query (optional)
            - date_from: (str) - Start date for measurements in 'YYYY-MM-DD' format (optional)
            - date_to: (str) - End date for measurements in 'YYYY-MM-DD' format
                       (defaults to yesterday if not provided)
            - datastore_path: (str) - Path for data storage (optional)
            - parameters: (list[str]) - Air quality parameters to fetch
                         (['pm1', 'pm10', 'pm25', 'temperature'])
            - api_key: (str) - OpenAQ API key for authentication
    Note:
        If date_to is not provided, it defaults to yesterday's date to ensure
        complete daily data availability.
    """
    # OpenAQ API key
    if dbutils is not None:
        print("Running inside Databricks")
        api_key = dbutils.secrets.get(scope = "air-polution-analytics APIs keys", key = "OPENAQ_API_KEY") # type: ignore
    else:
        print("Running inside IDE")
        api_key = os.environ['OPENAQ_API_KEY']
        
    argparse = ArgumentParser()
    argparse.add_argument("--mode", choices=['locations', 'measurements', 'test'], default='test')

    # Mode=locations
    argparse.add_argument("--latitude", type=float)
    argparse.add_argument("--longitude", type=float)
    argparse.add_argument("--city",  type=str)

    # Mode=measurements
    argparse.add_argument("--location_id", type=int)
    argparse.add_argument("--location_name", type=str)
    argparse.add_argument("--sensors", nargs="+")
    argparse.add_argument("--date_from", type=str)
    argparse.add_argument("--date_to", type=str)

    # Common args
    argparse.add_argument("--datastore_path", type=str)

    args = argparse.parse_args()

    result = args.__dict__

    # If date_to is not provided (e.g. when incrementally run daily), assume
    # date_to=yesterday to make sure there is data available for the whole day
    if result.get("date_to") is None:
        result["date_to"] = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    sensors = str(result.get("sensors"))
    if sensors and sensors[0].startswith("["):
        result["sensors"] = ["".join(filter(str.isnumeric, s)) for s in sensors[1:-1].split(",")]

    result["parameters"] = ['pm1', 'pm10', 'pm25', 'temperature']
    result["api_key"] = api_key

    return result


def store_file(datastore:str, filename: str, data: str, compression: str = "gzip") -> None:
    """
    Store data in the datastore a file with optional compression. Can use gzip
    and zip or store the data uncompressed.
    
    :param datastore: path to a directory, where the file will be stored
    :type datastore: str
    :param filename: name of the file, if compression is used gz or zip suffix is automatically added
    :type filename: str
    :param data: the data to store
    :type data: str
    :param compression: compression method, can be gzip, zip or none
    :type compression: str
    """
    content_to_store: io.BytesIO
    if compression == "zip":
        import zipfile
        content_to_store = io.BytesIO()
        with zipfile.ZipFile(content_to_store, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as file:
            file.writestr(filename, data)
        filename = filename + ".zip"
    elif compression == "gzip":
        import gzip
        content_to_store = io.BytesIO(gzip.compress(data.encode("utf-8"), compresslevel=9))
        filename = filename + ".gz"
    elif compression == "none":
        content_to_store = io.BytesIO(data.encode("utf-8"))
    else:
        raise io.UnsupportedOperation(f"Unsupported compression mechanism '{compression}'. Supported: 'zip', 'gzip', 'none'")

    with open(datastore + "/" + filename, "wb") as f:
        f.write(content_to_store.getvalue())


def split_time_period(date_from_str: str, date_to_str: str) -> list[tuple[str, str]]:
    """
    Split a time period between date_from_str and date_to_str into chunks with
    fixed boundaries (thresholds). This helps with idempotency of the pipeline
    as the chunks, which have already been downloaded, won't be requested again
    
    :param date_from_str: start of the period, format - yyyy-dd-mm
    :type date_from_str: str
    :param date_to_str: end od the period, format - yyyy-dd-mm
    :type date_to_str: str
    :return: list of tuples with start and end of the chunks
    :rtype: list[tuple[str, str]]
    """
    date_from = datetime.strptime(date_from_str, '%Y-%m-%d').date()
    date_to = datetime.strptime(date_to_str, '%Y-%m-%d').date()

    thresholds = (
        (3, 31),
        (6, 30),
        (9, 30),
        (12, 31)
    )
    from itertools import product, pairwise
    periods = [date_from - timedelta(days=1)]
    for date_str in product(range(date_from.year, date_to.year+1), thresholds):
        threshold_date = date(date_str[0], date_str[1][0], date_str[1][1])
        if(threshold_date <= date_from):
            continue
        if(threshold_date > date_to):
            break
        periods.append(threshold_date)
    if(periods[-1] != date_to):
        periods.append(date_to)

    result = [((left + timedelta(days=1)).strftime('%Y-%m-%d'), right.strftime('%Y-%m-%d')) for left, right in pairwise(periods)]
    return result


def file_downloaded(config, filename):
    """
    Check if file has already been downloaded
    
    :param config: dictionary with all config options
    :param filename: file to check
    """
    filename = config["datastore_path"] + filename 
    return os.path.exists(filename) or os.path.exists(filename+ '.zip') or os.path.exists(filename+ '.gz')


if __name__ == "__main__":
    if not is_running_in_databricks():
        dbutils = None
    config = setup_environment(dbutils)

    for p in split_time_period(config["date_from"], config["date_to"]):
        print(p)