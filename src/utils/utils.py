import os
from argparse import ArgumentParser
from typing import Any
# import zipfile
import io
import openaq
import time



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

    
    def __del(self):
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
    # OpenAQ API key
    # if is_running_in_databricks():
    if dbutils is not None:
        print("Running inside Databricks")
        api_key = dbutils.secrets.get(scope = "air-polution-analytics APIs keys", key = "OPENAQ_API_KEY") # type: ignore
    else:
        print("Running inside IDE")
        api_key = os.environ['OPENAQ_API_KEY']
        
    argparse = ArgumentParser()
    argparse.add_argument("--mode", choices=['locations', 'backfill', 'iterative'], required=True)

    # Mode=locations
    argparse.add_argument("--latitude", type=float)
    argparse.add_argument("--longitude", type=float)
    argparse.add_argument("--city",  type=str)

    # Mode=backfill
    argparse.add_argument("--backfill_years", nargs="+")
    
    # Mode=iterative & common args
    argparse.add_argument("--location", type=int)
    argparse.add_argument("--location_name", type=str)
    argparse.add_argument("--sensors", nargs="+")
    argparse.add_argument("--date_from", type=str)
    argparse.add_argument("--date_to", type=str)
    argparse.add_argument("--datastore_path", type=str)

    args = argparse.parse_args()

    result = args.__dict__
    result["parameters"] = ['pm1', 'pm10', 'pm25', 'temperature']
    result["api_key"] = api_key

    return result


def store_file(datastore:str, filename: str, data: str, compression: str = "zip"):
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


if __name__ == "__main__":
    pass