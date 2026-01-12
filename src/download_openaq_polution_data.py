import io
import json
import os
import time
from typing import Any
import httpcore
import httpx 
import openaq
import zipfile
from itertools import product
from argparse import ArgumentParser

from utils import utils




def call_openaq_api(client: openaq.OpenAQ, sensor_id: int, date_from: str, date_to: str, page_num: int) -> dict[str, Any]:
    """
    Run a single call to the OpenAQ API to get measurements for a particular parameter for the whole year.
    The service uses paging API so the method retrieves a single page of the results.
    Returns a parsed JSON for the API response.
    
    :param client: OpenAQ API client
    :type client: openaq.OpenAQ
    :param sensor_id: Id of the sensor to query
    :type sensor_id: int
    :param date_from: Start of the retrieval period
    :type date_from: str
    :param date_to: End of the retrieval period
    :type date_to: str
    :param page_num: Result page to retrieve
    :type page_num: int
    :return: Parsed JSON response
    :rtype: dict[str, Any]
    """
    result = {}
    
    limit = 1000
    sleep_seconds = 0

    # Initialize measurements variable
    measurements = None

    try:
        # Call the OpenAQ API
        measurements = client.measurements.list(
            sensors_id=int(sensor_id), 
            datetime_from=date_from, 
            datetime_to=date_to, 
            limit=limit, 
            page=page_num
        )
        print(f'    page={page_num} len={len(measurements.results)}', measurements.headers)
        
        # Pages are retrieved until current page returns zero results
        if not len(measurements.results) == 0:
            result = json.loads(measurements.json())
        
        # OpenAQ limits the rate at which the API can be called
        # This is to throttle the requests if close to rate limit
        if measurements.headers.x_ratelimit_remaining is not None and measurements.headers.x_ratelimit_remaining < 5:
            raise openaq.TimeoutError('Timing out to observe rate limits')
        
    except openaq.TimeoutError as e:
        # If timed out or throttling, sleep until the rate limit resets or 60 seconds
        # Raise exception to pass the info up the callstack
        if measurements is not None:
            if measurements.headers.x_ratelimit_reset is None:
                sleep_seconds = 60
            else:
                sleep_seconds = float(measurements.headers.x_ratelimit_reset)
        print(f'TIMED OUT, sleeping {sleep_seconds} seconds, retry page {page_num}')

        raise utils.TimeoutErrorExt(e, sleep_seconds)

    return result


def mark_processed_chunk(datastore: str, name: str, tag: str) -> None:
    """
    Mark a data chunk as processed by creating a marker file.
    
    Creates a marker file in the specified datastore directory with a given name
    and tag to indicate that a chunk has been successfully processed.
    
    Args:
        datastore (str): The directory path where the marker file will be created.
        name (str): The name of the data chunk file.
        tag (str): The file extension/tag for the marker file.
    
    Returns:
        None
    
    Example:
        >>> mark_processed_chunk('/data/store', 'chunk_001', 'done')
        # Creates file: /data/store/chunk_001.done
    """
    with open(os.path.join(datastore, f'{name}.{tag}'), 'w') as f:
        f.write("done")
        f.flush()


def check_processed_chunk(datastore: str, name: str, tag: str) -> bool:
    """
    Check if a data chunk has been processed and exists in the datastore.

    Uses tags to identify processed chunks. Tags are set by mark_processed_chunk()

    Args:
        datastore (str): The path to the datastore directory where processed chunks are stored.
        name (str): The name of the data chunk file to check for.
        tag (str): The file extension/tag identifying whether the chunk has been processed.

    Returns:
        bool: True if the processed chunk file exists, False otherwise.
    """
    return os.path.exists(os.path.join(datastore, f'{name}.{tag}'))


def download_chunk(config, connection_mgr, locationid, location_name, sensorid, date_from, date_to):
    """
    Download air pollution data from OpenAQ API for a specific location and sensor within a date range.
    Fetches data in paginated chunks and stores results in gzipped JSON format. Implements retry logic
    with exponential backoff for API timeouts and network errors. Skips already downloaded pages and
    chunks to support resumable downloads.
    Args:
        config (dict): Configuration dictionary containing 'datastore_path' key for data storage location.
        connection_mgr: Connection manager instance for managing API client lifecycle and recycling.
        locationid (int): Unique identifier for the location to download data from.
        location_name (str): Human-readable name of the location (spaces will be replaced with hyphens).
        sensorid (int): Unique identifier for the sensor at the location.
        date_from (str): Start date for the data range to download (format: YYYY-MM-DD).
        date_to (str): End date for the data range to download (format: YYYY-MM-DD).
    Returns:
        None
    Raises:
        utils.TimeoutErrorExt: Raised after 7 consecutive API timeout attempts without success.
    Side Effects:
        - Downloads JSON pages from OpenAQ API and stores them in gzipped format.
        - Marks processed chunks/pages as 'finished', 'skipped_page', or 'skipped_chunk'.
        - Recycled API client connection on timeout with adaptive sleep intervals.
        - Prints status messages for download progress and error handling.
    """
    client = connection_mgr.get_client()

    location_name = location_name.replace(' ', '-')
    chunk_name = f'{locationid}_{sensorid}_{location_name}_{date_from}_{date_to}'
    print(f'  Downloading chunk {chunk_name}')
    if check_processed_chunk(config["datastore_path"], chunk_name, 'finished'):
        print(f'  Chunk {chunk_name} already downloaded, skipping')
        return
    
    timed_out = 0
    page_num = 0
    while True:
        page_num += 1
        filename = f'{chunk_name}_page{page_num}.json'

        # If a page has already been downloaded, skip it
        if utils.file_downloaded(config, filename):
            print(f'    Page {filename} exists, skipping')
            continue

        try:
            page = call_openaq_api(client, sensorid, date_from, date_to, page_num)
            
            # Reset timeout count after a successful API call
            timed_out = 0
            if not page:
                break

            # Store the data in gzipped format. This shrinks the files about 50 times
            utils.store_file(config["datastore_path"], filename, json.dumps(page, indent=4), compression="gzip")
        except utils.TimeoutErrorExt as e:
            if timed_out == 3:
                print(f'  Timed out (API) 3 times, skipping page {filename}')
                mark_processed_chunk(config["datastore_path"], filename, 'skipped_page')
                client = connection_mgr.recycle_client(0)
                timed_out += 1
                continue
            if timed_out == 5:
                print(f'  Timed out (API) 5 times, skipping chunk {chunk_name}')
                mark_processed_chunk(config["datastore_path"], filename, 'skipped_chunk')
                client = connection_mgr.recycle_client(0)
                timed_out += 1
                break
            if timed_out == 7:    
                # If the API timed out 7 times in a row, cancel trying and bail the whole thing
                print('  Timed out (API) 7 times, canceling')
                raise e
            else:
                # If the API timed out, try resetting the connection and wait for the API-suggested amount of seconds and some
                time_to_sleep = e.timeout_seconds + 60 * timed_out
                print(f'TIMED OUT OPENAQ, sleeping for {time_to_sleep} seconds, {e}')
                timed_out += 1
                client = connection_mgr.recycle_client(time_to_sleep)
                page_num -= 1
        except httpx.TimeoutException as e:
            print(f'TIMED OUT HTTP, sleeping for 60 seconds, {e}')
            client = connection_mgr.recycle_client(60)
            page_num -= 1
        except httpcore.ReadTimeout as e:
            print(f'TIMED OUT HTTP READ, sleeping for 60 seconds, {e}')
            client = connection_mgr.recycle_client(60)
            page_num -= 1
        except TimeoutError as e:
            print(f'TIMED OUT NETWORK/IO, sleeping for 60 seconds, {e}')
            client = connection_mgr.recycle_client(60)
            page_num -= 1
    mark_processed_chunk(config["datastore_path"], chunk_name, 'finished')


def retrieve_single_location_measurements(config: dict[str, Any], connection_mgr: utils.OpenAQConnectionManager):
    """
    Retrieve air pollution measurements for a single location across multiple sensors and time periods.
    Splits the specified date range into periods and downloads measurement data for each combination
    of sensor and time period from the OpenAQ API.
    Args:
        config (dict[str, Any]): Configuration dictionary containing:
            - date_from (str): Start date for the measurement period.
            - date_to (str): End date for the measurement period.
            - sensors (list): List of sensor identifiers to retrieve data for.
            - location_id (str): The ID of the location.
            - location_name (str): The name of the location.
        connection_mgr (utils.OpenAQConnectionManager): Connection manager for OpenAQ API requests.
    Returns:
        None
    """
    date_from = config["date_from"]
    date_to = config["date_to"]

    periods = utils.split_time_period(date_from, date_to)
    # print(f'periods={periods}')
    for (sensor, date) in product(config["sensors"], periods):
        # print(f"  sensor {sensor}, date={date}")
        download_chunk(config,
                        connection_mgr,
                        config["location_id"],
                        config["location_name"],
                        sensor,
                        date[0],
                        date[1])


def retrieve_locations(config: dict[str,Any], connection_mgr: utils.OpenAQConnectionManager):
    """
    Retrieve air quality monitoring locations within a specified radius and store them.
    Fetches all available locations from the OpenAQ API within an 8 km radius of the 
    coordinates specified in the config, then saves the results to a compressed JSON file.
    Args:
        config (dict[str, Any]): Configuration dictionary containing:
            - latitude (float): Latitude coordinate of the center point
            - longitude (float): Longitude coordinate of the center point
            - city (str): Name of the city for naming the output file
            - datastore_path (str): Path where the locations file will be stored
        connection_mgr (utils.OpenAQConnectionManager): Connection manager instance 
            for accessing the OpenAQ API client
    Returns:
        None
    Side Effects:
        - Stores a gzip-compressed JSON file containing the location data
        - Prints the number of locations retrieved to stdout
    """
    coordinates: tuple[float, float] = (config["latitude"], config["longitude"])
    city: str = config["city"]

    # Find all available locations in the radius of 8 km around Belgrade center
    locations = connection_mgr.get_client().locations.list(coordinates=coordinates, radius=8000, limit=1000)

    # Store the locations for processing
    utils.store_file(
        config["datastore_path"],
        f'{city}-locations.json',
        json.dumps(json.loads(locations.json()), indent=4),
        compression="gzip")
    print(f'Got {len(locations.results)} locations')


if __name__ == "__main__":
    if not utils.is_running_in_databricks():
        dbutils = None
    config = utils.setup_environment(dbutils) # type: ignore

    connection_mgr = utils.OpenAQConnectionManager(config["api_key"])
    if config["mode"] =="measurements":
        if (not config.get("location_id") 
            or not config.get("location_name") 
            or not config.get("sensors")
            or not config.get("date_from")
            or not config.get("datastore_path")
        ):
            raise ValueError("Parameters are required: location_id, date_from, datastore_path")
        retrieve_single_location_measurements(config, connection_mgr)
    elif config["mode"] == "locations":
        if not config.get("city") or not config.get("latitude") or not config.get("longitude") or not config.get("datastore_path"):
            raise ValueError("Parameters are required: city, latitude, longitude, datastore_path")
        retrieve_locations(config, connection_mgr)
    else:
        print(f"config={config}")