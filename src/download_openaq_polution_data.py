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
    :type year: str
    :param date_to: End of the retrieval period
    :type year: str
    :param page_num: Result page to retrieve
    :type page_num: int
    :return: Parsed JSON response
    :rtype: dict[str, Any]
    """
    result = {}
    
    limit = 1000
    do_loop = True
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
    with open(datastore + f'{name}.{tag}', 'w') as f:
        f.write("done")
        f.flush()


def check_processed_chunk(datastore: str, name: str, tag: str) -> bool:
    return os.path.exists(datastore + f'{name}.{tag}')


def download_chunk(config, connection_mgr, locationid, location_name, sensorid, date_from, date_to):
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
    date_from = config["date_from"]
    date_to = config["date_to"]

    periods = utils.split_time_period(date_from, date_to, config["period_weeks"])
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
            or not config.get("period_weeks")
            or not config.get("date_from")
            or not config.get("date_to")
            or not config.get("datastore_path")
        ):
            raise ValueError("Parameters are required: location_id, period_weeks, date_from, date_to, longitude, datastore_path")
        retrieve_single_location_measurements(config, connection_mgr)
    elif config["mode"] == "locations":
        if not config.get("city") or not config.get("latitude") or not config.get("longitude") or not config.get("datastore_path"):
            raise ValueError("Parameters are required: city, latitude, longitude, datastore_path")
        retrieve_locations(config, connection_mgr)
    else:
        print(f"config={config}")