import io
import json
import os
import time
from typing import Any
import httpcore
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
        # print(f'  page={page_num} len={len(measurements.results)}', measurements.headers)
        
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
        # print(f'TIMED OUT, sleeping {sleep_seconds} seconds, retry page {page_num}')

        raise TimeoutErrorExt(e, sleep_seconds)

    return result


def mark_processed_chunk(datastore: str, name: str, tag: str) -> None:
    with open(datastore + f'{name}.{tag}', 'w') as f:
        f.write("done")
        f.flush()


def check_processed_chunk(datastore: str, name: str, tag: str) -> bool:
    return os.path.exists(datastore + f'{name}.{tag}')


def download_chunk(config, connection_mgr, locationid, sensorid, date_from, date_to):
    client = connection_mgr.get_client()

    chunk_name = f'{locationid}_{sensorid}_{date_from}_{date_to}'
    print(f'  Downloading chunk {chunk_name}')
    if check_processed_chunk(config["datastore"], chunk_name, 'finished'):
        print(f'  Chunk {chunk_name} already downloaded, skipping')
        return
    
    timed_out = 0
    page_num = 0
    while True:
        page_num += 1
        filename = f'{chunk_name}_page{page_num}.json'

        # If a page has already been downloaded, skip it
        if os.path.exists(config["datastore"] + filename + '.zip'):
            print(f'    Page {filename}.zip exists, skipping')
            continue

        try:
            page = call_openaq_api(client, sensorid, date_from, date_to, page_num)
            timed_out = 0
            if not page:
                break

            # Store the data in zipped format. This shrinks the files about 50 times
            utils.store_zipped(config["datastore"], filename, json.dumps(page, indent=4))
        except utils.TimeoutErrorExt as e:
            if timed_out == 3:
                print(f'  Timed out (API) 3 times, skipping page {filename}')
                mark_processed_chunk(config["datastore"], filename, 'skipped_page')
                client = connection_mgr.recycle_client(0)
                timed_out += 1
                continue
            if timed_out == 5:
                print(f'  Timed out (API) 5 times, skipping chunk {chunk_name}')
                mark_processed_chunk(config["datastore"], filename, 'skipped_chunk')
                client = connection_mgr.recycle_client(0)
                timed_out += 1
                break
            if timed_out == 7:    
                # If the API timed out 7 times in a row, cancel trying and bail the whole thing
                print('  Timed out (API) 7 times, canceling')
                raise e
            else:
                # If the API timed out, try resetting the connection and wait for the API suggested amount of seconds
                time_to_sleep = e.timeout_seconds + 60 * timed_out
                print(f'TIMED OUT OPENAQ, sleeping for {time_to_sleep} seconds, {e}')
                timed_out += 1
                client = connection_mgr.recycle_client(time_to_sleep)
                page_num -= 1
        except httpcore.ReadTimeout as e:
            print(f'TIMED OUT HTTP READ, sleeping for 60 seconds, {e}')
            client = connection_mgr.recycle_client(60)
            page_num -= 1
        except TimeoutError as e:
            print(f'TIMED OUT NETWORK/IO, sleeping for 60 seconds, {e}')
            client = connection_mgr.recycle_client(60)
            page_num -= 1
    mark_processed_chunk(config["datastore"], chunk_name, 'finished')


def retrieve_historic_data(config: dict[str, Any], connection_mgr: utils.OpenAQConnectionManager):
    coordinates: dict[str, tuple[float, float]] = config["coordinates"]

    city = config['city']
    # Retrieve data for these years to analyze
    years = config["backfill_years"]

    # Retrieve measurements for these parameters of the sensors
    parameters = config["parameters"]

    for l in locations.results:
        location_name = l.name.replace(' ', '-')

        # For current location find the ids of the sensors that provide measurements for parameters we are interested in
        # If no sensor measures interesting parameter, skip the parameter
        sensors_parameters = {s.parameter.name: s.id  for s in l.sensors}
        sensor_ids = filter(lambda f: f != 0, map(lambda x: sensors_parameters.get(x, 0), parameters))
        print(f'Downloading measurements for location {location_name}')
        
        for year_and_sensor in product(years, sensor_ids):
            download_chunk(config,
                           connection_mgr,
                           l.id,
                           year_and_sensor[1],
                           f"{year_and_sensor[0]}-01-01",
                           f"{year_and_sensor[0]}-12-31")


def retrieve_recent_data(config: dict[str,Any], connection_mgr: utils.OpenAQConnectionManager):
    coordinates: tuple[float, float] = (config["latitude"], config["longitude"])

    date_from = config["date_from"]
    date_to = config["date_to"]

    # Retrieve measurements for these parameters of the sensors
    parameters = config["parameters"]

    for s in config["locations_sensors"]:
        locationid, sensorid = s.split("_")
        print(f"Downloading data for location {locationid} sensor {sensorid}, from {date_from} to {date_to}")
        
        download_chunk(config,
                       connection_mgr,
                       locationid,
                       sensorid,
                       date_from,
                       date_to)


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

    print(config)

    connection_mgr = utils.OpenAQConnectionManager(config["api_key"])
    if config["mode"] =="backfill":
        if not config.get("city") or not config.get("latitude") or not config.get("longitude") or not config.get("datastore_path"):
            raise ValueError("Parameters are required: city, latitude, longitude, datastore_path")
        retrieve_historic_data(config, connection_mgr)
    elif config["mode"] == "locations":
        if not config.get("city") or not config.get("latitude") or not config.get("longitude") or not config.get("datastore_path"):
            raise ValueError("Parameters are required: city, latitude, longitude, datastore_path")
        retrieve_locations(config, connection_mgr)
    else:
        retrieve_recent_data(config, connection_mgr)