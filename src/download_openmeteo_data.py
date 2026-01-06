from typing import Any
import requests
from utils import utils
import json

class OpenmeteoAPI:
    api_endpoint = "https://archive-api.open-meteo.com/v1/archive"
    parameters = ["temperature_2m",
                  "relative_humidity_2m",
                  "apparent_temperature",
                  "precipitation",
                  "wind_speed_10m",
                  "wind_speed_100m",
                  "wind_direction_10m",
                  "wind_direction_100m",
                  "wind_gusts_10m",
                  "surface_pressure"]

    def __init__(self, latitude: float, longitude: float) -> None: 
        self.latitude = latitude
        self.longitude = longitude

    def api_url(self, date_from: str, date_to: str) -> str:
        url = f"{self.api_endpoint}?latitude={self.latitude}&longitude={self.longitude}"
        url += f"&start_date={date_from}&end_date={date_to}"
        url += f"&hourly={','.join(self.parameters)}"
        return url

    def get_measurements(self, date_from: str, date_to: str) -> dict[str, Any]:
        result = {}
        uri = self.api_url(date_from, date_to)
        response = requests.get(uri)
        if response.status_code == 200:
            result = json.loads(response.content)

        return result
        

def main(config):
    filename = f'openmeteo_{config["city"]}_{config["date_from"]}_{config["date_to"]}.json'
    if utils.file_downloaded(config, filename):
        print(f"file {filename} already dowloaded, skipping")
        return
    
    api = OpenmeteoAPI(config["latitude"], config["longitude"])
    
    res = api.get_measurements(config["date_from"], config["date_to"])

    utils.store_file(config["datastore_path"], filename, json.dumps(res, indent=4), "gzip")

if __name__ == "__main__":
    if not utils.is_running_in_databricks():
        dbutils = None
    config = utils.setup_environment(dbutils) # type: ignore

    main(config)