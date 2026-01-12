from typing import Any
import requests
from utils import utils
import json

class OpenmeteoAPI:
    """
    A client for interacting with the Open-Meteo Archive API to retrieve historical weather data.
    This class provides methods to construct API requests and fetch meteorological measurements
    for a specified geographic location and time period.
    Attributes:
        api_endpoint (str): The base URL for the Open-Meteo Archive API.
        parameters (list[str]): List of weather parameters to retrieve, including temperature,
            humidity, precipitation, and wind measurements.
        latitude (float): The latitude coordinate of the location for which to retrieve data.
        longitude (float): The longitude coordinate of the location for which to retrieve data.
    Example:
        >>> api = OpenmeteoAPI(latitude=52.52, longitude=13.405)
        >>> data = api.get_measurements("2023-01-01", "2023-01-31")
    """
        
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
        """
        Initialize the OpenmeteoAPI client with geographic coordinates.
        Args:
            latitude (float): The latitude of the target location.
            longitude (float): The longitude of the target location.
        """
        self.latitude = latitude
        self.longitude = longitude

    def api_url(self, date_from: str, date_to: str) -> str:
        """
        Construct the complete API request URL with parameters.
        Args:
            date_from (str): Start date in 'YYYY-MM-DD' format.
            date_to (str): End date in 'YYYY-MM-DD' format.
        Returns:
            str: The complete API endpoint URL with query parameters.
        """
        url = f"{self.api_endpoint}?latitude={self.latitude}&longitude={self.longitude}"
        url += f"&start_date={date_from}&end_date={date_to}"
        url += f"&hourly={','.join(self.parameters)}"
        return url

    def get_measurements(self, date_from: str, date_to: str) -> dict[str, Any]:
        """
        Fetch historical weather measurements for the specified date range.
        Args:
            date_from (str): Start date in 'YYYY-MM-DD' format.
            date_to (str): End date in 'YYYY-MM-DD' format.
        Returns:
            dict[str, Any]: A dictionary containing the API response with weather data,
                or an empty dictionary if the request fails.
        """
        result = {}
        uri = self.api_url(date_from, date_to)
        response = requests.get(uri)
        if response.status_code == 200:
            result = json.loads(response.content)

        return result
        

def main(config):
    filename = f'openmeteo_{config["city"]}_{config["date_from"]}_{config["date_to"]}.json'

    # Skip if file already downloaded
    if utils.file_downloaded(config, filename):
        print(f"file {filename} already dowloaded, skipping")
        return
    
    # Call API
    api = OpenmeteoAPI(config["latitude"], config["longitude"])
    res = api.get_measurements(config["date_from"], config["date_to"])

    # Store result
    utils.store_file(config["datastore_path"], filename, json.dumps(res, indent=4), "gzip")

if __name__ == "__main__":
    if not utils.is_running_in_databricks():
        dbutils = None
    config = utils.setup_environment(dbutils) # type: ignore

    main(config)