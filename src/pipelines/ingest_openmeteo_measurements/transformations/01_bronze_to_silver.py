from pyspark.sql.types import ArrayType, IntegerType
from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, sha2, concat_ws, to_timestamp

catalog = "air_polution_analytics_dev"
bronze_schema = "01_bronze"
silver_schema = "02_silver"


# In future same logic might be used for locations other than Belgrade, RS
# Thus generating a location ID I can use to store the data
@dp.temporary_view
def openmeteo_temp():
    df = spark.readStream.table(f"{catalog}.{bronze_schema}.temperature_measurements")
    return (
        df.select(
            "elevation",
            "generationtime_ms",
            "hourly",
            "hourly_units",
            "latitude",
            "longitude",
            "timezone",
            "timezone_abbreviation",
            "utc_offset_seconds")
        .withColumn("location_id", sha2(concat_ws('|', col("latitude"), col("longitude")), 256))
    )


@dp.table(
    name = f"{catalog}.{silver_schema}.temperature_locations",
    comment = "Open-meteo Locations Information"
)
def temperature_locations():
    df = spark.readStream.table("openmeteo_temp")
    return (
        df.select(
            "location_id",
            "elevation",
            "generationtime_ms",
            "hourly_units",
            "latitude",
            "longitude",
            "timezone",
            "timezone_abbreviation",
            "utc_offset_seconds")
    )


@dp.table(
    name = f"{catalog}.{silver_schema}.temperature_measurements",
    comment = "Open-meteo Temperature Measurements"
)
def temperature_measurements():
    df = spark.readStream.table("openmeteo_temp")
    return (
        df.selectExpr(
            "location_id",
            """
            explode(
                arrays_zip(
                    hourly.time,
                    hourly.apparent_temperature,
                    hourly.precipitation,
                    hourly.relative_humidity_2m,
                    hourly.temperature_2m,
                    hourly.wind_direction_100m,
                    hourly.wind_direction_10m,
                    hourly.wind_gusts_10m,
                    hourly.wind_speed_100m,
                    hourly.wind_speed_10m
                )
            ) as measurements
            """
        )
        .selectExpr(
            "location_id",
            "to_timestamp(measurements.time) as datetime",
            "float(measurements.apparent_temperature)",
            "float(measurements.precipitation)",
            "int(measurements.relative_humidity_2m)",
            "float(measurements.temperature_2m)",
            "int(measurements.wind_direction_100m)",
            "int(measurements.wind_direction_10m)",
            "float(measurements.wind_gusts_10m)",
            "float(measurements.wind_speed_100m)",
            "float(measurements.wind_speed_10m)"
        )
    )