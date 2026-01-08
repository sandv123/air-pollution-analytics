from pyspark.sql.types import ArrayType, IntegerType
from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, sha2, concat_ws, to_timestamp, split

catalog = "air_polution_analytics_dev"
source_schema = "01_bronze"
target_schema = "01_bronze"


# In future same logic might be used for locations other than Belgrade, RS
# Thus generating a location ID I can use to store the data
@dp.temporary_view
def openmeteo_temp():
    df = spark.readStream.table(f"{catalog}.{source_schema}.temperature_measurements_raw")

    split_col = split(df.source_file_name, "_")
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
            "utc_offset_seconds",
            "source_file_name")
        .withColumn("city", split_col.getItem(1))
        .withColumn("location_id", sha2(concat_ws('|', col("latitude"), col("longitude")), 256))
        .drop("source_file_name")
    )


@dp.table(
    name = f"{catalog}.{target_schema}.temperature_measurements",
    comment = "Open-meteo Temperature Measurements"
)
@dp.expect_all_or_drop({
                         "location_id is not null": "location_id is not null",
                         "datetime is not null": "datetime is not null",
                         "temperature_2m is not null": "temperature_2m IS NOT NULL",
                         "surface_pressure is not null": "surface_pressure IS NOT NULL"
 })
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
                    hourly.wind_speed_10m,
                    hourly.surface_pressure
                )
            ) as measurements
            """
        )
        .selectExpr(
            "location_id",
            "to_timestamp(measurements.time) as datetime",
            "float(measurements.apparent_temperature) as apparent_temperature",
            "float(measurements.precipitation) as precipitation",
            "int(measurements.relative_humidity_2m) as relative_humidity_2m",
            "float(measurements.temperature_2m) as temperature_2m",
            "int(measurements.wind_direction_100m) as wind_direction_100m",
            "int(measurements.wind_direction_10m) as wind_direction_10m",
            "float(measurements.wind_gusts_10m) as wind_gusts_10m",
            "float(measurements.wind_speed_100m) as wind_speed_100m",
            "float(measurements.wind_speed_10m) as wind_speed_10m",
            "float(measurements.surface_pressure) as surface_pressure"
        )
    )