from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, to_date, avg, min, max, year, month

catalog = "air_polution_analytics_dev"
silver_schema = "02_silver"
gold_schema = "03_gold"


@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.openaq_measurements_hourly",
    comment="Gold materialized view that stores hourly values for OpenAQ air quality measurements"
)
def hourly():
    df_measurements = spark.read.table(f"{catalog}.{silver_schema}.openaq_measurements")
    df_sensors = spark.read.table(f"{catalog}.{silver_schema}.openaq_sensors")
    df_parameters = spark.read.table(f"{catalog}.{silver_schema}.openaq_parameters")
    df_locations = spark.read.table(f"{catalog}.{silver_schema}.openaq_locations")

    return (
        df_measurements.join(df_sensors, df_measurements.sensor_id == df_sensors.id)
        .join(df_parameters, df_sensors.parameter_id == df_parameters.id)
        .join(df_locations, df_measurements.location_id == df_locations.id)
        .select(
            df_measurements.datetime_from.alias("datetime"),
            df_measurements.location_id.alias("location_id"),
            df_locations.name.alias("location_name"),
            df_locations.country,
            df_locations.city,
            df_locations.latitude,
            df_locations.longitude,
            df_parameters.name.alias("parameter"),
            df_parameters.units,
            col("value"),
        )
    )


@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.openaq_measurements_hourly_by_city",
    comment="Gold materialized view that stores hourly values for OpenAQ air quality measurements aggregated accross all locations in a city"
)
def hourly_bycity():
    df_measurements = spark.read.table(f"{catalog}.{gold_schema}.openaq_measurements_hourly")
    return (
        df_measurements
        .select(
            "datetime",
            "country",
            "city",
            "parameter",
            "units",
            col("value"),
        )
        .groupBy(
            "datetime",
            "country",
            "city",
            "parameter",
            "units"
        )
        .agg(
            avg("value").alias("avg_value"),
            min("value").alias("min_value"),
            max("value").alias("max_value")
        )
    )