from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, to_date, avg, min, max, year, month

catalog = "air_polution_analytics_dev"
silver_schema = "02_silver"
gold_schema = "03_gold"


@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.openaq_measurements_daily",
    comment="Gold materialized view that stores average daily aggregates for OpenAQ air quality measurements"
)
def daily():
    df_measurements = spark.read.table(f"{catalog}.{silver_schema}.openaq_measurements")
    df_sensors = spark.read.table(f"{catalog}.{silver_schema}.openaq_sensors")
    df_parameters = spark.read.table(f"{catalog}.{silver_schema}.openaq_parameters")
    df_locations = spark.read.table(f"{catalog}.{silver_schema}.openaq_locations")

    return (
        df_measurements.join(df_sensors, df_measurements.sensor_id == df_sensors.id)
        .join(df_parameters, df_sensors.parameter_id == df_parameters.id)
        .join(df_locations, df_measurements.location_id == df_locations.id)
        .select(
            to_date(df_measurements.datetime_from).alias("date"),
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
        .groupBy(
            "date",
            "location_id",
            "location_name",
            "country",
            "city",
            "latitude",
            "longitude",
            "parameter",
            "units"

        )
        .agg(avg("value").alias("avg_value"),
             min("value").alias("min_value"),
             max("value").alias("max_value"))
    )


@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.openaq_measurements_monthly",
    comment="Gold materialized view that stores average monthly aggregates for OpenAQ air quality measurements"
)
def monthly():
    df_measurements = spark.read.table(f"{catalog}.{gold_schema}.openaq_measurements_daily")

    return (
        df_measurements
        .select(
            year("date").alias("year"),
            month("date").alias("month"),
            "location_id",
            "location_name",
            "country",
            "city",
            "latitude",
            "longitude",
            "parameter",
            "units",
            "avg_value",
            "min_value",
            "max_value"
        )
        .groupBy(
            "year",
            "month",
            "location_id",
            "location_name",
            "country",
            "city",
            "latitude",
            "longitude",
            "parameter",
            "units"
        )
        .agg(
            avg("avg_value").alias("avg_value"),
            min("min_value").alias("min_value"),
            max("max_value").alias("max_value")
        )
    )