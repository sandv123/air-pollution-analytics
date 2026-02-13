from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, to_date, avg, min, max, trunc, lit, round

catalog = "air_polution_analytics_dev"
silver_schema = "02_silver"
gold_schema = "03_gold"


@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.agg_pollution_daily",
    comment="Gold materialized view that stores average daily aggregates for OpenAQ air quality measurements"
)
def daily():
    df_measurements = spark.read.table(f"{catalog}.{silver_schema}.fct_pollution")
    df_sensors = spark.read.table(f"{catalog}.{silver_schema}.dim_pollution_sensors")
    df_parameters = spark.read.table(f"{catalog}.{silver_schema}.dim_pollution_parameters")
    df_locations = spark.read.table(f"{catalog}.{silver_schema}.dim_pollution_locations")
    df_calendar = spark.read.table(f"{catalog}.{silver_schema}.dim_calendar")

    return (
        df_measurements.join(df_sensors, df_measurements.sensor_id == df_sensors.sensor_id)
        .join(df_parameters, df_sensors.parameter_id == df_parameters.parameter_id)
        .join(df_locations, df_measurements.location_id == df_locations.location_id)
        .join(df_calendar, df_measurements.date_int == df_calendar.date_int)
        .select(
            df_calendar.calendar_date.alias("date"),
            df_measurements.date_int.alias("date_int"),
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
            "date_int",
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
    name=f"{catalog}.{gold_schema}.agg_pollution_monthly",
    comment="Gold materialized view that stores average monthly aggregates for OpenAQ air quality measurements"
)
def monthly():
    df_measurements = spark.read.table(f"{catalog}.{gold_schema}.agg_pollution_daily")
    df_calendar = spark.read.table(f"{catalog}.{silver_schema}.dim_calendar")

    return (
        df_measurements
        .join(df_calendar, df_measurements.date_int == df_calendar.date_int)
        .select(
            trunc(df_calendar.calendar_date, "month").alias("date"),
            (round(df_measurements.date_int / lit(100)) * lit(100) + lit(1)).cast("integer").alias("date_int"),
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
            "date",
            "date_int",
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