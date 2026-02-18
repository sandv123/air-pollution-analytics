from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, when

catalog = "air_polution_analytics_dev"
silver_schema = "02_silver"
gold_schema = "03_gold"


@dp.materialized_view(
    name=f"{catalog}.{gold_schema}.agg_weather_hourly",
    comment="Gold materialized view that stores hourly values for openmeteo weather measurements"
)
def hourly():
    df_measurements = spark.read.table(f"{catalog}.{silver_schema}.fct_weather")
    df_locations = spark.read.table(f"{catalog}.{silver_schema}.dim_weather_locations")

    return (
        df_measurements.join(df_locations, "location_id")
        .select(
            df_measurements.date_int,
            df_measurements.time_int,
            df_measurements.location_id,
            df_locations.city,
            df_locations.latitude,
            df_locations.longitude,
            col("apparent_temperature"),
            col("precipitation"),
            col("relative_humidity_2m"),
            col("temperature_2m"),
            col("wind_direction_100m"),
            col("wind_direction_10m"),
            col("wind_gusts_10m"),
            col("wind_speed_100m"),
            col("wind_speed_10m"),
            col("surface_pressure")       
        )
        .withColumn(
            "wind_direction_bucket",
            when((col("wind_direction_10m") >= 22.5) & (col("wind_direction_10m") < 67.5), "NE")
            .when((col("wind_direction_10m") >= 67.5) & (col("wind_direction_10m") < 112.5), "E")
            .when((col("wind_direction_10m") >= 112.5) & (col("wind_direction_10m") < 157.5), "SE")
            .when((col("wind_direction_10m") >= 157.5) & (col("wind_direction_10m") < 202.5), "S")
            .when((col("wind_direction_10m") >= 202.5) & (col("wind_direction_10m") < 247.5), "SW")
            .when((col("wind_direction_10m") >= 247.5) & (col("wind_direction_10m") < 292.5), "W")
            .when((col("wind_direction_10m") >= 292.5) & (col("wind_direction_10m") < 337.5), "NW")
            .otherwise("N")
        )
    )