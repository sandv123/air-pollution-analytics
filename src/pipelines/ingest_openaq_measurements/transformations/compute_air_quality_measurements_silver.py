from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, split

catalog = "air_polution_analytics_dev"
bronze_schema = "01_bronze"
silver_schema = "02_silver"
filename_re = r"^(d+)_(d+)_(D+)_(.+)_"

@dp.table(
    name=f"{catalog}.{silver_schema}.air_quality_measurements",
    comment="OpenAQ Air quality measurements"
)
@dp.expect("period interval is 1 hour", "interval = '01:00:00'")
@dp.expect("datetime_from is not null", "datetime_from IS NOT NULL")
@dp.expect("value is not null", "value IS NOT NULL")
@dp.expect("location_id is not null", "location_id IS NOT NULL")
@dp.expect("sensor_id is not null", "sensor_id IS NOT NULL")
def compute_air_quality_measurements_silver():
    df = (
        spark.readStream.table(f"{catalog}.{bronze_schema}.air_quality_measurements")
            .select(col("results.period.datetimeFrom.utc").alias("datetime_from"),
                    col("results.period.datetimeTo.utc").alias("datetime_to"),
                    col("results.value"),
                    col("results.coordinates"),
                    col("source_file_name"),
                    col("results.period.interval").alias("interval"),)
    )
    split_col = split(df.source_file_name, "_")
    df = (df.withColumn("location_id", split_col.getItem(0))
            .withColumn("sensor_id", split_col.getItem(1))
    )

    return df


@dp.materialized_view(
    name=f"{catalog}.{silver_schema}.locations",
    comment="Air quality locations",
)
def locations():
    locations_df = spark.read.table(f"{catalog}.{bronze_schema}.locations")
    return (
        locations_df.select(
                    col("results.id").alias("id"),
                    col("results.name").alias("name"),
                    col("results.locality").alias("locality"),
                    col("results.country.code").alias("country"),
                    col("results.isMobile").alias("isMobile"),
                    col("results.isMonitor").alias("isMonitor"),
                    col("results.coordinates.latitude").alias("latitude"),
                    col("results.coordinates.longitude").alias("longitude")
        )       
    )


@dp.temporary_view
def raw_sensors():
    locations_df = spark.read.table(f"{catalog}.{bronze_schema}.locations")
    sensors_df = locations_df.selectExpr("results.id as id", "explode(results.sensors) as sensors")
    return(
        sensors_df.select(
            col("sensors.id").alias("id"),
            col("sensors.name").alias("name"),
            col("sensors.parameter").alias("parameter")
        )
    )


@dp.materialized_view(
    name=f"{catalog}.{silver_schema}.sensors",
    comment="OpenAQ Sensor information",
)
def sensors():
    return (
        spark.read.table("raw_sensors").select(
            col("id"),
            col("name")
        )
    )


@dp.materialized_view(
    name=f"{catalog}.{silver_schema}.parameters",
    comment="OpenAQ Sensor Parameters information",
)
def parameters():
    return (
        spark.read.table("raw_sensors").select(
            col("parameter.id").alias("id"),
            col("parameter.displayName").alias("display_name"),
            col("parameter.name").alias("name"),
            col("parameter.units").alias("units")
        ).distinct()

        # spark.read.table("raw_sensors").selectExpr(
        #     "distinct parameter.id as id",
        #     "parameter.displayName as display_name",
        #     "parameter.name as name",
        #     "parameter.units as units"
        # )
    )