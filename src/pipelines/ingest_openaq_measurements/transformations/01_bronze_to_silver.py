from pyspark.sql.types import ArrayType, IntegerType
from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, split, collect_set

catalog = "air_polution_analytics_dev"
bronze_schema = "01_bronze"
silver_schema = "02_silver"


@dp.table(
    name=f"{catalog}.{silver_schema}.air_quality_measurements",
    comment="Cleaned OpenAQ Air quality measurements"
    # Databricks recommends to avoid partitioning tables less that 1Tb in size,
    # thus no partitioning is enabled
)
@dp.expect_all_or_drop({
                        "period interval is 1 hour": "interval = '01:00:00'",
                        "datetime_from is not null": "datetime_from IS NOT NULL",
                        "value is not null": "value IS NOT NULL",
                        "location_id is not null": "location_id IS NOT NULL",
                        "sensor_id is not null": "sensor_id IS NOT NULL"
})
def compute_air_quality_measurements_silver():
    df = (
        spark.readStream.table(f"{catalog}.{bronze_schema}.air_quality_measurements")
            .select(col("results.period.datetimeFrom.utc").alias("datetime_from"),
                    col("results.period.datetimeTo.utc").alias("datetime_to"),
                    col("results.value").alias("value").cast("float"),
                    col("source_file_name"),
                    col("results.period.interval").alias("interval"))
    )
    split_col = split(df.source_file_name, "_")
    df = (df.withColumn("location_id", split_col.getItem(0).cast("int"))
            .withColumn("sensor_id", split_col.getItem(1).cast("int"))
            .drop(col("source_file_name"))
    )

    return df


@dp.temporary_view
def locations_temp():
    locations_df = spark.readStream.table(f"{catalog}.{bronze_schema}.locations")
    return (
        locations_df.select(
                    col("results.id").alias("id").cast("int"),
                    col("results.name").alias("name"),
                    col("results.locality").alias("locality"),
                    col("results.country.code").alias("country"),
                    col("results.isMobile").alias("is_mobile"),
                    col("results.isMonitor").alias("is_monitor"),
                    col("results.coordinates.latitude").alias("latitude").cast("float"),
                    col("results.coordinates.longitude").alias("longitude").cast("float"),
                    col("results.sensors").alias("sensors")
        )       
    )


@dp.table(
    name=f"{catalog}.{silver_schema}.locations",
    comment="Cleaned Air quality locations",
)
@dp.expect_all_or_drop({
    "id is not null": "id IS NOT NULL",
    "name is not null": "name IS NOT NULL"
})
def locations():
    locations_df = spark.readStream.table("locations_temp")
    return (
        locations_df.select(
            col("id"),
            col("name"),
            col("locality"),
            col("country"),
            col("is_mobile"),
            col("is_monitor"),
            col("latitude"),
            col("longitude"),
            col("sensors.id").alias("sensor_id_arr").cast(ArrayType(IntegerType()))
        )
    )


@dp.temporary_view
def sensors_temp():
    locations_df = spark.readStream.table("locations_temp")
    sensors_df = locations_df.selectExpr("id", "explode(sensors) as sensors")
    return(
        sensors_df.select(
            col("sensors.id").alias("id"),
            col("sensors.name").alias("name"),
            col("sensors.parameter").alias("parameter")
        )
    )


@dp.table(
    name=f"{catalog}.{silver_schema}.sensors",
    comment="Cleaned OpenAQ Sensor information",
)
@dp.expect_all_or_drop({
    "id is not null": "id IS NOT NULL"
})
def sensors():
    return (
        spark.readStream.table("sensors_temp")
            .select(
                col("id").cast("int"),
                col("name"),
                col("parameter.id").alias("parameter_id").cast("int")
            )
    )


@dp.table(
    name=f"{catalog}.{silver_schema}.parameters",
    comment="Cleaned OpenAQ Sensor Parameters information",
)
@dp.expect_all_or_drop({
    "id is not null": "id IS NOT NULL",
    "name is not null": "name IS NOT NULL",
})
def parameters():
    return (
        spark.readStream.table("sensors_temp").select(
            col("parameter.id").cast("int").alias("id"),
            col("parameter.displayName").alias("display_name"),
            col("parameter.name").alias("name"),
            col("parameter.units").alias("units")
        ).distinct()
    )