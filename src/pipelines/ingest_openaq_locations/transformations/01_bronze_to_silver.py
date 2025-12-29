from pyspark.sql.types import ArrayType, IntegerType
from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, split, collect_set

catalog = "air_polution_analytics_dev"
bronze_schema = "01_bronze"
silver_schema = "02_silver"

@dp.temporary_view
def locations_temp():
    locations_df = spark.readStream.table(f"{catalog}.{bronze_schema}.openaq_locations")
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
    name=f"{catalog}.{silver_schema}.openaq_locations",
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
    name=f"{catalog}.{silver_schema}.openaq_sensors",
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
    name=f"{catalog}.{silver_schema}.openaq_parameters",
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