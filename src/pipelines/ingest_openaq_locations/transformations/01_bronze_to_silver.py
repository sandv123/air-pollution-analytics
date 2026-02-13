from pyspark.sql.types import ArrayType, IntegerType
from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, split, collect_set

catalog = "air_polution_analytics_dev"
bronze_schema = "01_bronze"
silver_schema = "02_silver"

@dp.temporary_view
def locations_temp():
    locations_df = spark.readStream.table(f"{catalog}.{bronze_schema}.openaq_locations")

    split_col = split(locations_df.source_file_name, "-")
    return (
        locations_df
            .select(
                col("results.id").alias("id").cast("int"),
                col("results.name").alias("name"),
                col("results.locality").alias("locality"),
                col("results.country.code").alias("country"),
                col("results.isMobile").alias("is_mobile"),
                col("results.isMonitor").alias("is_monitor"),
                col("results.coordinates.latitude").alias("latitude").cast("float"),
                col("results.coordinates.longitude").alias("longitude").cast("float"),
                col("results.sensors").alias("sensors"),
                col("source_file_name")
            )
            .withColumn("city", split_col.getItem(0))
            .drop(col("source_file_name"))
    )


dim_locations_schema = """
    location_id INT NOT NULL,
    name STRING NOT NULL,
    locality STRING,
    country STRING,
    city STRING NOT NULL,
    is_mobile BOOLEAN,
    is_monitor BOOLEAN,
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (location_id) RELY
"""

@dp.table(
    name=f"{catalog}.{silver_schema}.dim_pollution_locations",
    comment="Dimension table for OpenAQ locations",
    schema=dim_locations_schema
)
@dp.expect_all_or_drop({
    "location_id is not null": "location_id IS NOT NULL",
    "name is not null": "name IS NOT NULL",
    "city is not null": "city IS NOT NULL"
})
def locations():
    locations_df = spark.readStream.table("locations_temp")
    return (
        locations_df.select(
            col("id").alias("location_id"),
            col("name"),
            col("locality"),
            col("country"),
            col("city"),
            col("is_mobile"),
            col("is_monitor"),
            col("latitude"),
            col("longitude")
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
            col("sensors.parameter").alias("parameter"),
            col("id").alias("location_id")
        )
    )


dim_parameters_schema = """
    parameter_id INT NOT NULL,
    display_name STRING,
    name STRING NOT NULL,
    units STRING,
    PRIMARY KEY (parameter_id) RELY
"""

@dp.table(
    name=f"{catalog}.{silver_schema}.dim_pollution_parameters",
    comment="Dimension table for OpenAQ Sensor Parameters information",
    schema=dim_parameters_schema
)
@dp.expect_all_or_drop({
    "parameter_id is not null": "parameter_id IS NOT NULL",
    "name is not null": "name IS NOT NULL",
})
def parameters():
    return (
        spark.readStream.table("sensors_temp").select(
            col("parameter.id").cast("int").alias("parameter_id"),
            col("parameter.displayName").alias("display_name"),
            col("parameter.name").alias("name"),
            col("parameter.units").alias("units")
        ).distinct()
    )


dim_sensors_schema = f"""
    sensor_id INT NOT NULL,
    name STRING NOT NULL,
    location_id INT NOT NULL REFERENCES {catalog}.{silver_schema}.dim_pollution_locations(location_id) RELY,
    parameter_id INT NOT NULL REFERENCES {catalog}.{silver_schema}.dim_pollution_parameters(parameter_id) RELY,
    PRIMARY KEY (sensor_id) RELY
"""

@dp.table(
    name=f"{catalog}.{silver_schema}.dim_pollution_sensors",
    comment="Dimension table for OpenAQ Sensor information",
    schema=dim_sensors_schema
)
@dp.expect_all_or_drop({
    "sensor_id is not null": "sensor_id IS NOT NULL",
    "location_id is not null": "location_id IS NOT NULL",
    "parameter_id is not null": "parameter_id IS NOT NULL"
})
def sensors():
    return (
        spark.readStream.table("sensors_temp")
            .select(
                col("id").cast("int").alias("sensor_id"),
                col("name"),
                col("location_id"),
                col("parameter.id").alias("parameter_id").cast("int")
            )
    )