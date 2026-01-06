from pyspark.sql.functions import col, current_timestamp # type: ignore
from pyspark import pipelines as dp # type: ignore

catalog = "air_polution_analytics_dev"
landing_schema = "00_landing"
bronze_schema = "01_bronze"

base_path = f"/Volumes/{catalog}/{landing_schema}/openmeteo"
measurements_path = f"{base_path}/measurements"
metadata_path = f"{base_path}/_metadata"


@dp.table(
    name=f"{catalog}.{bronze_schema}.temperature_measurements_raw",
    comment="Ingested raw open-meteo measurements data"
)
def temperature_measurements():
    return (spark.readStream
        .format("cloudFiles")
        .option("multiline", "true")
        .option("cloudFiles.format", "json")
        .option("pathGlobfilter", "openmeteo*.json.gz")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{measurements_path}/_schema")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .load(measurements_path)
        .select("*", col("_metadata.file_name").alias("source_file_name"))
        .withColumn("bronze_load_ts", current_timestamp())
    )