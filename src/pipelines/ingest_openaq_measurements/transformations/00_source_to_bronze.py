from pyspark.sql.functions import col, current_timestamp # type: ignore
from pyspark import pipelines as dp # type: ignore

catalog = "air_polution_analytics_dev"
landing_schema = "00_landing"
bronze_schema = "01_bronze"

base_path = f"/Volumes/{catalog}/{landing_schema}/openaq"
measurements_path = f"{base_path}/measurements"
locations_path = f"{base_path}/locations"
metadata_path = f"{base_path}/_metadata"

@dp.table(
    name=f"{catalog}.{bronze_schema}.air_quality_measurements",
    comment="Ingested raw OpenAQ measurements data"
)
def raw_measurements():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("multiline", "true")
        .option("pathGlobfilter", "[0-9]*.json")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{measurements_path}/_schema")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .option("cloudFiles.schemaHints", "results.element.period.datetimeFrom.local STRING, results.element.period.datetimeFrom.utc TIMESTAMP, results.element.period.datetimeTo.local STRING, results.element.period.datetimeTo.utc TIMESTAMP, results.element.value FLOAT, results.element.parameter.id INT")
        .load(measurements_path)
        .selectExpr('explode(results) as results')
        .select("*", col("_metadata.file_name").alias("source_file_name"))
        .withColumn("bronze_load_ts", current_timestamp())
    )


@dp.table(
    name=f"{catalog}.{bronze_schema}.locations",
    comment="Ingested raw OpenAQ locations data"
)
def locations():
    return (spark.readStream
        .format("cloudFiles")
        .option("multiline", "true")
        .option("cloudFiles.format", "json")
        .option("pathGlobfilter", "[a-zA-Z]*-locations*.json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{locations_path}/_schema")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .option("cloudFiles.schemaHints", "results.element.id INT, results.element.coordinates STRUCT<latitude FLOAT, longitude FLOAT>")
        .load(locations_path)
        .selectExpr('explode(results) as results')
        .select("*", col("_metadata.file_name").alias("source_file_name"))
        .withColumn("bronze_load_ts", current_timestamp())
    )