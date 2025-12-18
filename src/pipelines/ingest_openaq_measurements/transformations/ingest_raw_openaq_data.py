from pyspark.sql.functions import col, current_timestamp
from pyspark import pipelines as dp # type: ignore

landing_catalog = "air_polution_analytics_dev"
schema = "00_landing"

base_path = f"/Volumes/{landing_catalog}/{schema}/openaq"
source_path = f"{base_path}/new"
metadata_path = f"{base_path}/_metadata"

@dp.table(
    name="air_quality_measurements_bronze",
    comment="Ingested raw OpenAQ measurements data"
)
def raw_measurements():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("multiline", "true")
        .option("pathGlobfilter", "*.json")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{metadata_path}/_schema")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .option("cloudFiles.schemaHints", "results.element.period.datetimeFrom.local STRING, results.element.period.datetimeFrom.utc TIMESTAMP, results.element.period.datetimeTo.local STRING, results.element.period.datetimeTo.utc TIMESTAMP, results.element.value FLOAT, results.element.parameter.id INT")
        .load(source_path)
        .selectExpr('explode(results) as results')
        .select("*", col("_metadata.file_name").alias("source_file_name"))
        .withColumn("bronze_load_ts", current_timestamp())
    )