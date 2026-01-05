from pyspark.sql.functions import col, current_timestamp # type: ignore
from pyspark import pipelines as dp # type: ignore

catalog = "air_polution_analytics_dev"
landing_schema = "00_landing"
bronze_schema = "01_bronze"

base_path = f"/Volumes/{catalog}/{landing_schema}/openaq"
measurements_path = f"{base_path}/measurements"
# archive_path = f"{measurements_path}/processed_files"

@dp.table(
    name=f"{catalog}.{bronze_schema}.openaq_measurements",
    comment="Ingested raw OpenAQ measurements data"
)
def raw_measurements():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("multiline", "true")
        .option("pathGlobfilter", "[0-9]*.json.gz")
        .option("cloudFiles.format", "json")
        # .option("cloudFiles.cleanSource", "MOVE")
        # .option("cloudFiles.cleanSource.moveDestination", archive_path) 
        # .option("cloudFiles.cleanSource.retentionDuration", "1 hour")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{measurements_path}/_schema")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .option("cloudFiles.schemaHints", "results.element.period.datetimeFrom.local STRING, results.element.period.datetimeFrom.utc TIMESTAMP, results.element.period.datetimeTo.local STRING, results.element.period.datetimeTo.utc TIMESTAMP, results.element.value FLOAT, results.element.parameter.id INT")
        .load(measurements_path)
        .selectExpr('explode(results) as results')
        .select("*", col("_metadata.file_name").alias("source_file_name"))
        .withColumn("bronze_load_ts", current_timestamp())
    )