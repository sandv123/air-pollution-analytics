from pyspark.sql.functions import col, current_timestamp, concat, sha2, split # type: ignore
from delta.tables import * # type: ignore

catalog = "air_polution_analytics_dev"
landing_schema = "00_landing"
silver_schema = "02_silver"
source_table = "openaq_measurements"
target_table = "openaq_measurements_deduped"

base_path = f"/Volumes/{catalog}/{silver_schema}/metadata"
openaq_path = f"{base_path}/openaq"

create_table = f"create table if not exists {catalog}.{silver_schema}.{target_table} (id STRING, datetime_from TIMESTAMP, value FLOAT, location_id INT, sensor_id INT);"
create_volume = f"create volume if not exists {catalog}.{silver_schema}.metadata;"

spark.sql(create_table)
spark.sql(create_volume)
dbutils.fs.mkdirs(openaq_path)

def upsertToDelta(microBatchOutputDF, batchId):
    tableDeduped = DeltaTable.forName(spark, f"{catalog}.{silver_schema}.{target_table}")
    (tableDeduped.alias("t").merge(
        microBatchOutputDF.alias("s"),
        "s.id = t.id")
    .whenNotMatchedInsertAll()
    .execute()
    )
    
df = (spark.readStream
    .table(f"{catalog}.{silver_schema}.{source_table}")
    .withColumn("id", sha2(concat(col("sensor_id"), col("location_id"), col("datetime_from")), 256))
)

(df.writeStream
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .trigger(availableNow=True)
  .option("checkpointLocation", f"{openaq_path}/_schema")
  .start()
)
