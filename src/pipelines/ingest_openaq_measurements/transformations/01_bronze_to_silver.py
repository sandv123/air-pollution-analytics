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