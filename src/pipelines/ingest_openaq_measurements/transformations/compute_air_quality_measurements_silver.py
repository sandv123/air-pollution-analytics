from pyspark import pipelines as dp # type: ignore
from pyspark.sql.functions import col, split

catalog = "air_polution_analytics_dev"
schema = "01_bronze"
source_table = f"{catalog}.{schema}.air_quality_measurements_bronze"
filename_re = r"^(d+)_(d+)_(D+)_(.+)_"

@dp.table(
    name=f"{catalog}.02_silver.air_quality_measurements_silver",
    comment="Air quality measurements in the Silver layer",
)
@dp.expect("period interval is 1 hour", "interval = '01:00:00'")
@dp.expect("datetime_from is not null", "datetime_from IS NOT NULL")
@dp.expect("value is not null", "value IS NOT NULL")
@dp.expect("parameter_name is not null", "parameter_name IS NOT NULL")
@dp.expect("location_id is not null", "location_id IS NOT NULL")
@dp.expect("sensor_id is not null", "sensor_id IS NOT NULL")
def compute_air_quality_measurements_silver():
    df = (
        spark.readStream.table(source_table)
            .select(col("results.period.datetimeFrom.utc").alias("datetime_from"),
                    col("results.period.datetimeTo.utc").alias("datetime_to"),
                    col("results.value"),
                    col("results.parameter.id").alias("parameter_id"),
                    col("results.parameter.name").alias("parameter_name"),
                    col("results.parameter.units").alias("parameter_units"),
                    col("results.coordinates"),
                    # col("results.coverage"),
                    # col("results.summary"),
                    col("source_file_name"),
                    col("results.period.interval").alias("interval"),)
    )
    split_col = split(df.source_file_name, "_")
    df = (df.withColumn("location_id", split_col.getItem(0))
            .withColumn("sensor_id", split_col.getItem(1))
            .withColumn("location_name", split_col.getItem(3))
    )

    return df