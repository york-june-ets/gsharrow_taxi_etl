import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

@dlt.table(
  comment="dim table for taxi zones",
)
def taxi_zones():
    raw = (
      spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "csv")
           .option("header", True)
           .load("dbfs:/Volumes/ets_thriventcohort/default/taxi_zone/")
    )
    return (
        raw.withColumn("LocationID", col("LocationID").cast(IntegerType()))
           .drop("_rescued_data")
    )