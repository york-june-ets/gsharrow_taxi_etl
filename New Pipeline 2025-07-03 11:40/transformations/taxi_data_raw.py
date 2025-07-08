import dlt
from utilities.utils import fact_trips_schema

parquet_folder = "dbfs:/Volumes/ets_thriventcohort/default/test_volume"

@dlt.table(
  comment="Consolidated trip data from all Parquet files",
)
def trip_data():
    return (
      spark.readStream
           .schema(fact_trips_schema)
           .format("cloudFiles")
           .option("cloudFiles.format", "parquet")
           .load(parquet_folder)
    )
