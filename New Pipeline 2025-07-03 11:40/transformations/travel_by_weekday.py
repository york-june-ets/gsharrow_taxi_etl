import dlt
from pyspark.sql.functions import (
    date_format, avg, round, col
)
@dlt.table(
  comment="Average distance traveled by weekday")

def taxi_avg_distance_by_day():
  df = dlt.read("taxi_data_clean")
  return(
      df
      .withColumn("weekday", date_format(col("tpep_pickup_datetime"), "EEEE"))
      .groupBy("weekday")
      .agg(round(avg("trip_distance"), 2).alias("avg_distance"))
      )
      
  