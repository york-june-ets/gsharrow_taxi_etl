import dlt
from pyspark.sql.functions import (
    date_format, hour, count, row_number, col
)

from pyspark.sql.window import Window

@dlt.table(
  comment="Peak hours by day")

def peak_hours_by_day():
    trips = dlt.read("taxi_data_clean")

    hourly_counts = (
    trips
      .withColumn("weekday", date_format(col("tpep_pickup_datetime"), "EEEE"))
      .withColumn("trip_hour", hour(col("tpep_pickup_datetime")))
      .groupBy("weekday", "trip_hour")
      .agg(count("*").alias("trip_count")))
    
    week_window = Window.partitionBy("weekday") \
                    .orderBy(col("trip_count").desc())
    
    return (
        hourly_counts
            .withColumn("rank", row_number().over(week_window))
            .select("weekday", "trip_hour", "trip_count", "rank")
            .orderBy("weekday", "rank")
    )
