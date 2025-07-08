import dlt
from pyspark.sql.functions import col, avg, round

@dlt.table(
  comment="Highest average tips per zone")

def highest_avg_tips():
    trips = dlt.read("taxi_data_clean")
    zones = dlt.read("taxi_zones")

    return(
    trips
      .join(
        zones.select(col("LocationID"), col("Zone")),
        trips.PULocationID == zones.LocationID,
        how="left"
      )
      .groupBy("Zone")
      .agg(
        round(avg("tip_amount"), 2)
          .alias("avg_tip_amount")
      )
      .orderBy(col("avg_tip_amount").desc())
    )
