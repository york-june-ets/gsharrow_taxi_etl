import dlt
from pyspark.sql.functions import (
    date_format, sum as _sum, round, col, year
)

@dlt.table(
  comment="Average distance traveled by weekday"
)
def revenue_per_month():
    trips = dlt.read("taxi_data_clean")
    zones = dlt.read("taxi_zones")

    joined_df = trips.join(
        zones.select(
            col("LocationID").alias("PULocationID"),
            col("Borough")
        ),
        on="PULocationID",
        how="left"
    )

    revenue_by_borough = (
        joined_df
        # keep only 2024 trips
        .filter(year(col("tpep_pickup_datetime")) == 2024)
        # extract year-month string
        .withColumn("year_month", date_format(col("tpep_pickup_datetime"), "yyyy-MM"))
        # sum revenue per borough/month
        .groupBy("Borough", "year_month")
        .agg(
            round(_sum("total_amount"), 2).alias("monthly_revenue")
        )
        # order for easy review
        .orderBy("Borough", "year_month")
    )
    return revenue_by_borough
