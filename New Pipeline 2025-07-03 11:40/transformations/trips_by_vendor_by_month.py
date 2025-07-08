import dlt
from pyspark.sql.functions import date_format, count, col, year

@dlt.table(
  comment="Trips by vendor by month")

def trips_by_vendor():
    trips = dlt.read("taxi_data_clean")
    vendor = dlt.read("dim_vendor")

    joined = trips.join(
        vendor.select(col("vendor_id"), col("vendor_description")),
        trips.VendorID == vendor.vendor_id,
        how="left"
    )
    return(
        joined
        .filter(year(col("tpep_pickup_datetime")) == 2024)
        .withColumn("year_month", date_format(col("tpep_pickup_datetime"), "yyyy-MM"))
        .groupBy("year_month", "vendor_description")
        .agg(count("*").alias("trip_count"))
        .orderBy(col("year_month"), col("trip_count").desc())
    )