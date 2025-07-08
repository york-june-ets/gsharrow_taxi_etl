import dlt
from pyspark.sql.functions import col, when

@dlt.table(
  comment="Dimension table of taxi vendors"
)
def dim_vendor():
    df = dlt.read_stream("taxi_data_clean")

    # 2. Extract distinct VendorIDs
    vendors = df.select(col("VendorID").alias("vendor_id")).distinct()

    # 3. Map each ID to its description
    return vendors.withColumn(
        "vendor_description",
        when(col("vendor_id") == 1, "Creative Mobile Technologies, LLC")
        .when(col("vendor_id") == 2, "Curb Mobility, LLC")
        .when(col("vendor_id") == 6, "Myle Technologies Inc")
        .when(col("vendor_id") == 7, "Helix")
        .otherwise("Unknown")
    )
