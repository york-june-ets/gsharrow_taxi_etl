import dlt
from pyspark.sql.functions import col, when

@dlt.table(
  comment="Dimension table of taxi ratecodes"
)
def dim_ratecode():
    df = dlt.read_stream("taxi_data_clean")

    # 2. Extract distinct RatecodeIDs
    ratecode = df.select(col("RatecodeID").alias("ratecode_id")).distinct()

    # 3. Map each ID to its description
    return ratecode.withColumn(
        "ratecode_description",
        when(col("ratecode_id") == 0, "Standard rate")
        .when(col("ratecode_id") == 1, "JFK")
        .when(col("ratecode_id") == 2, "Newark")
        .when(col("ratecode_id") == 3, "Nassau or Westchester")
        .when(col("ratecode_id") == 4, "Negotiated fare")
        .when(col("ratecode_id") == 5, "Group ride")
        .otherwise("Null/Unknown")
    )
