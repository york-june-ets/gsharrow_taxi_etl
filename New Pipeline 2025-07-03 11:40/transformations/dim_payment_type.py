import dlt
from pyspark.sql.functions import col, when

@dlt.table(
  comment="Dimension table of taxi payment types"
)
def dim_payment_types():
    df = dlt.read_stream("taxi_data_clean")

    # 2. Extract distinct payment types
    payment = df.select(col("payment_type").alias("payment_id")).distinct()

    # 3. Map each ID to its description
    return payment.withColumn(
        "payment_description",
        when(col("payment_id") == 0, "Flex Fare trip")
        .when(col("payment_id") == 1, "Credit card")
        .when(col("payment_id") == 2, "Cash")
        .when(col("payment_id") == 3, "No charge")
        .when(col("payment_id") == 4, "Dispute")
        .when(col("payment_id") == 5, "Unknown")
        .when(col("payment_id") == 6, "Voided trip")
    )
