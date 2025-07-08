import dlt
from pyspark.sql.functions import col
from utilities.utils import to_boolean_store_and_forward_flag

@dlt.table(comment="Cleaned up taxi table")
@dlt.expect("valid_vendor",  "VendorID     IN (1, 2, 6, 7)")
@dlt.expect("valid_ratecode","RatecodeID  IN (1, 2, 3, 4, 5, 6, 99)")
@dlt.expect("valid_payment", "payment_type IN (0, 1, 2, 3, 4, 5, 6)")
def taxi_data_clean():
    return (
        dlt.read_stream("trip_data")
           .filter(col("VendorID").isNotNull())
           .withColumn(
               "store_and_fwd_flag",
               to_boolean_store_and_forward_flag(col("store_and_fwd_flag"))
           )
           .na.fill({"RatecodeID": 99})
           )
    
