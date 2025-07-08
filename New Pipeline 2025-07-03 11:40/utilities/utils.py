from pyspark.sql.types import *
from pyspark.sql.functions import when

# Fact Trips schema
fact_trips_schema = StructType([
    StructField("VendorID",               LongType(), False),
    StructField("tpep_pickup_datetime",   TimestampType(), True),
    StructField("tpep_dropoff_datetime",  TimestampType(), True),
    StructField("passenger_count",        LongType(), True),
    StructField("trip_distance",          DoubleType(),  True),
    StructField("RatecodeID",             LongType(), True),
    StructField("store_and_fwd_flag",     StringType(),  True),
    StructField("PULocationID",           LongType(), True),
    StructField("DOLocationID",           LongType(), True),
    StructField("payment_type",           LongType(), True),
    StructField("fare_amount",            DoubleType(),  True),
    StructField("extra",                  DoubleType(),  True),
    StructField("mta_tax",                DoubleType(),  True),
    StructField("tip_amount",             DoubleType(),  True),
    StructField("tolls_amount",           DoubleType(),  True),
    StructField("improvement_surcharge",  DoubleType(),  True),
    StructField("total_amount",           DoubleType(),  True),
    StructField("congestion_surcharge",   DoubleType(),  True),
    StructField("airport_fee",            DoubleType(),  True),
])


def to_boolean_store_and_forward_flag(col):
    return when(col == 'Y', True).otherwise(False)


