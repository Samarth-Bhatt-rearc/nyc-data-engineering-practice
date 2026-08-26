# Pure DataFrame logic for the bronze layer, kept free of any `pyspark.pipelines`
# import so it can be unit tested directly (pipeline decorators like @dp.table
# raise immediately when invoked outside a live pipeline graph, which is always
# true in a local test process — see tests/test_bronze_logic.py).
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType

# The unified schema every era's raw columns get renamed + cast into.
CANONICAL_SCHEMA = {
    "vendor_id": StringType(),
    "pickup_datetime": TimestampType(),
    "dropoff_datetime": TimestampType(),
    "passenger_count": DoubleType(),
    "trip_distance": DoubleType(),
    "rate_code_id": DoubleType(),
    "store_and_fwd_flag": StringType(),
    "pickup_location_id": DoubleType(),
    "dropoff_location_id": DoubleType(),
    "pickup_longitude": DoubleType(),
    "pickup_latitude": DoubleType(),
    "dropoff_longitude": DoubleType(),
    "dropoff_latitude": DoubleType(),
    "payment_type": StringType(),
    "fare_amount": DoubleType(),
    "extra": DoubleType(),
    "mta_tax": DoubleType(),
    "tip_amount": DoubleType(),
    "tolls_amount": DoubleType(),
    "improvement_surcharge": DoubleType(),
    "congestion_surcharge": DoubleType(),
    "airport_fee": DoubleType(),
    "cbd_congestion_fee": DoubleType(),
    "total_amount": DoubleType(),
}

# NYC TLC has re-used the "yellow_tripdata" file name across at least three
# incompatible column-naming conventions. Map each era's source columns onto
# CANONICAL_SCHEMA; missing columns are filled with nulls at read time. Each
# era is landed into its own subfolder (see src/nyc_data_analysis_project/main.py)
# so Auto Loader sees one stable schema per stream.
ERA_2009_COLUMNS = {
    "vendor_name": "vendor_id",
    "Trip_Pickup_DateTime": "pickup_datetime",
    "Trip_Dropoff_DateTime": "dropoff_datetime",
    "Passenger_Count": "passenger_count",
    "Trip_Distance": "trip_distance",
    "Start_Lon": "pickup_longitude",
    "Start_Lat": "pickup_latitude",
    "Rate_Code": "rate_code_id",
    "store_and_forward": "store_and_fwd_flag",
    "End_Lon": "dropoff_longitude",
    "End_Lat": "dropoff_latitude",
    "Payment_Type": "payment_type",
    "Fare_Amt": "fare_amount",
    "surcharge": "extra",
    "mta_tax": "mta_tax",
    "Tip_Amt": "tip_amount",
    "Tolls_Amt": "tolls_amount",
    "Total_Amt": "total_amount",
}

ERA_2010_COLUMNS = {
    "vendor_id": "vendor_id",
    "pickup_datetime": "pickup_datetime",
    "dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "pickup_longitude": "pickup_longitude",
    "pickup_latitude": "pickup_latitude",
    "rate_code": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "dropoff_longitude": "dropoff_longitude",
    "dropoff_latitude": "dropoff_latitude",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "surcharge": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "total_amount": "total_amount",
}

# Covers 2011 onward: column names have been stable since 2011, only the
# physical types (int64 vs int32 vs double) and the airport_fee casing have
# drifted, both absorbed by the explicit .cast(...) in normalize below.
MODERN_COLUMNS = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
    "Airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
    "total_amount": "total_amount",
}

FILE_NAME_REGEX = r"yellow_tripdata_(\d{4})-(\d{2})\.parquet"


# Renames + casts one era's raw columns into CANONICAL_SCHEMA, filling columns
# that era doesn't have with null, and tags each row with its source file/year/month
# (parsed from the Spark-populated `_metadata.file_path` hidden column).
def normalize(df, mapping):
    """Rename/cast one era's raw columns into CANONICAL_SCHEMA and tag rows with source metadata.

    Args:
        df: Raw DataFrame for a single schema era (as read by Auto Loader).
        mapping: Dict of {source_column_name: canonical_column_name} for that
            era (e.g. ERA_2009_COLUMNS, ERA_2010_COLUMNS, or MODERN_COLUMNS).
    """
    present_canonical = set()
    select_exprs = []
    for source_col, canonical_col in mapping.items():
        if source_col in df.columns:
            select_exprs.append(F.col(source_col).cast(CANONICAL_SCHEMA[canonical_col]).alias(canonical_col))
            present_canonical.add(canonical_col)
    for canonical_col, dtype in CANONICAL_SCHEMA.items():
        if canonical_col not in present_canonical:
            select_exprs.append(F.lit(None).cast(dtype).alias(canonical_col))

    # Resolved inside the same select() as the other columns — a `.withColumn()`
    # chained after a select that doesn't include `_metadata` can't resolve it
    # under standard Spark semantics (confirmed by testing this locally; it only
    # appeared to work before because Databricks Runtime resolves hidden metadata
    # columns more leniently than open-source Spark does).
    file_path = F.col("_metadata.file_path")
    select_exprs.append(F.element_at(F.split(file_path, "/"), -1).alias("source_file"))
    select_exprs.append(F.regexp_extract(file_path, FILE_NAME_REGEX, 1).cast("int").alias("year"))
    select_exprs.append(F.regexp_extract(file_path, FILE_NAME_REGEX, 2).cast("int").alias("month"))

    return df.select(*select_exprs)
