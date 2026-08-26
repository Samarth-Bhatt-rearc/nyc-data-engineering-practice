# Pure DataFrame logic for the trip-summary gold table, kept free of any
# `pyspark.pipelines` import so it can be unit tested directly — see
# tests/test_gold_summary_logic.py.
from pyspark.sql import functions as F


def build_trip_summary(df):
    """Aggregate trip distance/count by year, month, vendor, and payment type.

    Args:
        df: Silver-layer DataFrame containing year, month, vendor_name,
            payment_type, and trip_distance columns.
    """
    return df.groupBy("year", "month", "vendor_name", "payment_type").agg(
        F.sum("trip_distance").alias("total_distance"),
        F.avg("trip_distance").alias("avg_distance"),
        F.count("*").alias("trip_count"),
    )
