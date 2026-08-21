from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="yellow_tripdata_gold",
    comment=(
        "Yellow taxi trip summary aggregated by year, month, vendor, and payment "
        "type: total distance, average distance, and trip count."
    ),
    cluster_by=["year", "month"],
)
def yellow_tripdata_gold():
    return (
        spark.read.table("yellow_tripdata_silver")
        .groupBy("year", "month", "vendor_name", "payment_type")
        .agg(
            F.sum("trip_distance").alias("total_distance"),
            F.avg("trip_distance").alias("avg_distance"),
            F.count("*").alias("trip_count"),
        )
    )
