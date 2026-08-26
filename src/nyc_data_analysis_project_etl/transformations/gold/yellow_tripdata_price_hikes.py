from pyspark import pipelines as dp
from pyspark.sql import functions as F

SURCHARGE_COLUMNS = [
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
]


@dp.materialized_view(
    name="yellow_tripdata_price_hikes",
    comment=(
        "Daily price-hike summary by pickup location: surcharge/fee totals "
        "(improvement, congestion, airport, CBD congestion), whether each "
        "surcharge occurred, each surcharge's percent contribution to the "
        "combined total, and overall fare — grouped by pickup location and date."
    ),
    cluster_by=["pickup_date"],
)
def yellow_tripdata_price_hikes():
    df = spark.read.table("yellow_tripdata_silver")

    # 2011+ rows have pickup_location_id; 2009-2010 legacy rows only have
    # lat/long, so bucket those into a coarse (~1km) grid to make them groupable.
    pickup_location = F.when(
        F.col("pickup_location_id").isNotNull(),
        F.concat(F.lit("LOC_"), F.col("pickup_location_id").cast("string")),
    ).otherwise(
        F.concat(
            F.lit("GRID_"),
            F.round(F.col("pickup_longitude"), 2).cast("string"),
            F.lit("_"),
            F.round(F.col("pickup_latitude"), 2).cast("string"),
        )
    )

    aggregated = (
        df.withColumn("pickup_location", pickup_location)
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .groupBy("pickup_location", "pickup_date")
        .agg(
            F.coalesce(F.sum("improvement_surcharge"), F.lit(0.0)).alias("total_improvement_surcharge"),
            F.coalesce(F.sum("congestion_surcharge"), F.lit(0.0)).alias("total_congestion_surcharge"),
            F.coalesce(F.sum("airport_fee"), F.lit(0.0)).alias("total_airport_fee"),
            F.coalesce(F.sum("cbd_congestion_fee"), F.lit(0.0)).alias("total_cbd_congestion_fee"),
            F.sum("total_amount").alias("total_amount_sum"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.count("*").alias("trip_count"),
        )
        .withColumn(
            "total_price_hikes",
            F.col("total_improvement_surcharge")
            + F.col("total_congestion_surcharge")
            + F.col("total_airport_fee")
            + F.col("total_cbd_congestion_fee"),
        )
    )

    def pct_of_total(total_column):
        return F.when(
            F.col("total_price_hikes") > 0,
            F.round(F.col(total_column) / F.col("total_price_hikes") * 100, 2),
        ).otherwise(F.lit(0.0))

    result = aggregated.withColumn("had_any_surcharge", F.col("total_price_hikes") > 0)
    for surcharge in SURCHARGE_COLUMNS:
        total_column = f"total_{surcharge}"
        result = (
            result.withColumn(f"had_{surcharge}", F.col(total_column) > 0)
            .withColumn(f"pct_{surcharge}", pct_of_total(total_column))
        )

    return result
