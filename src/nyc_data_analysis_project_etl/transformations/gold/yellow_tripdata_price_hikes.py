from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="yellow_tripdata_price_hikes",
    comment=(
        "Daily price-hike summary by pickup location: surcharge/fee totals "
        "(improvement, congestion, airport, CBD congestion) and overall fare, "
        "grouped by pickup location and pickup date."
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

    return (
        df.withColumn("pickup_location", pickup_location)
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .groupBy("pickup_location", "pickup_date")
        .agg(
            F.sum("improvement_surcharge").alias("total_improvement_surcharge"),
            F.sum("congestion_surcharge").alias("total_congestion_surcharge"),
            F.sum("airport_fee").alias("total_airport_fee"),
            F.sum("cbd_congestion_fee").alias("total_cbd_congestion_fee"),
            (
                F.coalesce(F.sum("improvement_surcharge"), F.lit(0.0))
                + F.coalesce(F.sum("congestion_surcharge"), F.lit(0.0))
                + F.coalesce(F.sum("airport_fee"), F.lit(0.0))
                + F.coalesce(F.sum("cbd_congestion_fee"), F.lit(0.0))
            ).alias("total_price_hikes"),
            F.sum("total_amount").alias("total_amount_sum"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.count("*").alias("trip_count"),
        )
    )
