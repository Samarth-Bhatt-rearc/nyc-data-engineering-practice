# Pure DataFrame logic for the price-hikes gold table, kept free of any
# `pyspark.pipelines` import so it can be unit tested directly — see
# tests/test_gold_price_hikes_logic.py.
from pyspark.sql import functions as F

SURCHARGE_COLUMNS = [
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
]


# 2011+ rows have pickup_location_id; 2009-2010 legacy rows only have lat/long,
# so bucket those into a coarse (~1km) grid to make them groupable.
def add_pickup_location(df):
    """Add an era-aware pickup_location key: LOC_<id> for 2011+, GRID_<lon>_<lat> for legacy rows.

    Args:
        df: DataFrame containing pickup_location_id, pickup_longitude, and
            pickup_latitude columns.
    """
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
    return df.withColumn("pickup_location", pickup_location)


# Each surcharge's share of that group's total_price_hikes (0 when there were
# no surcharges at all, avoiding a divide-by-zero).
def pct_of_total(total_column):
    """Build a column expression for one surcharge's percent share of total_price_hikes (0 if none).

    Args:
        total_column: Name of the surcharge total column to express as a
            percent of total_price_hikes (e.g. "total_improvement_surcharge").
    """
    return F.when(
        F.col("total_price_hikes") > 0,
        F.round(F.col(total_column) / F.col("total_price_hikes") * 100, 2),
    ).otherwise(F.lit(0.0))


def build_price_hikes(df):
    """Aggregate surcharge/fee totals by pickup location and date, with per-surcharge flags and percentages.

    Args:
        df: Silver-layer DataFrame containing pickup_location_id,
            pickup_longitude, pickup_latitude, pickup_datetime, the
            SURCHARGE_COLUMNS fee columns, and total_amount.
    """
    aggregated = (
        add_pickup_location(df)
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

    # For each surcharge type, add a had_<surcharge> flag and a pct_<surcharge>
    # contribution column, plus one overall had_any_surcharge flag.
    result = aggregated.withColumn("had_any_surcharge", F.col("total_price_hikes") > 0)
    for surcharge in SURCHARGE_COLUMNS:
        total_column = f"total_{surcharge}"
        result = result.withColumn(f"had_{surcharge}", F.col(total_column) > 0).withColumn(
            f"pct_{surcharge}", pct_of_total(total_column)
        )

    return result
