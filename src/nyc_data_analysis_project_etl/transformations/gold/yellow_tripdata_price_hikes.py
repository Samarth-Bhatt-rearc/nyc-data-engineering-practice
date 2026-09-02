# Gold layer: daily surcharge/fee summary by pickup location — backs the
# dashboard's "Price Hikes" page.
#
# The aggregation logic lives in price_hikes_logic.py (no pyspark.pipelines
# import there) so it can be unit tested outside a live pipeline — see
# tests/test_gold_price_hikes_logic.py.
# Lakeflow automatically adds each pipeline source file's own directory to
# sys.path, so a same-directory module resolves via a plain top-level import
# with no manual path manipulation needed.
from price_hikes_logic import build_price_hikes
from pyspark import pipelines as dp


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
    """Materialized view: price-hike summary aggregated from yellow_tripdata_silver."""
    return build_price_hikes(spark.read.table("yellow_tripdata_silver"))
