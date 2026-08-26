# Gold layer: daily surcharge/fee summary by pickup location — backs the
# dashboard's "Price Hikes" page.
#
# The aggregation logic lives in price_hikes_logic.py (no pyspark.pipelines
# import there) so it can be unit tested outside a live pipeline — see
# tests/test_gold_price_hikes_logic.py.
import os
import sys

from pyspark import pipelines as dp

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from price_hikes_logic import build_price_hikes  # noqa: E402


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
