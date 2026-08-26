# Gold layer: trip counts/distance rolled up by year, month, vendor, and payment
# type — backs the dashboard's "Trip Summary" page.
#
# The aggregation logic lives in summary_logic.py (no pyspark.pipelines import
# there) so it can be unit tested outside a live pipeline — see
# tests/test_gold_summary_logic.py.
import os
import sys

from pyspark import pipelines as dp

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from summary_logic import build_trip_summary  # noqa: E402


@dp.materialized_view(
    name="yellow_tripdata_gold",
    comment=(
        "Yellow taxi trip summary aggregated by year, month, vendor, and payment "
        "type: total distance, average distance, and trip count."
    ),
    cluster_by=["year", "month"],
)
def yellow_tripdata_gold():
    """Materialized view: trip summary aggregated from yellow_tripdata_silver."""
    return build_trip_summary(spark.read.table("yellow_tripdata_silver"))
