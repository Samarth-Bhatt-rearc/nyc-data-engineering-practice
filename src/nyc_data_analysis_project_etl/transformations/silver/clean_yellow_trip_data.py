# Silver layer: cleans bronze's yellow_tripdata_raw — trims whitespace, normalizes
# payment/vendor/store-and-forward codes (which vary by era: numeric codes, 3-letter
# abbreviations, and full words all show up across 2009-2026) to consistent values,
# and fills sentinel defaults where a field is legitimately missing.
#
# The actual cleaning logic lives in silver_logic.py (no pyspark.pipelines import
# there) so it can be unit tested outside a live pipeline — see tests/test_silver_logic.py.
# Lakeflow automatically adds each pipeline source file's own directory to
# sys.path, so a same-directory module resolves via a plain top-level import
# with no manual path manipulation needed. The module is named silver_logic, not
# logic — Lakeflow's sys.path additions are shared across the whole pipeline, not
# scoped per source directory, so a same-named bronze/logic.py would collide with
# this one via sys.modules and silently win depending on load order.
from pyspark import pipelines as dp
from silver_logic import (
    PAYMENT_TYPE_LOOKUP,
    STORE_AND_FWD_FLAG_LOOKUP,
    VENDOR_LOOKUP,
    mapping_with_defaults,
    replacing_nulls_with_default,
    trim_whitespace,
)


@dp.table(
    name="yellow_tripdata_silver",
    comment=(
        "Yellow taxi trip records cleaned and transformed to a unified schema, incrementally ingested via Auto Loader."
    ),
)
def yellow_tripdata_silver():
    """Streaming table: clean bronze's yellow_tripdata_raw into yellow_tripdata_silver."""
    df = spark.readStream.table("yellow_tripdata_raw")
    df = trim_whitespace(df)
    df = mapping_with_defaults(df, PAYMENT_TYPE_LOOKUP, "payment_type")
    # vendor_id (raw code) is kept as-is; vendor_name is the cleaned display name.
    df = mapping_with_defaults(df, VENDOR_LOOKUP, "vendor_id", new_column_name="vendor_name")
    df = mapping_with_defaults(df, STORE_AND_FWD_FLAG_LOOKUP, "store_and_fwd_flag", "N")
    # 99 matches TLC's own documented sentinel for "Null/unknown" RatecodeID.
    df = replacing_nulls_with_default(df, "passenger_count", 99)
    df = replacing_nulls_with_default(df, "rate_code_id", 99)
    # These fees didn't exist in earlier years (e.g. congestion_surcharge predates
    # 2019), so bronze leaves them null for those rows — default to 0 so gold's
    # SUMs/flags don't have to special-case nulls.
    list_of_surcharges = ["improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]
    for surcharge in list_of_surcharges:
        df = replacing_nulls_with_default(df, surcharge, 0.0)
    return df
