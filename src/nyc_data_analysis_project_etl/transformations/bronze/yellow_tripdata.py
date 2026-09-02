# Bronze layer: ingests the yellow taxi parquet files landed by
# src/nyc_data_analysis_project/main.py, via Auto Loader (one streaming flow per
# schema era, since NYC TLC has reused this file naming across 3 incompatible
# column layouts over 2009-2026). All three flows append into one target table,
# yellow_tripdata_raw, normalized to logic.CANONICAL_SCHEMA.
#
# The actual normalization logic lives in bronze_logic.py (no pyspark.pipelines
# import there) so it can be unit tested outside a live pipeline — see
# tests/test_bronze_logic.py.
# Lakeflow automatically adds each pipeline source file's own directory to
# sys.path, so a same-directory module resolves via a plain top-level import
# with no manual path manipulation needed. (An earlier version of this file
# used a __file__-based sys.path.insert instead — that crashed with
# `NameError: name '__file__' is not defined`, since __file__ isn't set in the
# pipeline execution context.) The module is named bronze_logic, not logic —
# Lakeflow's sys.path additions are shared across the whole pipeline, not
# scoped per source directory, so a same-named silver/logic.py would collide
# with this one via sys.modules and silently win depending on load order.
from bronze_logic import ERA_2009_COLUMNS, ERA_2010_COLUMNS, MODERN_COLUMNS, normalize
from pyspark import pipelines as dp


def _read_era_stream(subfolder):
    """Open an Auto Loader stream over one era's landed parquet subfolder.

    Args:
        subfolder: Era subfolder name under the source volume
            ("legacy_2009", "legacy_2010", or "modern").
    """
    volume_path = spark.conf.get("source_volume_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"{volume_path}/{subfolder}")
    )


dp.create_streaming_table(
    name="yellow_tripdata_raw",
    comment=(
        "Yellow taxi trip records normalized to one unified schema, incrementally "
        "ingested via Auto Loader (one flow per schema era) so re-running the "
        "pipeline only processes newly landed months, not the whole history."
    ),
)


@dp.append_flow(target="yellow_tripdata_raw", name="yellow_tripdata_legacy_2009")
def yellow_tripdata_legacy_2009():
    """Append flow: normalize and stream the 2009-era files into yellow_tripdata_raw."""
    return normalize(_read_era_stream("legacy_2009"), ERA_2009_COLUMNS)


@dp.append_flow(target="yellow_tripdata_raw", name="yellow_tripdata_legacy_2010")
def yellow_tripdata_legacy_2010():
    """Append flow: normalize and stream the 2010-era files into yellow_tripdata_raw."""
    return normalize(_read_era_stream("legacy_2010"), ERA_2010_COLUMNS)


@dp.append_flow(target="yellow_tripdata_raw", name="yellow_tripdata_modern")
def yellow_tripdata_modern():
    """Append flow: normalize and stream the 2011-onward files into yellow_tripdata_raw."""
    return normalize(_read_era_stream("modern"), MODERN_COLUMNS)
