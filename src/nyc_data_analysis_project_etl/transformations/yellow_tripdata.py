import dlt


@dlt.table(
    name="yellow_tripdata_raw",
    comment=(
        "Yellow taxi trip records read from the Unity Catalog volume they were "
        "landed into from the NYC TLC public dataset."
    ),
)
def yellow_tripdata_raw():
    volume_path = spark.conf.get("source_volume_path")
    return spark.read.option("mergeSchema", "true").parquet(f"{volume_path}/*.parquet")
