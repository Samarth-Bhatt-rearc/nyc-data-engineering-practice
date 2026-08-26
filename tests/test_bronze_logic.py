from pyspark.sql import functions as F


def _with_file_path(df, file_name, subfolder="modern"):
    # Auto Loader normally populates the hidden _metadata.file_path column from
    # the real file being read; normalize() relies on it to derive
    # source_file/year/month, so tests fake it the same way a landed file's path
    # would look.
    path = f"dbfs:/Volumes/catalog/schema/nyc_data_volume/{subfolder}/{file_name}"
    return df.withColumn("_metadata", F.struct(F.lit(path).alias("file_path")))


class TestNormalize2009Era:
    def test_renames_columns_and_derives_source_file_year_month(self, spark, bronze_logic):
        df = spark.createDataFrame(
            [("CMT", "2009-01-04 02:52:00", "2009-01-04 03:02:00", 2, 3.5, "10.5")],
            "vendor_name string, Trip_Pickup_DateTime string, Trip_Dropoff_DateTime string, "
            "Passenger_Count long, Trip_Distance double, Fare_Amt string",
        )
        df = _with_file_path(df, "yellow_tripdata_2009-01.parquet", "legacy_2009")

        result = bronze_logic.normalize(df, bronze_logic.ERA_2009_COLUMNS).collect()[0]

        assert result.vendor_id == "CMT"
        assert str(result.pickup_datetime) == "2009-01-04 02:52:00"
        assert result.passenger_count == 2.0  # long -> double cast
        assert result.fare_amount == 10.5  # string -> double cast, matches the 2009 era's actual raw type
        assert result.source_file == "yellow_tripdata_2009-01.parquet"
        assert result.year == 2009
        assert result.month == 1

    def test_columns_the_2009_era_has_no_concept_of_become_null_not_an_error(self, spark, bronze_logic):
        # 2009's raw schema has no location-ID columns at all (that's a 2011+
        # concept) — CANONICAL_SCHEMA must still produce the column, just null,
        # rather than raising or silently omitting it (every era must produce
        # the same output schema so unionByName-style appends stay consistent).
        df = spark.createDataFrame([("CMT",)], "vendor_name string")
        df = _with_file_path(df, "yellow_tripdata_2009-01.parquet", "legacy_2009")

        result = bronze_logic.normalize(df, bronze_logic.ERA_2009_COLUMNS).collect()[0]

        assert result.pickup_location_id is None
        assert result.dropoff_location_id is None


class TestNormalizeModernEra:
    def test_handles_both_airport_fee_casings(self, spark, bronze_logic):
        # Verified against real TLC data this session: airport_fee is lowercase
        # through ~2023, then "Airport_fee" (capital A) from 2024 on. Both must
        # land in the same canonical airport_fee column.
        lower = spark.createDataFrame([(1, 1.75)], "VendorID long, airport_fee double")
        upper = spark.createDataFrame([(1, 1.75)], "VendorID long, `Airport_fee` double")

        lower_result = bronze_logic.normalize(
            _with_file_path(lower, "yellow_tripdata_2022-01.parquet"), bronze_logic.MODERN_COLUMNS
        ).collect()[0]
        upper_result = bronze_logic.normalize(
            _with_file_path(upper, "yellow_tripdata_2024-01.parquet"), bronze_logic.MODERN_COLUMNS
        ).collect()[0]

        assert lower_result.airport_fee == 1.75
        assert upper_result.airport_fee == 1.75

    def test_absorbs_int32_vs_int64_id_type_drift(self, spark, bronze_logic):
        # Verified against real TLC data this session: VendorID/PULocationID
        # switch from int64 (pre-2024) to int32 (2024+). Casting to
        # CANONICAL_SCHEMA's fixed types must succeed either way, not just for
        # whichever type happened to be tested first.
        as_int32 = spark.createDataFrame([(1, 237)], "VendorID int, PULocationID int")
        as_int64 = spark.createDataFrame([(1, 237)], "VendorID long, PULocationID long")

        for df in (as_int32, as_int64):
            result = bronze_logic.normalize(
                _with_file_path(df, "yellow_tripdata_2020-01.parquet"), bronze_logic.MODERN_COLUMNS
            ).collect()[0]
            assert result.vendor_id == "1"  # cast to CANONICAL_SCHEMA's StringType
            assert result.pickup_location_id == 237.0  # cast to CANONICAL_SCHEMA's DoubleType

    def test_column_not_yet_introduced_in_an_earlier_modern_file_becomes_null(self, spark, bronze_logic):
        # cbd_congestion_fee only exists from 2025 onward; a 2020 file simply
        # won't have that column at all.
        df = spark.createDataFrame([(1,)], "VendorID long")
        result = bronze_logic.normalize(
            _with_file_path(df, "yellow_tripdata_2020-01.parquet"), bronze_logic.MODERN_COLUMNS
        ).collect()[0]

        assert result.cbd_congestion_fee is None
