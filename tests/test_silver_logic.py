from datetime import datetime


class TestTrimWhitespace:
    def test_trims_string_columns_only(self, spark, silver_logic):
        # Regression test: an earlier version of this function called F.trim()
        # on every column indiscriminately, which raised AnalysisException the
        # moment it hit a non-string column (this table has timestamp/double/int
        # columns alongside the string ones) — the pipeline never even got past
        # this step. Confirming both halves here: strings DO get trimmed, and
        # non-strings survive at all (not just "don't error").
        df = spark.createDataFrame(
            [(" Cash ", datetime(2020, 1, 1), 3.5)],
            "payment_type string, pickup_datetime timestamp, trip_distance double",
        )

        result = silver_logic.trim_whitespace(df).collect()[0]

        assert result.payment_type == "Cash"
        assert result.pickup_datetime == datetime(2020, 1, 1)
        assert result.trip_distance == 3.5


class TestMappingWithDefaults:
    def test_matches_modern_digit_codes(self, spark, silver_logic):
        df = spark.createDataFrame([("1",)], "payment_type string")
        result = silver_logic.mapping_with_defaults(df, silver_logic.PAYMENT_TYPE_LOOKUP, "payment_type").collect()[0]
        assert result.payment_type == "Credit Card"

    def test_matches_legacy_abbreviations_case_insensitively(self, spark, silver_logic):
        # Lookup keys are stored in initcap form ("Csh"); the raw legacy value
        # can be any casing ("CSH", "csh", ...) since F.initcap() normalizes it
        # before comparison.
        df = spark.createDataFrame([("CSH",)], "payment_type string")
        result = silver_logic.mapping_with_defaults(df, silver_logic.PAYMENT_TYPE_LOOKUP, "payment_type").collect()[0]
        assert result.payment_type == "Cash"

    def test_unmapped_value_falls_back_to_the_given_default(self, spark, silver_logic):
        df = spark.createDataFrame([("totally-unknown-code",)], "payment_type string")
        result = silver_logic.mapping_with_defaults(
            df, silver_logic.PAYMENT_TYPE_LOOKUP, "payment_type", default_value="Unknown"
        ).collect()[0]
        assert result.payment_type == "Unknown"

    def test_renaming_to_a_new_column_still_matches_against_the_initcapped_value(self, spark, silver_logic):
        # Regression test: an earlier version compared against the *original*
        # (un-initcapped) column when new_column_name differs from column_name,
        # so raw "CMT" never matched the lookup's "Cmt" key and silently fell
        # through to "Unknown" for every legacy CMT row. Also checks the
        # original column is left untouched, since vendor_id (raw code) and
        # vendor_name (cleaned label) are meant to coexist.
        df = spark.createDataFrame([("CMT",)], "vendor_id string")
        result = silver_logic.mapping_with_defaults(
            df, silver_logic.VENDOR_LOOKUP, "vendor_id", new_column_name="vendor_name"
        ).collect()[0]

        assert result.vendor_name == "Creative Mobile Technologies, LLC"
        assert result.vendor_id == "CMT"  # original column untouched

    def test_store_and_fwd_flag_normalizes_legacy_numeric_encodings(self, spark, silver_logic):
        # Bronze casts 2009's numeric store_and_forward flag straight to string
        # ("0.0"/"1.0"), unlike the modern "Y"/"N" strings — both must resolve
        # to the same Y/N domain.
        df = spark.createDataFrame([("1.0",), ("0.0",), ("Y",)], "store_and_fwd_flag string")
        results = silver_logic.mapping_with_defaults(
            df, silver_logic.STORE_AND_FWD_FLAG_LOOKUP, "store_and_fwd_flag", default_value="N"
        ).collect()
        assert {r.store_and_fwd_flag for r in results} == {"Y", "N"}


class TestReplacingNullsWithDefault:
    def test_replaces_null_and_preserves_non_null(self, spark, silver_logic):
        df = spark.createDataFrame([(None,), (42.0,)], "rate_code_id double")
        results = silver_logic.replacing_nulls_with_default(df, "rate_code_id", 99).collect()
        values = sorted(r.rate_code_id for r in results)
        assert values == [42.0, 99.0]
