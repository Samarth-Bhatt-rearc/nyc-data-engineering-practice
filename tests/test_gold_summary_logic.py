class TestBuildTripSummary:
    def test_groups_by_year_month_vendor_payment_type(self, spark, summary_logic):
        df = spark.createDataFrame(
            [
                (2020, 1, "Vendor A", "Cash", 3.0),
                (2020, 1, "Vendor A", "Cash", 5.0),
                (2020, 1, "Vendor A", "Credit Card", 10.0),
            ],
            "year int, month int, vendor_name string, payment_type string, trip_distance double",
        )

        result = {(r.vendor_name, r.payment_type): r for r in summary_logic.build_trip_summary(df).collect()}

        assert len(result) == 2  # the two identical (Vendor A, Cash) rows merge into one group
        cash = result[("Vendor A", "Cash")]
        assert cash.total_distance == 8.0
        assert cash.avg_distance == 4.0
        assert cash.trip_count == 2

    def test_trip_count_uses_count_star_so_null_distance_rows_still_count(self, spark, summary_logic):
        # count("*") was chosen deliberately over count("trip_distance") so a
        # row with a null distance still contributes to trip_count (only
        # total_distance/avg_distance would be affected by the null, not the
        # count) — this test pins that choice down.
        df = spark.createDataFrame(
            [(2020, 1, "Vendor A", "Cash", None), (2020, 1, "Vendor A", "Cash", 5.0)],
            "year int, month int, vendor_name string, payment_type string, trip_distance double",
        )

        result = summary_logic.build_trip_summary(df).collect()[0]

        assert result.trip_count == 2
        assert result.total_distance == 5.0
