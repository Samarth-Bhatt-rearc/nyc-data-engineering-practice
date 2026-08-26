from datetime import datetime


class TestAddPickupLocation:
    def test_uses_loc_prefix_when_pickup_location_id_is_present(self, spark, price_hikes_logic):
        # 2011+ rows carry a real pickup_location_id.
        df = spark.createDataFrame([(237.0, None, None)], "pickup_location_id double, pickup_longitude double, pickup_latitude double")
        result = price_hikes_logic.add_pickup_location(df).collect()[0]
        assert result.pickup_location == "LOC_237.0"

    def test_falls_back_to_a_rounded_lat_long_grid_when_location_id_is_null(self, spark, price_hikes_logic):
        # 2009-2010 legacy rows have no location ID at all, only lat/long —
        # verified against real data this session that pickup_location_id is
        # NULL for those rows (not just absent), so this is the path they
        # always take. Rounded to 2 decimals (~1km) so nearby trips group
        # together instead of every exact coordinate being its own bucket.
        df = spark.createDataFrame(
            [(None, -73.9857, 40.7484)], "pickup_location_id double, pickup_longitude double, pickup_latitude double"
        )
        result = price_hikes_logic.add_pickup_location(df).collect()[0]
        assert result.pickup_location == "GRID_-73.99_40.75"


class TestPctOfTotal:
    def test_computes_percent_share_of_the_combined_total(self, spark, price_hikes_logic):
        df = spark.createDataFrame([(2.8, 0.3)], "total_price_hikes double, total_improvement_surcharge double")
        result = df.select(price_hikes_logic.pct_of_total("total_improvement_surcharge").alias("pct")).collect()[0]
        assert result.pct == round(0.3 / 2.8 * 100, 2)

    def test_is_zero_not_null_or_a_divide_by_zero_error_when_total_is_zero(self, spark, price_hikes_logic):
        # Most (pickup_location, pickup_date) groups have no surcharges at all
        # (e.g. before congestion pricing existed) — this must resolve cleanly
        # to 0, not NULL or a Spark divide-by-zero failure.
        df = spark.createDataFrame([(0.0, 0.0)], "total_price_hikes double, total_improvement_surcharge double")
        result = df.select(price_hikes_logic.pct_of_total("total_improvement_surcharge").alias("pct")).collect()[0]
        assert result.pct == 0.0


class TestBuildPriceHikes:
    def _row(self, pickup_location_id, dt, improvement, congestion, airport, cbd, total_amount):
        return (pickup_location_id, None, None, dt, improvement, congestion, airport, cbd, total_amount)

    def _schema(self):
        return (
            "pickup_location_id double, pickup_longitude double, pickup_latitude double, "
            "pickup_datetime timestamp, improvement_surcharge double, congestion_surcharge double, "
            "airport_fee double, cbd_congestion_fee double, total_amount double"
        )

    def test_had_any_surcharge_is_false_when_every_fee_is_zero(self, spark, price_hikes_logic):
        df = spark.createDataFrame(
            [self._row(200.0, datetime(2020, 1, 1, 11, 0), 0.0, 0.0, 0.0, 0.0, 15.0)], self._schema()
        )
        result = price_hikes_logic.build_price_hikes(df).collect()[0]

        assert result.had_any_surcharge is False
        assert result.had_improvement_surcharge is False
        assert result.pct_improvement_surcharge == 0.0

    def test_percent_contributions_sum_to_the_whole_when_surcharges_exist(self, spark, price_hikes_logic):
        df = spark.createDataFrame(
            [self._row(100.0, datetime(2020, 1, 1, 10, 0), 0.3, 2.5, 0.0, 0.0, 20.0)], self._schema()
        )
        result = price_hikes_logic.build_price_hikes(df).collect()[0]

        assert result.had_any_surcharge is True
        assert result.had_improvement_surcharge is True
        assert result.had_congestion_surcharge is True
        assert result.had_airport_fee is False
        total_pct = (
            result.pct_improvement_surcharge
            + result.pct_congestion_surcharge
            + result.pct_airport_fee
            + result.pct_cbd_congestion_fee
        )
        assert round(total_pct, 1) == 100.0

    def test_groups_separately_by_pickup_location_and_date(self, spark, price_hikes_logic):
        df = spark.createDataFrame(
            [
                self._row(100.0, datetime(2020, 1, 1, 10, 0), 0.3, 0.0, 0.0, 0.0, 20.0),
                self._row(200.0, datetime(2020, 1, 1, 11, 0), 0.0, 0.0, 0.0, 0.0, 15.0),
            ],
            self._schema(),
        )
        result = price_hikes_logic.build_price_hikes(df)

        assert result.select("pickup_location").distinct().count() == 2
