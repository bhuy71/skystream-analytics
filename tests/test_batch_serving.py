"""
tests/test_batch_serving.py

Unit tests for Batch Layer (05) and Serving Layer (06) logic.
"""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, TimestampType, IntegerType
)
from datetime import datetime, timedelta


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def silver_30d(spark):
    """30 days of silver flight data for batch tests."""
    now = datetime.now()
    rows = [
        # airline_icao, icao24, on_ground, velocity_kmh, altitude_ft, vertical_rate, origin_country, is_cargo, lat, lon, snapshot_time
        ("VNA", "abc001", False, 882.0, 35000, 0.1, "Vietnam",       False,  10.8, 106.7, now - timedelta(days=1)),
        ("VNA", "abc002", False, 878.0, 35100, -0.2,"Vietnam",       False,  10.9, 106.8, now - timedelta(days=1)),
        ("VNA", "abc003", True,  0.0,   0,     0.0, "Vietnam",       False,  10.8, 106.7, now - timedelta(days=2)),
        ("UAL", "xyz001", False, 910.0, 36000, 0.0, "United States", False,  41.9, -87.6, now - timedelta(days=1)),
        ("UAL", "xyz002", False, 905.0, 35800, 0.2, "United States", False,  41.8, -87.5, now - timedelta(days=1)),
        ("UAL", "xyz003", False, 895.0, 35500, 0.1, "United States", False,  41.7, -87.4, now - timedelta(days=1)),
        ("FDX", "fdx001", False, 876.0, 34000, 0.0, "United States", True,   33.9, -118.4,now - timedelta(days=1)),
        ("FDX", "fdx002", False, 871.0, 34200, 0.1, "United States", True,   34.0, -118.3,now - timedelta(days=2)),
        ("ANA", "ana001", False, 893.0, 35900, 0.0, "Japan",         False,  35.7, 139.7, now - timedelta(days=1)),
        ("ANA", "ana002", False, 897.0, 36100, -0.1,"Japan",         False,  35.8, 139.8, now - timedelta(days=1)),
        # rapid descent
        ("VNA", "abc004", False, 650.0, 5000, -15.0,"Vietnam",       False,  10.5, 106.5, now - timedelta(days=3)),
    ]
    schema = StructType([
        StructField("airline_icao",    StringType(),    True),
        StructField("icao24",          StringType(),    True),
        StructField("on_ground",       BooleanType(),   True),
        StructField("velocity_kmh",    DoubleType(),    True),
        StructField("altitude_ft",     IntegerType(),   True),
        StructField("vertical_rate",   DoubleType(),    True),
        StructField("origin_country",  StringType(),    True),
        StructField("is_cargo",        BooleanType(),   True),
        StructField("latitude",        DoubleType(),    True),
        StructField("longitude",       DoubleType(),    True),
        StructField("snapshot_time",   TimestampType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# ── Batch Layer Tests ──────────────────────────────────────────────────────────

class TestBatchAirlineReliability:

    def test_airborne_pct_calculated_correctly(self, silver_30d):
        """VNA has 3 records: 2 airborne, 1 on_ground → airborne_pct = 66.67%"""
        result = (
            silver_30d
            .filter(F.col("airline_icao") == "VNA")
            .agg(
                (F.sum(F.when(~F.col("on_ground"), 1).otherwise(0)) / F.count("*") * 100)
                .alias("airborne_pct")
            )
        )
        pct = result.collect()[0]["airborne_pct"]
        assert abs(pct - 75.0) < 1.0  # 3 airborne out of 4 VNA records

    def test_rapid_descent_detection(self, silver_30d):
        """VNA abc004 has vertical_rate=-15.0 → should be flagged."""
        rapid = silver_30d.filter(
            (F.col("airline_icao") == "VNA") & (F.col("vertical_rate") < -10)
        )
        assert rapid.count() == 1
        row = rapid.collect()[0]
        assert row["icao24"] == "abc004"

    def test_reliability_score_ordering(self, silver_30d):
        """ANA (no rapid descents, stable speed) should score higher than VNA."""
        def compute_score(df, airline):
            row = (
                df.filter(F.col("airline_icao") == airline)
                .agg(
                    (F.sum(F.when(~F.col("on_ground"), 1).otherwise(0)) / F.count("*") * 100)
                    .alias("airborne_pct"),
                    F.stddev("velocity_kmh").alias("speed_stddev"),
                    F.avg(F.abs("vertical_rate")).alias("avg_abs_vrate"),
                )
                .collect()[0]
            )
            airborne = row["airborne_pct"] or 0
            stddev   = row["speed_stddev"] or 1
            vrate    = row["avg_abs_vrate"] or 0
            return airborne * 0.4 + 100 * 0.3 / (stddev / 50 + 1) + 100 * 0.3 / (vrate / 2 + 1)

        score_ana = compute_score(silver_30d, "ANA")
        score_vna = compute_score(silver_30d, "VNA")
        assert score_ana > score_vna, f"ANA ({score_ana:.1f}) should beat VNA ({score_vna:.1f})"

    def test_filters_airlines_with_sparse_data(self, silver_30d):
        """Airlines with < 100 snapshots should be filtered out — all our test airlines have < 100."""
        result = silver_30d.groupBy("airline_icao").count().filter(F.col("count") >= 100)
        assert result.count() == 0  # test data has < 100 per airline, all filtered out

    def test_cargo_flag_is_preserved(self, silver_30d):
        """FDX flights should all be is_cargo=True."""
        fdx = silver_30d.filter(F.col("airline_icao") == "FDX")
        non_cargo = fdx.filter(~F.col("is_cargo"))
        assert non_cargo.count() == 0


class TestBatchRouteGrowth:

    def test_growth_rate_positive_when_current_exceeds_prior(self, spark):
        """growth_rate_pct should be positive when current > prior period."""
        from pyspark.sql import Window
        rows = [
            ("VNA", "Vietnam", 50.0, "2024-01-01"),
            ("VNA", "Vietnam", 60.0, "2024-01-08"),
            ("VNA", "Vietnam", 70.0, "2024-01-15"),
            ("VNA", "Vietnam", 80.0, "2024-01-22"),
            ("VNA", "Vietnam", 100.0,"2024-01-29"),
            ("VNA", "Vietnam", 110.0,"2024-02-05"),
            ("VNA", "Vietnam", 120.0,"2024-02-12"),
            ("VNA", "Vietnam", 130.0,"2024-02-19"),
        ]
        df = spark.createDataFrame(rows, ["airline_icao", "origin_country", "unique_flights", "week"])
        df = df.withColumn("week", F.to_date("week"))

        window_cur  = Window.partitionBy("airline_icao","origin_country").orderBy("week").rowsBetween(-3, 0)
        window_prev = Window.partitionBy("airline_icao","origin_country").orderBy("week").rowsBetween(-7, -4)

        result = df \
            .withColumn("avg_cur",  F.avg("unique_flights").over(window_cur)) \
            .withColumn("avg_prev", F.avg("unique_flights").over(window_prev)) \
            .withColumn("growth_pct",
                ((F.col("avg_cur") - F.col("avg_prev")) / (F.col("avg_prev") + 1) * 100)
            ) \
            .orderBy(F.col("week").desc()).limit(1)

        growth = result.collect()[0]["growth_pct"]
        assert growth > 0, f"Expected positive growth, got {growth}"

    def test_growth_rate_negative_when_declining(self, spark):
        rows = [
            ("RYR", "Ireland", 200.0, "2024-01-01"),
            ("RYR", "Ireland", 180.0, "2024-01-08"),
            ("RYR", "Ireland", 160.0, "2024-01-15"),
            ("RYR", "Ireland", 140.0, "2024-01-22"),
            ("RYR", "Ireland", 90.0,  "2024-01-29"),
            ("RYR", "Ireland", 80.0,  "2024-02-05"),
            ("RYR", "Ireland", 70.0,  "2024-02-12"),
            ("RYR", "Ireland", 60.0,  "2024-02-19"),
        ]
        df = spark.createDataFrame(rows, ["airline_icao","origin_country","unique_flights","week"])
        df = df.withColumn("week", F.to_date("week")).withColumn("unique_flights", F.col("unique_flights").cast(DoubleType()))

        from pyspark.sql import Window
        window_cur  = Window.partitionBy("airline_icao","origin_country").orderBy("week").rowsBetween(-3, 0)
        window_prev = Window.partitionBy("airline_icao","origin_country").orderBy("week").rowsBetween(-7, -4)

        result = df \
            .withColumn("avg_cur",  F.avg("unique_flights").over(window_cur)) \
            .withColumn("avg_prev", F.avg("unique_flights").over(window_prev)) \
            .withColumn("growth_pct",
                ((F.col("avg_cur") - F.col("avg_prev")) / (F.col("avg_prev") + 1) * 100)
            ) \
            .orderBy(F.col("week").desc()).limit(1)

        growth = result.collect()[0]["growth_pct"]
        assert growth < 0, f"Expected negative growth for declining route, got {growth}"


class TestBatchFuelEfficiency:

    def test_efficiency_grade_assignment(self, spark):
        """Test grade boundaries: <2.5=A, <3.0=B, <3.5=C, else D."""
        rows = [(2.1,), (2.7,), (3.3,), (3.8,)]
        df = spark.createDataFrame(rows, ["est_co2_kg_per_km"])
        result = df.withColumn(
            "grade",
            F.when(F.col("est_co2_kg_per_km") < 2.5, "A")
             .when(F.col("est_co2_kg_per_km") < 3.0, "B")
             .when(F.col("est_co2_kg_per_km") < 3.5, "C")
             .otherwise("D")
        ).collect()

        assert result[0]["grade"] == "A"
        assert result[1]["grade"] == "B"
        assert result[2]["grade"] == "C"
        assert result[3]["grade"] == "D"

    def test_esg_risk_flag_from_grade(self, spark):
        rows = [("A",), ("B",), ("C",), ("D",)]
        df = spark.createDataFrame(rows, ["efficiency_grade"])
        result = df.withColumn(
            "esg_risk_flag",
            F.when(F.col("efficiency_grade") == "D", "HIGH_RISK")
             .when(F.col("efficiency_grade") == "C", "MEDIUM_RISK")
             .otherwise("LOW_RISK")
        ).collect()

        assert result[0]["esg_risk_flag"] == "LOW_RISK"
        assert result[1]["esg_risk_flag"] == "LOW_RISK"
        assert result[2]["esg_risk_flag"] == "MEDIUM_RISK"
        assert result[3]["esg_risk_flag"] == "HIGH_RISK"


# ── Serving Layer Tests ────────────────────────────────────────────────────────

class TestServingLayer:

    def test_investment_signal_strong_buy(self, spark):
        """growth>10% AND demand>50 → STRONG_BUY."""
        rows = [
            ("VNA", "Vietnam",   23.4, 78.5),
            ("THA", "Thailand",  -8.2, 31.2),
            ("SIA", "Singapore", 15.7, 65.4),
            ("RYR", "Ireland",   -15.0, 20.0),
        ]
        df = spark.createDataFrame(rows, ["airline_icao","origin_country","growth_rate_pct","live_demand_score"])
        result = df.withColumn(
            "investment_signal",
            F.when(
                (F.col("growth_rate_pct") > 10) & (F.col("live_demand_score") > 50),
                "STRONG_BUY"
            ).when(
                (F.col("growth_rate_pct") > 5) | (F.col("live_demand_score") > 30),
                "BUY"
            ).when(F.col("growth_rate_pct") < -10, "SELL")
            .otherwise("HOLD")
        ).collect()

        signals = {r["airline_icao"]: r["investment_signal"] for r in result}
        assert signals["VNA"] == "STRONG_BUY"
        assert signals["SIA"] == "STRONG_BUY"
        assert signals["THA"] == "BUY"    # demand>30 but growth not > 10
        assert signals["RYR"] == "SELL"

    def test_operational_status_thresholds(self, spark):
        """Test congestion_score → operational_status mapping."""
        rows = [(0.95,), (0.75,), (0.45,), (0.10,)]
        df = spark.createDataFrame(rows, ["live_congestion_score"])
        result = df.withColumn(
            "operational_status",
            F.when(F.col("live_congestion_score") > 0.8, "CRITICAL")
             .when(F.col("live_congestion_score") > 0.6, "HIGH")
             .when(F.col("live_congestion_score") > 0.3, "NORMAL")
             .otherwise("LOW")
        ).collect()

        statuses = [r["operational_status"] for r in result]
        assert statuses == ["CRITICAL", "HIGH", "NORMAL", "LOW"]

    def test_tourism_vs_seasonal_avg(self, spark):
        """Test tourism signal classification."""
        rows = [
            ("HCM",    234.5, 312.0),   # +33.1% → ABOVE_TREND
            ("Phuket", 187.3, 145.0),   # -22.5% → BELOW_TREND
            ("Dubai",  456.2, 478.0),   # +4.8%  → ON_TREND
        ]
        df = spark.createDataFrame(rows, ["region","avg_weekly_flights_52w","live_inbound_flights"])
        result = df \
            .withColumn(
                "vs_seasonal_avg_pct",
                ((F.col("live_inbound_flights") - F.col("avg_weekly_flights_52w"))
                 / (F.col("avg_weekly_flights_52w") + 1) * 100)
            ) \
            .withColumn(
                "tourism_signal",
                F.when(F.col("vs_seasonal_avg_pct") > 20, "ABOVE_TREND")
                 .when(F.col("vs_seasonal_avg_pct") < -20, "BELOW_TREND")
                 .otherwise("ON_TREND")
            ).collect()

        signals = {r["region"]: r["tourism_signal"] for r in result}
        assert signals["HCM"]    == "ABOVE_TREND"
        assert signals["Phuket"] == "BELOW_TREND"
        assert signals["Dubai"]  == "ON_TREND"

    def test_serving_join_preserves_all_batch_records(self, spark):
        """Left join with speed data should not lose batch records."""
        batch = spark.createDataFrame(
            [("VNA",), ("ANA",), ("UAL",), ("RYR",)],
            ["airline_icao"]
        )
        speed = spark.createDataFrame(
            [("VNA", 134, 1.34), ("UAL", 892, 8.92)],
            ["airline_icao", "live_active_flights", "live_market_share_pct"]
        )
        result = batch.join(speed, "airline_icao", "left")
        # All 4 batch records preserved
        assert result.count() == 4
        # ANA and RYR have null live data
        nulls = result.filter(F.col("live_active_flights").isNull())
        assert nulls.count() == 2
