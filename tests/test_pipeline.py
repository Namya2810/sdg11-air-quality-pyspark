from pathlib import Path

from pyspark.sql import functions as F

from src.analytics import run_reports, station_table
from src.parse import COLUMNS, parse_snapshot
from src.quality import build_rules, quality_report, tag_rejects, type_readings

RAW = Path(__file__).resolve().parents[1] / "data" / "raw" / "data_aqi_cpcb_2023-11-20.xml"
TS = "20-11-2023 12:00:00"


def readings(spark, rows):
    """rows: (state, city, station, last_update, lat, lon, pollutant, min, max, avg) as the feed gives them (strings)."""
    full = [("snap",) + r for r in rows]
    return type_readings(spark.createDataFrame(full, COLUMNS))


def reasons(spark, rows):
    tagged = tag_rejects(readings(spark, rows), build_rules())
    return {r["pollutant"]: r["reject_reason"] for r in tagged.collect()}


def row(pollutant, avg, mn="1", mx="999", lat="28.6", lon="77.2", ts=TS, station="S1", city="C1"):
    return ("Delhi", city, station, ts, lat, lon, pollutant, mn, mx, avg)


def test_parse_real_snapshot_has_expected_shape():
    parsed = parse_snapshot(RAW)
    assert len(parsed) == 3285
    assert {r[7] for r in parsed} == {"PM2.5", "PM10", "SO2", "CO", "OZONE", "NO2", "NH3"}
    assert len({r[3] for r in parsed}) == 492  # station ids


def test_each_rule_rejects_its_row_and_good_row_passes(spark):
    got = reasons(spark, [
        row("PM2.5", "120", "100", "140"),               # valid
        row("PM10", "NA", "NA", "NA"),                   # feed's NA -> missing_average
        row("CO", "600"),                                # sub-index above 500
        row("NO2", "50", "60", "70"),                    # avg outside min..max
        row("SO2", "20", lat="0.0", lon="0.0"),          # coordinates outside India
        row("NH3", "20", ts="not a time"),               # unreadable timestamp
    ])
    assert got == {"PM2.5": None, "PM10": "missing_average", "CO": "subindex_out_of_range",
                   "NO2": "min_max_inconsistent", "SO2": "invalid_coordinates", "NH3": "missing_timestamp"}


def test_missing_min_max_is_allowed_but_quality_report_counts_add_up(spark):
    tagged = tag_rejects(readings(spark, [row("PM2.5", "80", "NA", "NA"), row("PM10", "NA")]), build_rules())
    report = quality_report(tagged)
    assert report == {"rows_in": 2, "rows_clean": 1, "rows_rejected": 1, "reject_rate_pct": 50.0,
                      "rejected_by_rule": {"missing_average": 1}}


def test_station_aqi_is_the_highest_subindex_with_its_pollutant(spark):
    clean = readings(spark, [row("PM2.5", "120"), row("PM10", "90"), row("NO2", "40")])
    _, valid = station_table(spark, clean)
    s = valid.collect()[0]
    assert s["aqi"] == 120 and s["dominant_pollutant"] == "PM2.5" and s["n_pollutants"] == 3


def test_station_needs_three_pollutants_including_a_particulate(spark):
    clean = readings(spark, [
        row("PM2.5", "100", station="A"), row("SO2", "20", station="A"),                                 # only 2
        row("SO2", "20", station="B"), row("NO2", "30", station="B"), row("CO", "60", station="B"),      # no PM
        row("PM10", "70", station="C"), row("NO2", "30", station="C"), row("CO", "60", station="C"),     # valid
    ])
    all_stations, valid = station_table(spark, clean)
    assert all_stations.count() == 3
    assert [r["station"] for r in valid.collect()] == ["C"]


def test_category_boundaries_and_summary(spark):
    clean = readings(spark, [
        row("PM2.5", "50", station="G"), row("PM10", "10", station="G"), row("NO2", "10", station="G"),
        row("PM2.5", "51", station="S"), row("PM10", "10", station="S"), row("NO2", "10", station="S"),
        row("PM2.5", "201", station="P", city="C2"), row("PM10", "10", station="P", city="C2"), row("NO2", "10", station="P", city="C2"),
    ])
    _, valid = station_table(spark, clean)
    reports = run_reports(spark, valid)
    cats = {r["category"]: r["stations"] for r in reports["category_distribution"].collect()}
    assert cats == {"1 Good": 1, "2 Satisfactory": 1, "4 Poor": 1}
    summary = reports["sdg11_summary"].collect()[0]
    assert summary["stations_with_valid_aqi"] == 3
    assert round(summary["pct_good_or_satisfactory"], 1) == 66.7 and round(summary["pct_poor_or_worse"], 1) == 33.3
