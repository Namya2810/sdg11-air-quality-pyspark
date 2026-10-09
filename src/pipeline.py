"""SDG 11 air-quality ETL: parse CPCB XML -> validate/quarantine -> station AQI -> curated parquet -> Spark SQL reports.

Run:  python -m src.pipeline
"""
import argparse
import glob
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.analytics import run_reports, station_table
from src.parse import COLUMNS, parse_snapshot
from src.quality import build_rules, quality_report, tag_rejects, type_readings
from src.spark_session import get_spark, write_parquet

ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("aqi_pipeline")


def run(pattern: str) -> dict:
    (ROOT / "logs").mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(ROOT / "logs" / "pipeline.log", mode="w"), logging.StreamHandler()])
    files = sorted(glob.glob(str(ROOT / pattern)))
    if not files:
        raise FileNotFoundError(f"no input files match {pattern}")
    spark = get_spark("sdg11-air-quality", shuffle_partitions=4)
    spark.sparkContext.setLogLevel("ERROR")
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(exist_ok=True)
    timings: dict[str, float] = {}

    def stage(name: str, t0: float) -> None:
        timings[name] = round(time.perf_counter() - t0, 2)
        log.info("stage %-9s done in %.2fs", name, timings[name])

    # ---- extract
    t0 = time.perf_counter()
    rows = [r for f in files for r in parse_snapshot(Path(f))]
    schema = StructType([StructField(c, StringType()) for c in COLUMNS])
    raw = spark.createDataFrame(rows, schema)
    log.info("extract: %d snapshot file(s), %d pollutant readings", len(files), len(rows))
    stage("extract", t0)

    # ---- validate
    t0 = time.perf_counter()
    tagged = tag_rejects(type_readings(raw), build_rules()).persist(StorageLevel.MEMORY_AND_DISK)
    quality = quality_report(tagged)
    log.info("validate: %d in, %d clean, %d rejected (%.2f%%)", quality["rows_in"], quality["rows_clean"],
             quality["rows_rejected"], quality["reject_rate_pct"])
    for rule, n in quality["rejected_by_rule"].items():
        log.info("  rejected by %-22s %d", rule, n)
    stage("validate", t0)

    # ---- transform: station AQI
    t0 = time.perf_counter()
    clean = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejects = tagged.filter(F.col("reject_reason").isNotNull())
    all_stations, valid_stations = station_table(spark, clean)
    n_all, n_valid = all_stations.count(), valid_stations.count()
    log.info("stations: %d with readings, %d with a valid AQI (>=3 pollutants incl. PM)", n_all, n_valid)
    stage("transform", t0)

    # ---- load
    t0 = time.perf_counter()
    write_parquet(clean, ROOT / "data" / "curated" / "readings", ["state"])
    write_parquet(valid_stations, ROOT / "data" / "curated" / "station_aqi")
    write_parquet(rejects, ROOT / "data" / "quarantine" / "rejects")
    stage("load", t0)

    # ---- reports
    t0 = time.perf_counter()
    for name, frame in run_reports(spark, valid_stations).items():
        frame.toPandas().to_csv(reports_dir / f"{name}.csv", index=False)
        log.info("report written: %s.csv", name)
    stage("report", t0)

    summary = {
        "run_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "spark_version": spark.version,
        "input_files": [Path(f).name for f in files],
        "data_quality": quality,
        "stations_with_readings": n_all,
        "stations_with_valid_aqi": n_valid,
        "stage_seconds": timings,
        "total_seconds": round(sum(timings.values()), 2),
    }
    (reports_dir / "run_report.json").write_text(json.dumps(summary, indent=2))
    log.info("pipeline finished in %.2fs", summary["total_seconds"])
    spark.stop()
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="data/raw/*.xml", help="glob of CPCB AQI XML snapshot files")
    run(parser.parse_args().input)
