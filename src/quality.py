"""Cleaning, typing and data-quality rules for pollutant readings."""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# India's approximate bounding box (generous): catches swapped or zeroed coordinates
LAT_RANGE, LON_RANGE = (6.0, 38.0), (68.0, 98.0)
MAX_SUBINDEX = 500  # CPCB sub-indices run 0-500


def type_readings(raw: DataFrame) -> DataFrame:
    """Cast the string columns; the feed's 'NA' becomes NULL, state names lose their underscores."""
    return (
        raw.withColumn("state", F.regexp_replace("state", "_", " "))
        .withColumn("last_update", F.to_timestamp("last_update", "dd-MM-yyyy HH:mm:ss"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("min_value", F.col("min_value").cast("double"))
        .withColumn("max_value", F.col("max_value").cast("double"))
        .withColumn("avg_value", F.col("avg_value").cast("double"))
    )


def build_rules() -> list[tuple[str, Column]]:
    """Ordered (rule, 'row is valid' condition); a row's reject reason is the FIRST rule it fails."""
    return [
        ("invalid_coordinates",
         F.col("latitude").between(*LAT_RANGE) & F.col("longitude").between(*LON_RANGE)),
        ("missing_average", F.col("avg_value").isNotNull()),
        ("subindex_out_of_range", F.col("avg_value").between(0, MAX_SUBINDEX)),
        ("min_max_inconsistent",
         F.col("min_value").isNull() | F.col("max_value").isNull()
         | ((F.col("min_value") <= F.col("avg_value")) & (F.col("avg_value") <= F.col("max_value")))),
        ("missing_timestamp", F.col("last_update").isNotNull()),
    ]


def tag_rejects(df: DataFrame, rules: list[tuple[str, Column]]) -> DataFrame:
    """Add `reject_reason` (NULL = valid). A rule that evaluates to NULL counts as a failure."""
    reason = F.lit(None).cast("string")
    for name, valid in reversed(rules):
        reason = F.when(~F.coalesce(valid, F.lit(False)), F.lit(name)).otherwise(reason)
    return df.withColumn("reject_reason", reason)


def quality_report(tagged: DataFrame) -> dict:
    by_reason = {r["reject_reason"]: r["count"] for r in tagged.groupBy("reject_reason").count().collect()}
    clean = by_reason.pop(None, 0)
    rejected = sum(by_reason.values())
    total = clean + rejected
    return {
        "rows_in": total, "rows_clean": clean, "rows_rejected": rejected,
        "reject_rate_pct": round(100 * rejected / total, 2) if total else 0.0,
        "rejected_by_rule": dict(sorted(by_reason.items(), key=lambda kv: -kv[1])),
    }
