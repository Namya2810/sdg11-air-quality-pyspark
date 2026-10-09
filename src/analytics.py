"""Station AQI and the SDG 11 summary tables, computed with Spark SQL.

The feed's pollutant values are CPCB *sub-indices* (0-500), not raw concentrations: PM10 is lower than PM2.5 at 55% of
stations, which is impossible for concentrations. So a station's AQI is the highest sub-index among its pollutants
(the CPCB rule), valid only when at least 3 pollutants including PM2.5 or PM10 are reported.
"""
from pyspark.sql import DataFrame, SparkSession

STATION_AQI = """
    SELECT state, city, station, latitude, longitude,
           COUNT(*)                                  AS n_pollutants,
           MAX(pollutant IN ('PM2.5', 'PM10'))       AS has_pm,
           MAX(avg_value)                            AS aqi,
           MAX_BY(pollutant, avg_value)              AS dominant_pollutant
    FROM readings
    GROUP BY state, city, station, latitude, longitude
"""

CATEGORY = """
    CASE WHEN aqi <= 50 THEN '1 Good' WHEN aqi <= 100 THEN '2 Satisfactory' WHEN aqi <= 200 THEN '3 Moderate'
         WHEN aqi <= 300 THEN '4 Poor' WHEN aqi <= 400 THEN '5 Very Poor' ELSE '6 Severe' END
"""

REPORTS = {
    "category_distribution": f"""
        SELECT category, COUNT(*) AS stations, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_stations
        FROM (SELECT {CATEGORY} AS category FROM station_aqi) GROUP BY category ORDER BY category
    """,
    # window function: the 10 cities with the highest average station AQI
    "worst_cities_top10": """
        SELECT city_rank, city, state, stations, avg_aqi
        FROM (
            SELECT city, state, COUNT(*) AS stations, ROUND(AVG(aqi), 1) AS avg_aqi,
                   RANK() OVER (ORDER BY AVG(aqi) DESC) AS city_rank
            FROM station_aqi GROUP BY city, state
        )
        WHERE city_rank <= 10 ORDER BY city_rank, city
    """,
    "state_summary": """
        SELECT state, COUNT(*) AS stations, ROUND(AVG(aqi), 1) AS avg_aqi, MAX(aqi) AS max_aqi,
               ROUND(100.0 * AVG(CASE WHEN aqi > 200 THEN 1 ELSE 0 END), 1) AS pct_poor_or_worse
        FROM station_aqi GROUP BY state ORDER BY avg_aqi DESC
    """,
    "dominant_pollutant": """
        SELECT dominant_pollutant, COUNT(*) AS stations, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_stations
        FROM station_aqi GROUP BY dominant_pollutant ORDER BY stations DESC
    """,
    # SDG 11.6 (air quality in cities): how many stations meet 'good or satisfactory' air
    "sdg11_summary": """
        SELECT COUNT(*) AS stations_with_valid_aqi, COUNT(DISTINCT city) AS cities, COUNT(DISTINCT state) AS states,
               ROUND(100.0 * AVG(CASE WHEN aqi <= 100 THEN 1 ELSE 0 END), 1) AS pct_good_or_satisfactory,
               ROUND(100.0 * AVG(CASE WHEN aqi > 200 THEN 1 ELSE 0 END), 1)  AS pct_poor_or_worse
        FROM station_aqi
    """,
}


def station_table(spark: SparkSession, clean_readings: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (all stations with AQI, stations with a valid AQI)."""
    clean_readings.createOrReplaceTempView("readings")
    stations = spark.sql(STATION_AQI).withColumn("aqi_valid", _valid())
    return stations, stations.filter("aqi_valid").drop("aqi_valid")


def _valid():
    from pyspark.sql import functions as F
    return (F.col("n_pollutants") >= 3) & F.col("has_pm")


def run_reports(spark: SparkSession, valid_stations: DataFrame) -> dict[str, DataFrame]:
    valid_stations.createOrReplaceTempView("station_aqi")
    return {name: spark.sql(sql) for name, sql in REPORTS.items()}
