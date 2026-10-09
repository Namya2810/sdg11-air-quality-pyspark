# SDG 11 Air Quality Analytics (PySpark)

A PySpark batch pipeline that turns India's CPCB real-time air-quality feed into a validated station-level AQI table and
city/state summaries, to look at **UN SDG 11 (Sustainable Cities and Communities)** through air quality (target 11.6).

Data: [Real time Air Quality Index](https://data.gov.in/catalog/real-time-air-quality-index), CPCB / Ministry of Environment,
Forest and Climate Change (published under the National Data Sharing and Accessibility Policy). The snapshot included here
is from **20 Nov 2023, 12:00**: 31 states and UTs, 265 cities, 492 stations, 3,285 pollutant readings, 7 pollutants
(PM2.5, PM10, NO2, SO2, CO, ozone, NH3).

## Pipeline

```
data/raw/*.xml ──► parse XML ──► type + validate ──► station AQI (Spark SQL) ──► data/curated (parquet)
                                     └── rejected rows ──► data/quarantine (parquet, with reject_reason)
                                                          └── reports/*.csv + run_report.json
```

| Stage | What it does |
|---|---|
| Parse | Flattens Country > State > City > Station > Pollutant into one row per reading (`src/parse.py`) |
| Validate | Casts types (the feed's `NA` becomes NULL) and applies 5 ordered rules; each failing row is quarantined with the first rule it breaks: invalid coordinates, missing average, sub-index outside 0-500, min/max inconsistent, missing timestamp |
| Transform | Station AQI = highest pollutant sub-index, valid only if at least 3 pollutants including PM2.5 or PM10 are reported (the CPCB rule) |
| Report | Spark SQL: category distribution, 10 worst cities (window function), state summary, dominant pollutant, SDG 11 summary |

## Finding: the feed already contains sub-indices

The `Pollutant_Index` values are CPCB **sub-indices (0-500)**, not concentrations in ug/m3. Evidence: PM10 is lower than PM2.5 at
55% of stations (median PM10/PM2.5 ratio 0.94), which is impossible for concentrations, and Delhi's CO values sit around 100.
So no breakpoint interpolation is applied; the station AQI is simply the maximum sub-index.

## Results (20 Nov 2023 snapshot)

| Metric | Value |
|---|---|
| Readings in / clean / rejected | 3,285 / 3,065 / 220 (6.7%, all `missing_average`) |
| Stations with readings / with a valid AQI | 479 / 454 (244 cities, 26 states) |
| Stations with Good or Satisfactory air | 35.9% |
| Stations with Poor, Very Poor or Severe air | 31.3% |
| Dominant pollutant | PM2.5 at 53.7% of stations, PM10 at 40.3% |
| Run time | 67.7 s on a laptop |

- **Delhi:** all 38 stations with a valid AQI are Poor or worse (average AQI 343.2).
- **Worst cities by average station AQI:** Sri Ganganagar (419), Hanumangarh (394), Sonipat (367), Bahadurgarh (359), Fatehabad (344), then Delhi.
- **Worst states by average AQI:** Delhi (343), Haryana (251), Rajasthan (242), Punjab (219), Uttar Pradesh (191).

Full tables are in `reports/`.

## Run it

Requirements: Python 3.10-3.12, Java 11 or 17 (`JAVA_HOME` set).

```bash
pip install -r requirements.txt
python -m src.pipeline            # reads data/raw/*.xml
python -m pytest -q               # 6 tests
```

## Limits

- This is **one hourly snapshot on one day**, not a time series. Spark is more than this data size needs; the pipeline takes a glob
  of snapshot files (`--input "data/raw/*.xml"`) so more snapshots can be added, but it has only been run on this one.
- The AQI is the maximum of the feed's reported sub-indices. The official CPCB AQI uses 24-hour averages (8-hour for CO and ozone),
  so treat the numbers as indicative.
- SDG indicator 11.6.2 is the annual mean PM2.5 in cities; this project does not compute it.
- On Windows the curated layer is written with PyArrow instead of Spark's writer (which needs Hadoop's winutils); see `src/spark_session.py`.

## Source data

`source_data/` keeps the original data.gov.in download (feed XML plus the catalog metadata CSV) for provenance. The pipeline reads the copy in `data/raw/`.
