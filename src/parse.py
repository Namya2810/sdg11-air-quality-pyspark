"""Flatten the CPCB real-time AQI XML (Country > State > City > Station > Pollutant_Index) into rows."""
import xml.etree.ElementTree as ET
from pathlib import Path

COLUMNS = ["snapshot", "state", "city", "station", "last_update", "latitude", "longitude",
           "pollutant", "min_value", "max_value", "avg_value"]


def parse_snapshot(path: Path) -> list[tuple]:
    """One row per (station, pollutant). Values stay strings here: the feed uses 'NA' for missing."""
    snapshot = Path(path).stem
    rows = []
    for state in ET.parse(path).getroot().iter("State"):
        for city in state.iter("City"):
            for station in city.iter("Station"):
                for p in station.iter("Pollutant_Index"):
                    rows.append((
                        snapshot, state.get("id"), city.get("id"), station.get("id"), station.get("lastupdate"),
                        station.get("latitude"), station.get("longitude"),
                        p.get("id"), p.get("Min"), p.get("Max"), p.get("Avg"),
                    ))
    return rows
