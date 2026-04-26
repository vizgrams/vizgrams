# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""OpenFlights data source tool.

Downloads airports, airlines, and routes directly from the OpenFlights
dataset hosted on GitHub and yields clean normalised records.

Source: https://github.com/jpatokal/openflights (Open Database License)

Available commands
------------------
airports    IATA airport codes with geographic coordinates (~7 k records)
airlines    Active airline records with IATA/ICAO codes (~6 k records)
routes      Flight routes between airports (~67 k records)
"""

from __future__ import annotations

import csv
import io
import logging
from collections.abc import Iterator

import requests

from tools.base import BaseTool

logger = logging.getLogger(__name__)

_BASE_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data"

COMMANDS = ["airports", "airlines", "routes"]

# Column definitions for each .dat file (fixed-position CSV, no header row)
_AIRPORT_COLS = [
    "airport_id", "name", "city", "country",
    "iata_code", "icao_code",
    "latitude", "longitude", "altitude_ft",
    "timezone_offset", "dst", "tz_name", "type", "source",
]

_AIRLINE_COLS = [
    "airline_id", "name", "alias",
    "iata_code", "icao_code",
    "callsign", "country", "active",
]

_ROUTE_COLS = [
    "airline_code", "airline_id",
    "source_airport_code", "source_airport_id",
    "dest_airport_code", "dest_airport_id",
    "codeshare", "stops", "equipment",
]


def _null(value: str) -> str | None:
    """Convert OpenFlights null sentinel '\\N' to Python None."""
    return None if value == r"\N" else value


def _fetch(filename: str) -> str:
    url = f"{_BASE_URL}/{filename}"
    logger.info("OpenFlights: downloading %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def _parse_csv(text: str, fieldnames: list[str]) -> Iterator[dict]:
    reader = csv.DictReader(
        io.StringIO(text),
        fieldnames=fieldnames,
        quotechar='"',
    )
    for row in reader:
        yield {k: _null(v.strip() if v else v) for k, v in row.items()}


class OpenFlightsTool(BaseTool):
    """Downloads OpenFlights static datasets from GitHub on demand."""

    def __init__(self, config: dict | None = None, **_kwargs):
        pass

    def list_commands(self) -> list[str]:
        return list(COMMANDS)

    def run(self, command: str, params: dict | None = None) -> Iterator[dict]:
        handler = {
            "airports": self._airports,
            "airlines": self._airlines,
            "routes": self._routes,
        }.get(command)
        if handler is None:
            raise ValueError(
                f"Unknown OpenFlights command: {command!r}. "
                f"Available: {', '.join(COMMANDS)}"
            )
        yield from handler(params or {})

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    def _airports(self, _params: dict) -> Iterator[dict]:
        text = _fetch("airports.dat")
        for row in _parse_csv(text, _AIRPORT_COLS):
            iata = row.get("iata_code")
            if not iata or iata == r"\N":
                continue
            yield {
                "iata_code": iata,
                "icao_code": row["icao_code"],
                "name": row["name"],
                "city": row["city"],
                "country": row["country"],
                "latitude": float(row["latitude"]) if row["latitude"] else None,
                "longitude": float(row["longitude"]) if row["longitude"] else None,
                "altitude_ft": int(row["altitude_ft"]) if row["altitude_ft"] else None,
                "timezone": row["tz_name"],
            }

    def _airlines(self, _params: dict) -> Iterator[dict]:
        text = _fetch("airlines.dat")
        for row in _parse_csv(text, _AIRLINE_COLS):
            iata = row.get("iata_code")
            if not iata or iata == "-":
                continue
            yield {
                "iata_code": iata,
                "icao_code": row["icao_code"],
                "name": row["name"],
                "callsign": row["callsign"],
                "country": row["country"],
                "active": row["active"],
            }

    def _routes(self, _params: dict) -> Iterator[dict]:
        text = _fetch("routes.dat")
        seen: set[str] = set()
        for row in _parse_csv(text, _ROUTE_COLS):
            airline = row.get("airline_code")
            src = row.get("source_airport_code")
            dst = row.get("dest_airport_code")
            if not airline or not src or not dst:
                continue
            # Deduplicate codeshare duplicates on the same city-pair
            key = f"{airline}-{src}-{dst}"
            if key in seen:
                continue
            seen.add(key)
            yield {
                "route_id": key,
                "airline_code": airline,
                "source_airport_code": src,
                "dest_airport_code": dst,
                "codeshare": row["codeshare"] == "Y" if row["codeshare"] else False,
                "stops": int(row["stops"]) if row["stops"] else 0,
                "equipment": row["equipment"],
            }
