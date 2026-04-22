# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Garmin Connect data source tool.

Fetches workout, wellness, and biometric data from the unofficial Garmin
Connect API via the garminconnect Python library (pip install garminconnect).

Authentication uses email + password — ``login()`` performs the SSO
handshake automatically on each run, exactly like a standalone script.

Optionally set ``token_store: file:~/.secrets/garmin_tokens.json`` in
config.yaml to cache the garth session and skip the ~1–2s SSO roundtrip
on repeat runs.  The file is created automatically on first login.

Available commands
------------------
activities          Workout summaries (running, cycling, swimming, etc.)
wellness            Daily wellness snapshot (steps, calories, stress, body battery)
sleep               Nightly sleep with stage breakdown (deep / light / REM / awake)
heart_rate          Daily resting HR and HR statistics
body_composition    Weight, BMI, body-fat %, muscle mass (when synced from scale)
hrv                 Nightly HRV status and weekly average
personal_records    All-time personal best records per activity type

Suggested additional commands (not yet implemented)
---------------------------------------------------
hydration           Daily fluid intake (requires manual logging in Connect)
training_readiness  Garmin's combined readiness score (HRV + sleep + history)
respiration         Resting respiration rate per day
spo2                Blood-oxygen saturation data
body_battery        Body battery level readings throughout the day (intraday)
race_predictions    Predicted finish times derived from recent VO2 max
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from typing import Any

from tools.base import BaseTool

logger = logging.getLogger(__name__)

# Module-level cache: one authenticated client per email address.
# Shared across all GarminTool instances within a process so that the SSO
# handshake is performed only once even when the extractor creates a new
# GarminTool instance per task.
_client_cache: dict[str, Any] = {}
_cache_lock = threading.Lock()

COMMANDS = [
    "activities",
    "wellness",
    "sleep",
    "heart_rate",
    "body_composition",
    "hrv",
    "personal_records",
]

# Default look-back when no ``since`` is provided
_DEFAULT_LOOKBACK_DAYS = 90


class GarminTool(BaseTool):
    """Garmin Connect data source.

    config keys (from models/<model>/config.yaml tools.garmin section):
      email        Garmin Connect account email address
      password     Account password (support file: prefix)
      token_store  Optional path to a garth token JSON file (file: prefix
                   supported).  When provided, avoids re-login / MFA prompts.
    """

    def __init__(self, config: dict, **_kwargs):
        try:
            from garminconnect import Garmin  # type: ignore[import]
        except ImportError as exc:
            raise ImportError(
                "garminconnect is required for the Garmin tool. "
                "Install with: pip install garminconnect"
            ) from exc

        self._Garmin = Garmin
        self._email: str = config["email"]
        self._password: str = config.get("password", "")
        self._token_store: str | None = config.get("token_store")
        self._client = None

    # ------------------------------------------------------------------
    # BaseTool interface
    # ------------------------------------------------------------------

    def list_commands(self) -> list[str]:
        return list(COMMANDS)

    def run(self, command: str, params: dict | None = None) -> Iterator[dict]:
        params = params or {}
        handler = {
            "activities": self._activities,
            "wellness": self._wellness,
            "sleep": self._sleep,
            "heart_rate": self._heart_rate,
            "body_composition": self._body_composition,
            "hrv": self._hrv,
            "personal_records": self._personal_records,
        }.get(command)
        if handler is None:
            raise ValueError(
                f"Unknown Garmin command: {command!r}. "
                f"Available: {', '.join(COMMANDS)}"
            )
        yield from handler(params)

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_client(self):
        with _cache_lock:
            if self._email in _client_cache:
                return _client_cache[self._email]

            client = self._Garmin(self._email, self._password)

            # Restore saved session tokens to skip interactive MFA
            if self._token_store:
                try:
                    token_json = self._resolve_token_store()
                    client.garth.loads(token_json)
                    client.display_name = client.get_full_name()
                    logger.debug("Garmin: session restored from token store")
                    # Re-persist in case garth refreshed the tokens during restore
                    self._persist_tokens(client)
                except Exception as exc:
                    logger.warning(
                        "Garmin: could not restore tokens (%s), logging in fresh", exc
                    )
                    client.login()
                    self._persist_tokens(client)
            else:
                client.login()

            _client_cache[self._email] = client
            return client

    def _resolve_token_store(self) -> str:
        """Read token JSON from file path (supports file: prefix)."""
        path = self._token_store or ""
        if path.startswith("file:"):
            path = path[5:].strip()
        import pathlib
        return pathlib.Path(path).expanduser().read_text()

    def _persist_tokens(self, client) -> None:
        """Write refreshed session tokens back to token_store if configured."""
        if not self._token_store:
            return
        try:
            path = self._token_store
            if path.startswith("file:"):
                path = path[5:].strip()
            import pathlib
            token_path = pathlib.Path(path).expanduser()
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text(json.dumps(client.garth.dumps()))
            logger.debug("Garmin: tokens persisted to %s", token_path)
        except Exception as exc:
            logger.warning("Garmin: failed to persist tokens: %s", exc)

    # ------------------------------------------------------------------
    # Date helpers
    # ------------------------------------------------------------------

    def _date_range(self, params: dict) -> tuple[str, str]:
        """Return (start_date_iso, end_date_iso) derived from params."""
        end = params.get("end_date") or date.today().isoformat()
        since = params.get("since")
        if since:
            # since may include a time component — strip to date
            start = since[:10]
        else:
            start = (date.today() - timedelta(days=_DEFAULT_LOOKBACK_DAYS)).isoformat()
        return start, end

    @staticmethod
    def _date_list(start: str, end: str) -> list[date]:
        start_d = date.fromisoformat(start)
        end_d = date.fromisoformat(end)
        delta = (end_d - start_d).days
        return [start_d + timedelta(days=i) for i in range(max(delta + 1, 0))]

    # ------------------------------------------------------------------
    # Command implementations
    # ------------------------------------------------------------------

    def _activities(self, params: dict) -> Iterator[dict]:
        """Yield activity/workout summary dicts, newest-first until since."""
        client = self._get_client()
        since = params.get("since")
        since_dt: datetime | None = None
        if since:
            try:
                since_dt = datetime.fromisoformat(since)
            except ValueError:
                pass

        start_idx = 0
        page_size = 100

        while True:
            batch = client.get_activities(start_idx, page_size)
            if not batch:
                break

            for act in batch:
                # Flatten activityType nested dict for easier JSON path extraction
                at = act.get("activityType")
                if isinstance(at, dict):
                    act = dict(act)
                    act["activityTypeKey"] = at.get("typeKey", "")
                    act["activityTypeName"] = at.get("typeName", "")

                if since_dt:
                    # Activities are ordered newest-first — stop when we go past since
                    ts = act.get("startTimeGMT") or act.get("startTimeLocal", "")
                    if ts:
                        try:
                            act_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                            act_dt_naive = act_dt.replace(tzinfo=None)
                            if act_dt_naive < since_dt:
                                return
                        except ValueError:
                            pass

                yield act

            if len(batch) < page_size:
                break
            start_idx += page_size

    def _wellness(self, params: dict) -> Iterator[dict]:
        """Yield one daily wellness summary dict per day."""
        client = self._get_client()
        start, end = self._date_range(params)
        for d in self._date_list(start, end):
            try:
                data = client.get_stats(d.isoformat())
                if data:
                    # Ensure calendarDate is always present
                    if "calendarDate" not in data:
                        data = dict(data)
                        data["calendarDate"] = d.isoformat()
                    yield data
            except Exception as exc:
                logger.warning("Garmin wellness: failed for %s — %s", d, exc)

    def _sleep(self, params: dict) -> Iterator[dict]:
        """Yield one sleep summary dict per night."""
        client = self._get_client()
        start, end = self._date_range(params)
        for d in self._date_list(start, end):
            try:
                resp = client.get_sleep_data(d.isoformat())
                dto = (resp or {}).get("dailySleepDTO")
                if dto:
                    row = dict(dto)
                    if "calendarDate" not in row:
                        row["calendarDate"] = d.isoformat()
                    yield row
            except Exception as exc:
                logger.warning("Garmin sleep: failed for %s — %s", d, exc)

    def _heart_rate(self, params: dict) -> Iterator[dict]:
        """Yield one heart-rate summary dict per day (resting HR + stats)."""
        client = self._get_client()
        start, end = self._date_range(params)
        for d in self._date_list(start, end):
            try:
                data = client.get_heart_rates(d.isoformat())
                if data:
                    row = dict(data)
                    if "calendarDate" not in row:
                        row["calendarDate"] = d.isoformat()
                    # Drop the raw intraday values array — too large for a summary table
                    row.pop("heartRateValues", None)
                    yield row
            except Exception as exc:
                logger.warning("Garmin heart_rate: failed for %s — %s", d, exc)

    def _body_composition(self, params: dict) -> Iterator[dict]:
        """Yield one body-composition measurement dict per recorded entry."""
        client = self._get_client()
        start, end = self._date_range(params)
        try:
            resp = client.get_body_composition(start, end)
            entries = (resp or {}).get("dateWeightList") or []
            yield from entries
        except Exception as exc:
            logger.warning("Garmin body_composition: failed — %s", exc)

    def _hrv(self, params: dict) -> Iterator[dict]:
        """Yield one HRV status dict per night."""
        client = self._get_client()
        start, end = self._date_range(params)
        for d in self._date_list(start, end):
            try:
                resp = client.get_hrv_data(d.isoformat())
                summary = (resp or {}).get("hrvSummary")
                if summary:
                    row = dict(summary)
                    if "calendarDate" not in row:
                        row["calendarDate"] = d.isoformat()
                    yield row
            except Exception as exc:
                logger.warning("Garmin hrv: failed for %s — %s", d, exc)

    def _personal_records(self, params: dict) -> Iterator[dict]:
        """Yield all personal record achievements (full refresh — no date filter)."""
        client = self._get_client()
        try:
            records = client.get_personal_record() or []
            yield from records
        except Exception as exc:
            logger.warning("Garmin personal_records: failed — %s", exc)
