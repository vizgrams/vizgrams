# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Significance score computation for published vizgrams.

Produces a score in [0.0, 10.0] that represents how noteworthy a data snapshot
is at the time of publishing.  Higher = more unusual / worth surfacing.

Two signals are combined:

z-score signal
    How far the most recent data point deviates from the preceding series
    (leave-one-out z-score).  When no time column is detected the most extreme
    value across the whole series is used instead.

Period-over-period (PoP) signal
    Percentage change between the last two chronologically-ordered values.
    Only active when a time column is detected.

Final score = max(z_component, pop_component) across all numeric columns,
each normalised to [0, 10]:
    z  → z = 3 σ maps to 10
    PoP → 100 % change maps to 10
"""

from __future__ import annotations

import math
import statistics
from datetime import date, datetime
from typing import Any


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_significance_score(
    data_snapshot: list | None,
    chart_config: dict | None = None,
) -> float:
    """Return a significance score in [0.0, 10.0] for the given snapshot.

    Args:
        data_snapshot:  List of rows.  Each row is a list of values (positional)
                        or a dict.  Matches the format emitted by QueryResult.rows.
        chart_config:   The chart_config dict stored alongside the vizgram.
                        ``chart_config["columns"]`` is used to name positional rows.

    Returns:
        Float in [0.0, 10.0].  Returns 0.0 when the snapshot is empty, has a
        single row, or contains no usable numeric columns.
    """
    if not data_snapshot or len(data_snapshot) < 2:
        return 0.0

    chart_config = chart_config or {}
    columns: list[str] = chart_config.get("columns") or []

    rows = _normalise_rows(data_snapshot, columns)
    if not rows:
        return 0.0

    col_names = list(rows[0].keys())
    time_col = _detect_time_column(col_names, rows)

    if time_col:
        rows = _sort_by_time(rows, time_col)

    max_score = 0.0
    for col in col_names:
        if col == time_col:
            continue
        values = _extract_numerics(rows, col)
        if len(values) < 2:
            continue
        col_score = _score_series(values, has_time=time_col is not None)
        if col_score > max_score:
            max_score = col_score

    return round(min(max_score, 10.0), 4)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalise_rows(rows: list, columns: list[str]) -> list[dict[str, Any]]:
    """Convert list-of-lists to list-of-dicts using *columns* for names.

    If rows are already dicts, returns them unchanged (after filtering None).
    """
    if not rows:
        return []
    first = rows[0]
    if isinstance(first, dict):
        return [r for r in rows if r is not None]
    if not columns:
        # No column names — synthesise generic names
        width = max(len(r) for r in rows if isinstance(r, (list, tuple)))
        columns = [f"col_{i}" for i in range(width)]
    result = []
    for row in rows:
        if row is None:
            continue
        result.append(dict(zip(columns, row)))
    return result


_TIME_KEYWORDS = frozenset({
    "date", "day", "week", "month", "quarter", "year", "period",
    "time", "timestamp", "created_at", "updated_at", "at",
})


def _detect_time_column(col_names: list[str], rows: list[dict]) -> str | None:
    """Return the name of a time-like column, or None.

    Checks column names against known time keywords first; then tries parsing
    the first non-null value in candidate columns as an ISO date/datetime.
    """
    candidates = [
        c for c in col_names
        if any(kw in c.lower() for kw in _TIME_KEYWORDS)
    ]
    for col in candidates:
        sample = next((r[col] for r in rows if r.get(col) is not None), None)
        if _is_date_like(sample):
            return col
    # Fallback: try any column whose first non-null value parses as a date
    for col in col_names:
        if col in candidates:
            continue
        sample = next((r[col] for r in rows if r.get(col) is not None), None)
        if isinstance(sample, str) and _is_date_like(sample):
            return col
    return None


def _is_date_like(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (date, datetime)):
        return True
    if not isinstance(value, str):
        return False
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%W", "%Y-%m", "%Y"):
        try:
            datetime.strptime(value[:len(fmt) + 2].strip(), fmt)
            return True
        except ValueError:
            continue
    return False


def _sort_by_time(rows: list[dict], time_col: str) -> list[dict]:
    def _key(r):
        v = r.get(time_col)
        if v is None:
            return ""
        return str(v)
    return sorted(rows, key=_key)


def _extract_numerics(rows: list[dict], col: str) -> list[float]:
    """Return all non-null numeric values for *col* in row order."""
    result = []
    for row in rows:
        v = row.get(col)
        if v is None:
            continue
        try:
            result.append(float(v))
        except (TypeError, ValueError):
            pass
    return result


def _score_series(values: list[float], *, has_time: bool) -> float:
    """Compute a [0, 10] score for a single numeric series.

    When *has_time* is True uses the last point as the "current" observation.
    When False uses the most extreme deviation across all points.
    """
    n = len(values)
    if n < 2:
        return 0.0

    z_score = 0.0
    pop_score = 0.0

    if has_time:
        # Leave-one-out: score the last value against the preceding series
        rest = values[:-1]
        last = values[-1]
        if len(rest) >= 1:
            mu = statistics.mean(rest)
            sigma = statistics.pstdev(rest)  # population stdev — stable for small n
            if sigma > 0:
                z_score = abs(last - mu) / sigma

        # Period-over-period: % change between last two values
        prev = values[-2]
        if abs(prev) > 1e-9:
            pop_score = abs(last - prev) / abs(prev)
        elif abs(last) > 1e-9:
            # prev is ~0 but last is not — treat as large change
            pop_score = 1.0

    else:
        # No time ordering — z-score the most extreme value
        mu = statistics.mean(values)
        sigma = statistics.pstdev(values)
        if sigma > 0:
            z_score = max(abs(v - mu) / sigma for v in values)

    # Normalise to [0, 10]: z=3σ → 10; PoP=100% → 10
    z_component = min(z_score * 10.0 / 3.0, 10.0)
    pop_component = min(pop_score * 10.0, 10.0)

    return max(z_component, pop_component)
