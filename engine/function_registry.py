# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""SQL function registry.

Each function maps to per-dialect render callables that produce SQL fragments.
Register a function under the "*" dialect as a fallback for all dialects.

Usage in compilers:
    from engine.function_registry import render_function, DialectFunctionError
    sql = render_function("datetime_diff", [a_sql, b_sql], {"unit": "hours"})
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


class DialectFunctionError(ValueError):
    """Raised when a function has no render implementation for the requested dialect."""


@dataclass
class FunctionSpec:
    """A single dialect implementation of a named SQL function."""
    render: Callable[[list[str], dict[str, object]], str]
    arity_min: int
    arity_max: int | None  # None = variadic


_REGISTRY: dict[str, dict[str, FunctionSpec]] = {}


def register(
    name: str,
    dialect: str,
    render: Callable[[list[str], dict[str, object]], str],
    arity_min: int = 0,
    arity_max: int | None = None,
) -> None:
    """Register a render function for (name, dialect).

    Use dialect="*" for a dialect-agnostic implementation.
    """
    _REGISTRY.setdefault(name, {})[dialect] = FunctionSpec(
        render=render, arity_min=arity_min, arity_max=arity_max
    )


def render_function(
    name: str,
    args: list[str],
    kwargs: dict[str, object],
    dialect: str = "sqlite",
) -> str:
    """Look up and invoke the render function for name.

    Lookup order: exact dialect → "*" wildcard → DialectFunctionError.
    kwargs may contain raw Python values (e.g. unit="hours") or SQL strings,
    depending on the convention established per function.
    """
    impls = _REGISTRY.get(name.lower())
    if impls is None:
        raise DialectFunctionError(
            f"Unknown function {name!r}. "
            "If this is a passthrough SQL function, use it in a raw_sql feature."
        )
    spec = impls.get(dialect) or impls.get("*")
    if spec is None:
        raise DialectFunctionError(
            f"Function {name!r} has no implementation for dialect {dialect!r}."
        )
    n = len(args)
    if n < spec.arity_min:
        raise DialectFunctionError(
            f"{name}() requires at least {spec.arity_min} argument(s), got {n}"
        )
    if spec.arity_max is not None and n > spec.arity_max:
        raise DialectFunctionError(
            f"{name}() accepts at most {spec.arity_max} argument(s), got {n}"
        )
    return spec.render(args, kwargs)


# ---------------------------------------------------------------------------
# datetime_diff
# kwargs: unit (str) — raw unit name, already validated (hours|days|minutes|seconds|years)
# ---------------------------------------------------------------------------

_DATETIME_DIFF_UNITS = {"seconds", "minutes", "hours", "days", "years"}

_SQLITE_DATETIME_DIFF_FACTORS = {
    "seconds": "* 86400",
    "minutes": "* 1440",
    "hours":   "* 24",
    "days":    "AS_INTEGER",  # special
    "years":   "/ 365.25",
}


def _render_datetime_diff_sqlite(args: list[str], kwargs: dict) -> str:
    a_sql, b_sql = args
    unit = str(kwargs["unit"])
    diff = f"(julianday(substr({b_sql}, 1, 19)) - julianday(substr({a_sql}, 1, 19)))"
    factor = _SQLITE_DATETIME_DIFF_FACTORS[unit]
    if factor == "AS_INTEGER":
        return f"CAST({diff} AS INTEGER)"
    return f"({diff} {factor})"


register("datetime_diff", "sqlite", _render_datetime_diff_sqlite, arity_min=2, arity_max=2)


# ---------------------------------------------------------------------------
# format_time
# kwargs: pattern (str) — raw pattern string e.g. "YYYY-MM", "YYYY-WW", "YYYY-wWW"
# ---------------------------------------------------------------------------

_FORMAT_TIME_SQLITE_TOKENS = [
    ("YYYY", "%G"),
    ("WW",   "%V"),
    ("MM",   "%m"),
    ("DD",   "%d"),
    ("HH",   "%H"),
]


def _render_format_time_sqlite(args: list[str], kwargs: dict) -> str:
    col_sql = args[0]
    pattern = str(kwargs["pattern"])
    fmt = pattern
    for token, directive in _FORMAT_TIME_SQLITE_TOKENS:
        fmt = fmt.replace(token, directive)
    return f"strftime('{fmt}', substr({col_sql}, 1, 19))"


register("format_time", "sqlite", _render_format_time_sqlite, arity_min=1, arity_max=1)


# ---------------------------------------------------------------------------
# format_date  (Java-style format tokens applied to a DATE column)
# kwargs: pattern (str) — raw Java-style format string e.g. "yyyy-MM-dd"
# ---------------------------------------------------------------------------

_SHORT_MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
_FULL_MONTHS  = ["January","February","March","April","May","June",
                 "July","August","September","October","November","December"]
_SHORT_DAYS   = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
_FULL_DAYS    = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]


def _sqlite_case(strftime_tok: str, col: str, names: list[str], zero_based: bool = False) -> str:
    whens = " ".join(
        f"WHEN '{i if zero_based else i+1:02d}' THEN '{name}'"
        for i, name in enumerate(names)
    )
    return f"CASE strftime('{strftime_tok}', {col}) {whens} END"


# Longest-first to avoid partial token matches
_JAVA_TOKENS_SQLITE: list[tuple[str, object]] = [
    ("MMMM", lambda col: _sqlite_case("%m", col, _FULL_MONTHS)),
    ("MMM",  lambda col: _sqlite_case("%m", col, _SHORT_MONTHS)),
    ("EEEE", lambda col: _sqlite_case("%w", col, _FULL_DAYS, zero_based=True)),
    ("yyyy", lambda col: f"strftime('%Y', {col})"),
    ("DDD",  lambda col: f"strftime('%j', {col})"),
    ("MM",   lambda col: f"strftime('%m', {col})"),
    ("HH",   lambda col: f"strftime('%H', {col})"),
    ("dd",   lambda col: f"strftime('%d', {col})"),
    ("mm",   lambda col: f"strftime('%M', {col})"),
    ("ss",   lambda col: f"strftime('%S', {col})"),
    ("yy",   lambda col: f"substr(strftime('%Y', {col}), 3, 2)"),
    ("E",    lambda col: _sqlite_case("%w", col, _SHORT_DAYS, zero_based=True)),
]



def _apply_java_tokens_sqlite(col_sql: str, fmt: str) -> str:
    norm = f"substr({col_sql}, 1, 19)"
    parts: list[str] = []
    i = 0
    while i < len(fmt):
        for token, sql_fn in _JAVA_TOKENS_SQLITE:
            if fmt[i:i + len(token)] == token:
                parts.append(sql_fn(norm))  # type: ignore[operator]
                i += len(token)
                break
        else:
            ch = fmt[i].replace("'", "''")
            parts.append(f"'{ch}'")
            i += 1
    return parts[0] if len(parts) == 1 else " || ".join(parts)


def _render_format_date_sqlite(args: list[str], kwargs: dict) -> str:
    return _apply_java_tokens_sqlite(args[0], str(kwargs["pattern"]))


register("format_date", "sqlite", _render_format_date_sqlite, arity_min=1, arity_max=1)


# ---------------------------------------------------------------------------
# json_has_key
# args: [json_sql, key_sql]
# ---------------------------------------------------------------------------

register(
    "json_has_key", "sqlite",
    lambda args, _: f"json_extract({args[0]}, '$.' || {args[1]}) IS NOT NULL",
    arity_min=2, arity_max=2,
)


# ---------------------------------------------------------------------------
# concat  (dialect-agnostic: SQL || concatenation)
# ---------------------------------------------------------------------------

register(
    "concat", "*",
    lambda args, _: "(" + " || ".join(args) + ")",
    arity_min=2,
)


# ---------------------------------------------------------------------------
# ClickHouse implementations
# ---------------------------------------------------------------------------

_CH_DIFF_UNITS = {
    "seconds": "second",
    "minutes": "minute",
    "hours": "hour",
    "days": "day",
    "years": "year",
}


def _render_datetime_diff_clickhouse(args: list[str], kwargs: dict) -> str:
    a_sql, b_sql = args
    ch_unit = _CH_DIFF_UNITS[str(kwargs["unit"])]
    # Columns are stored as ISO-8601 strings (e.g. "2024-10-15T08:50:18Z").
    # toDateTime() cannot parse the T/Z format — use parseDateTimeBestEffortOrNull
    # (substr to 19 chars strips the trailing Z before parsing).
    def _wrap(s: str) -> str:
        return f"parseDateTimeBestEffortOrNull(substr(toString({s}), 1, 19))"
    return f"dateDiff('{ch_unit}', {_wrap(a_sql)}, {_wrap(b_sql)})"


register("datetime_diff", "clickhouse", _render_datetime_diff_clickhouse, arity_min=2, arity_max=2)


_FORMAT_TIME_CH_TOKENS = [
    ("YYYY", "%Y"),
    ("WW",   "%V"),
    ("MM",   "%m"),
    ("DD",   "%d"),
    ("HH",   "%H"),
]


def _render_format_time_clickhouse(args: list[str], kwargs: dict) -> str:
    col_sql = args[0]
    fmt = str(kwargs["pattern"])
    for token, directive in _FORMAT_TIME_CH_TOKENS:
        fmt = fmt.replace(token, directive)
    return f"formatDateTime(parseDateTimeBestEffort(substr({col_sql}, 1, 19)), '{fmt}')"


register("format_time", "clickhouse", _render_format_time_clickhouse, arity_min=1, arity_max=1)


# Longest-first to avoid partial matches
_JAVA_TOKENS_CH: list[tuple[str, str]] = [
    ("MMMM", "%B"),
    ("MMM",  "%b"),
    ("EEEE", "%A"),
    ("yyyy", "%Y"),
    ("DDD",  "%j"),
    ("MM",   "%m"),
    ("HH",   "%H"),
    ("dd",   "%d"),
    ("mm",   "%M"),
    ("ss",   "%S"),
    ("yy",   "%y"),
    ("E",    "%a"),
]


def _render_format_date_clickhouse(args: list[str], kwargs: dict) -> str:
    col_sql = args[0]
    pattern = str(kwargs["pattern"])
    fmt = ""
    i = 0
    while i < len(pattern):
        for token, directive in _JAVA_TOKENS_CH:
            if pattern[i:i + len(token)] == token:
                fmt += directive
                i += len(token)
                break
        else:
            fmt += pattern[i]
            i += 1
    return f"formatDateTime(parseDateTimeBestEffort(substr({col_sql}, 1, 19)), '{fmt}')"


register("format_date", "clickhouse", _render_format_date_clickhouse, arity_min=1, arity_max=1)

register(
    "json_has_key", "clickhouse",
    lambda args, _: f"JSONHas({args[0]}, {args[1]})",
    arity_min=2, arity_max=2,
)
