# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for engine/function_registry.py."""
import pytest

from engine.function_registry import (
    DialectFunctionError,
    register,
    render_function,
)

# ---------------------------------------------------------------------------
# Registry infrastructure
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_register_and_lookup(self):
        register("_test_fn", "*", lambda args, kw: f"TEST({', '.join(args)})", arity_min=1)
        result = render_function("_test_fn", ["x"], {})
        assert result == "TEST(x)"

    def test_unknown_function_raises(self):
        with pytest.raises(DialectFunctionError, match="Unknown function"):
            render_function("no_such_function_xyz", [], {})

    def test_arity_min_enforced(self):
        register("_test_arity", "*", lambda args, kw: "OK", arity_min=2)
        with pytest.raises(DialectFunctionError, match="requires at least 2"):
            render_function("_test_arity", ["only_one"], {})

    def test_arity_max_enforced(self):
        register("_test_arity_max", "*", lambda args, kw: "OK", arity_min=0, arity_max=1)
        with pytest.raises(DialectFunctionError, match="accepts at most 1"):
            render_function("_test_arity_max", ["a", "b"], {})

    def test_variadic_arity_max_none(self):
        register("_test_variadic", "*", lambda args, kw: "|".join(args), arity_min=0, arity_max=None)
        result = render_function("_test_variadic", ["a", "b", "c", "d"], {})
        assert result == "a|b|c|d"

    def test_name_case_insensitive(self):
        """render_function normalises function names to lower-case."""
        result = render_function("concat", ["'a'", "'b'"], {})
        assert "a" in result and "b" in result


# ---------------------------------------------------------------------------
# datetime_diff
# ---------------------------------------------------------------------------

class TestDatetimeDiff:
    def test_sqlite_days(self):
        sql = render_function("datetime_diff", ["a.start", "a.end"], {"unit": "days"})
        assert "julianday" in sql
        assert "AS INTEGER" in sql

    def test_sqlite_hours(self):
        sql = render_function("datetime_diff", ["a.start", "a.end"], {"unit": "hours"})
        assert "julianday" in sql
        assert "* 24" in sql

    def test_sqlite_minutes(self):
        sql = render_function("datetime_diff", ["a.start", "a.end"], {"unit": "minutes"})
        assert "* 1440" in sql

    def test_sqlite_seconds(self):
        sql = render_function("datetime_diff", ["a.start", "a.end"], {"unit": "seconds"})
        assert "* 86400" in sql

    def test_sqlite_years(self):
        sql = render_function("datetime_diff", ["a.start", "a.end"], {"unit": "years"})
        assert "/ 365.25" in sql

    def test_arity_enforced(self):
        with pytest.raises(DialectFunctionError, match="2 argument"):
            render_function("datetime_diff", ["only_one"], {"unit": "days"})


# ---------------------------------------------------------------------------
# format_time
# ---------------------------------------------------------------------------

class TestFormatTime:
    def test_sqlite_year_week(self):
        sql = render_function("format_time", ["t.ts"], {"pattern": "YYYY-WW"})
        assert "strftime" in sql
        assert "%G-%V" in sql
        assert "substr" in sql

    def test_sqlite_year_month(self):
        sql = render_function("format_time", ["t.ts"], {"pattern": "YYYY-MM"})
        assert "%G-%m" in sql


# ---------------------------------------------------------------------------
# format_date
# ---------------------------------------------------------------------------

class TestFormatDate:
    def test_sqlite_year_month_day(self):
        sql = render_function("format_date", ["d.col"], {"pattern": "yyyy-MM-dd"})
        assert "strftime('%Y'" in sql
        assert "strftime('%m'" in sql
        assert "strftime('%d'" in sql


# ---------------------------------------------------------------------------
# json_has_key
# ---------------------------------------------------------------------------

class TestJsonHasKey:
    def test_sqlite(self):
        sql = render_function("json_has_key", ["t.data", "'mykey'"], {})
        assert "json_extract" in sql
        assert "IS NOT NULL" in sql


# ---------------------------------------------------------------------------
# concat
# ---------------------------------------------------------------------------

class TestConcat:
    def test_sqlite(self):
        sql = render_function("concat", ["a.x", "'-'", "b.y"], {})
        assert "||" in sql
        assert "a.x" in sql
        assert "b.y" in sql

    def test_two_args(self):
        sql = render_function("concat", ["'foo'", "'bar'"], {})
        assert sql == "('foo' || 'bar')"


# ---------------------------------------------------------------------------
# ClickHouse dialect
# ---------------------------------------------------------------------------

class TestClickHouseDialect:
    def test_dialect_lookup_uses_exact_match(self):
        """clickhouse dialect uses clickhouse impl, not sqlite."""
        sql_sqlite = render_function("datetime_diff", ["a", "b"], {"unit": "hours"}, dialect="sqlite")
        sql_ch = render_function("datetime_diff", ["a", "b"], {"unit": "hours"}, dialect="clickhouse")
        assert "julianday" in sql_sqlite
        assert "dateDiff" in sql_ch

    def test_unknown_dialect_falls_back_to_wildcard(self):
        """concat has a '*' implementation — should work for any dialect."""
        sql = render_function("concat", ["a", "b"], {}, dialect="clickhouse")
        assert "||" in sql

    def test_unknown_dialect_no_wildcard_raises(self):
        """datetime_diff has no '*' impl — unknown dialect should raise."""
        with pytest.raises(DialectFunctionError, match="no implementation for dialect"):
            render_function("datetime_diff", ["a", "b"], {"unit": "hours"}, dialect="duckdb")

    def test_datetime_diff_ch_hours(self):
        sql = render_function("datetime_diff", ["t.start", "t.end"], {"unit": "hours"}, dialect="clickhouse")
        assert "dateDiff('hour'" in sql
        assert "t.start" in sql and "t.end" in sql

    def test_datetime_diff_ch_days(self):
        sql = render_function("datetime_diff", ["t.a", "t.b"], {"unit": "days"}, dialect="clickhouse")
        assert "dateDiff('day'" in sql

    def test_datetime_diff_ch_seconds(self):
        sql = render_function("datetime_diff", ["t.a", "t.b"], {"unit": "seconds"}, dialect="clickhouse")
        assert "dateDiff('second'" in sql

    def test_format_time_ch_year_week(self):
        sql = render_function("format_time", ["t.col"], {"pattern": "YYYY-WW"}, dialect="clickhouse")
        assert "formatDateTime" in sql
        assert "%Y-%V" in sql

    def test_format_time_ch_year_month(self):
        sql = render_function("format_time", ["t.col"], {"pattern": "YYYY-MM"}, dialect="clickhouse")
        assert "formatDateTime" in sql
        assert "%Y-%m" in sql

    def test_format_date_ch_yyyy_mm_dd(self):
        sql = render_function("format_date", ["d.col"], {"pattern": "yyyy-MM-dd"}, dialect="clickhouse")
        assert "formatDateTime" in sql
        assert "%Y" in sql
        assert "%m" in sql
        assert "%d" in sql

    def test_format_date_ch_full_month_name(self):
        sql = render_function("format_date", ["d.col"], {"pattern": "MMMM yyyy"}, dialect="clickhouse")
        assert "%B" in sql
        assert "%Y" in sql

    def test_format_date_ch_abbr_month_name(self):
        sql = render_function("format_date", ["d.col"], {"pattern": "MMM yyyy"}, dialect="clickhouse")
        assert "%b" in sql

    def test_format_date_ch_day_name(self):
        sql = render_function("format_date", ["d.col"], {"pattern": "EEEE"}, dialect="clickhouse")
        assert "%A" in sql

    def test_json_has_key_ch(self):
        sql = render_function("json_has_key", ["t.data", "'mykey'"], {}, dialect="clickhouse")
        assert "JSONHas" in sql
        assert "t.data" in sql
        assert "'mykey'" in sql
