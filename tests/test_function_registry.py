# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for engine/function_registry.py."""
import pytest

from engine.function_registry import (
    DialectFunctionError,
    is_registered,
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

    def test_is_registered(self):
        """All built-in functions report registered; unknowns report not."""
        assert is_registered("format_date")
        assert is_registered("format_time")
        assert is_registered("json_has_key")
        assert is_registered("datetime_diff")
        assert is_registered("concat")
        # Case-insensitive — same normalisation as render_function.
        assert is_registered("FORMAT_DATE")
        assert not is_registered("definitely_not_a_function")


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

    def test_unregistered_dialect_no_wildcard_raises(self):
        """datetime_diff has no '*' impl — a never-registered dialect should raise."""
        with pytest.raises(DialectFunctionError, match="no implementation for dialect"):
            render_function("datetime_diff", ["a", "b"], {"unit": "hours"}, dialect="postgres")

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

    # -----------------------------------------------------------------
    # DuckDB dialect — added in Phase 2 of the CH→DuckDB migration
    # -----------------------------------------------------------------

    def test_datetime_diff_duckdb_hours(self):
        sql = render_function("datetime_diff", ["t.start", "t.end"], {"unit": "hours"}, dialect="duckdb")
        assert "date_diff('hour'" in sql
        assert "t.start" in sql and "t.end" in sql
        # ISO-8601 strings cast to TIMESTAMP after the trailing Z is stripped.
        assert "CAST(substr(t.start, 1, 19) AS TIMESTAMP)" in sql

    def test_datetime_diff_duckdb_days(self):
        sql = render_function("datetime_diff", ["t.a", "t.b"], {"unit": "days"}, dialect="duckdb")
        assert "date_diff('day'" in sql

    def test_datetime_diff_duckdb_seconds(self):
        sql = render_function("datetime_diff", ["t.a", "t.b"], {"unit": "seconds"}, dialect="duckdb")
        assert "date_diff('second'" in sql

    def test_format_time_duckdb_year_week(self):
        sql = render_function("format_time", ["t.col"], {"pattern": "YYYY-WW"}, dialect="duckdb")
        # Timestamp-first argument order (Python-style strftime).
        assert sql.startswith("strftime(CAST(")
        # ISO year + ISO week — matches SQLite's behaviour at year boundaries.
        assert "%G-%V" in sql

    def test_format_time_duckdb_year_month(self):
        sql = render_function("format_time", ["t.col"], {"pattern": "YYYY-MM"}, dialect="duckdb")
        assert "%G-%m" in sql

    def test_format_date_duckdb_yyyy_mm_dd(self):
        sql = render_function("format_date", ["d.col"], {"pattern": "yyyy-MM-dd"}, dialect="duckdb")
        assert sql.startswith("strftime(CAST(")
        assert "%Y-%m-%d" in sql

    def test_format_date_duckdb_full_month_name(self):
        sql = render_function("format_date", ["d.col"], {"pattern": "MMMM yyyy"}, dialect="duckdb")
        assert "%B" in sql and "%Y" in sql

    def test_format_date_duckdb_abbr_month_name(self):
        sql = render_function("format_date", ["d.col"], {"pattern": "MMM yyyy"}, dialect="duckdb")
        assert "%b" in sql

    def test_format_date_duckdb_day_name(self):
        sql = render_function("format_date", ["d.col"], {"pattern": "EEEE"}, dialect="duckdb")
        assert "%A" in sql

    def test_json_has_key_duckdb(self):
        sql = render_function("json_has_key", ["t.data", "'mykey'"], {}, dialect="duckdb")
        assert "json_extract(t.data" in sql
        assert "IS NOT NULL" in sql
        assert "'$.'" in sql

    def test_concat_works_for_duckdb(self):
        """concat is dialect-agnostic ('*') so should produce || on duckdb too."""
        sql = render_function("concat", ["a", "b"], {}, dialect="duckdb")
        assert sql == "(a || b)"

    def test_duckdb_rendered_sql_executes(self):
        """End-to-end: every rendered duckdb fragment must run against a live
        in-memory DuckDB. Catches dialect mismatches the string assertions
        above can miss (wrong argument order, unknown functions, etc.).
        """
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(":memory:")
        try:
            datetime_diff = render_function(
                "datetime_diff",
                ["'2026-01-15T08:00:00Z'", "'2026-01-15T11:30:00Z'"],
                {"unit": "hours"}, dialect="duckdb",
            )
            assert con.execute(f"SELECT {datetime_diff}").fetchone() == (3,)

            format_time = render_function(
                "format_time", ["'2026-12-30T08:00:00Z'"],
                {"pattern": "YYYY-WW"}, dialect="duckdb",
            )
            assert con.execute(f"SELECT {format_time}").fetchone() == ("2026-53",)

            format_date = render_function(
                "format_date", ["'2026-12-30'"],
                {"pattern": "MMMM yyyy"}, dialect="duckdb",
            )
            assert con.execute(f"SELECT {format_date}").fetchone() == ("December 2026",)

            con.execute("INSTALL json; LOAD json")
            json_has_key = render_function(
                "json_has_key", ["'{\"name\":\"a\"}'", "'name'"],
                {}, dialect="duckdb",
            )
            assert con.execute(f"SELECT {json_has_key}").fetchone() == (True,)
        finally:
            con.close()
