# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for engine.filter_compiler — Groups A–D per ADR-121."""

import re

import pytest

from engine.filter_compiler import (
    FilterExpressionError,
    collect_filter_column_refs,
    compile_filter_yaml,
)
from semantic.expression import (
    BinOp,
    DurationLit,
    FieldRef,
    FuncCallExpr,
    InExpr,
    ListLit,
    Lit,
    MethodCallExpr,
    parse_expression_str,
)

# ===========================================================================
# Group A — Parsing (TestFilterExpressionParsing)
# ===========================================================================

class TestFilterExpressionParsing:
    # --- Comparisons ---

    def test_eq_double_equals(self):
        expr = parse_expression_str('issue_type == "Story"')
        assert isinstance(expr, BinOp)
        assert expr.op == "="
        assert isinstance(expr.left, FieldRef)
        assert expr.left.parts == ["issue_type"]
        assert isinstance(expr.right, Lit)
        assert expr.right.value == "Story"

    def test_eq_single_equals(self):
        expr = parse_expression_str('status = "open"')
        assert isinstance(expr, BinOp)
        assert expr.op == "="

    def test_neq(self):
        expr = parse_expression_str('status != "closed"')
        assert isinstance(expr, BinOp)
        assert expr.op == "!="

    def test_gt(self):
        expr = parse_expression_str("priority > 3")
        assert isinstance(expr, BinOp)
        assert expr.op == ">"

    def test_gte(self):
        expr = parse_expression_str("priority >= 5")
        assert isinstance(expr, BinOp)
        assert expr.op == ">="

    def test_lt(self):
        expr = parse_expression_str("score < 10")
        assert isinstance(expr, BinOp)
        assert expr.op == "<"

    def test_lte(self):
        expr = parse_expression_str("score <= 7")
        assert isinstance(expr, BinOp)
        assert expr.op == "<="

    def test_float_rhs(self):
        expr = parse_expression_str("score > 3.5")
        assert isinstance(expr, BinOp)
        assert isinstance(expr.right, Lit)
        assert abs(expr.right.value - 3.5) < 1e-9

    # --- IN / NOT IN ---

    def test_in_list_strings(self):
        expr = parse_expression_str('status in ["open", "active"]')
        assert isinstance(expr, InExpr)
        assert isinstance(expr.expr, FieldRef)
        assert expr.expr.parts == ["status"]
        assert not expr.negated
        assert len(expr.values) == 2
        assert expr.values[0] == Lit("open")
        assert expr.values[1] == Lit("active")

    def test_in_list_single(self):
        expr = parse_expression_str('status in ["open"]')
        assert isinstance(expr, InExpr)
        assert len(expr.values) == 1

    def test_not_in_list(self):
        expr = parse_expression_str('status not_in ["closed"]')
        assert isinstance(expr, InExpr)
        assert expr.negated
        assert len(expr.values) == 1
        assert expr.values[0] == Lit("closed")

    def test_in_with_numbers(self):
        expr = parse_expression_str("priority in [1, 2, 3]")
        assert isinstance(expr, InExpr)
        assert len(expr.values) == 3
        assert expr.values[0] == Lit(1)

    # --- Duration / now() ---

    def test_duration_lit_days(self):
        expr = parse_expression_str("7d")
        assert isinstance(expr, DurationLit)
        assert expr.amount == 7
        assert expr.unit == "d"

    def test_duration_lit_weeks(self):
        expr = parse_expression_str("4w")
        assert isinstance(expr, DurationLit)
        assert expr.amount == 4
        assert expr.unit == "w"

    def test_now_zero_args(self):
        expr = parse_expression_str("now()")
        assert isinstance(expr, FuncCallExpr)
        assert expr.name == "now"
        assert expr.args == []

    def test_now_minus_days(self):
        expr = parse_expression_str("now() - 7d")
        assert isinstance(expr, BinOp)
        assert expr.op == "-"
        assert isinstance(expr.left, FuncCallExpr)
        assert expr.left.name == "now"
        assert isinstance(expr.right, DurationLit)
        assert expr.right.amount == 7
        assert expr.right.unit == "d"

    def test_now_minus_weeks(self):
        expr = parse_expression_str("now() - 4w")
        assert isinstance(expr, BinOp)
        assert isinstance(expr.right, DurationLit)
        assert expr.right.amount == 4
        assert expr.right.unit == "w"

    # --- Method calls ---

    def test_method_is_null(self):
        expr = parse_expression_str("field.is_null()")
        assert isinstance(expr, MethodCallExpr)
        assert isinstance(expr.expr, FieldRef)
        assert expr.expr.parts == ["field"]
        assert expr.method == "is_null"
        assert expr.args == []

    def test_method_not_null(self):
        expr = parse_expression_str("field.not_null()")
        assert isinstance(expr, MethodCallExpr)
        assert expr.method == "not_null"

    def test_method_startswith(self):
        expr = parse_expression_str('field.startswith("AD-")')
        assert isinstance(expr, MethodCallExpr)
        assert expr.method == "startswith"
        assert len(expr.args) == 1
        assert isinstance(expr.args[0], Lit)
        assert expr.args[0].value == "AD-"

    def test_method_endswith(self):
        expr = parse_expression_str('name.endswith("Review")')
        assert isinstance(expr, MethodCallExpr)
        assert expr.method == "endswith"

    def test_method_contains(self):
        expr = parse_expression_str('labels.contains("bug")')
        assert isinstance(expr, MethodCallExpr)
        assert expr.method == "contains"
        assert expr.args[0] == Lit("bug")

    def test_method_contains_any(self):
        expr = parse_expression_str('labels.containsAny(["bug","defect"])')
        assert isinstance(expr, MethodCallExpr)
        assert expr.method == "containsAny"
        assert len(expr.args) == 1
        assert isinstance(expr.args[0], ListLit)
        assert len(expr.args[0].values) == 2

    def test_method_json_any(self):
        expr = parse_expression_str('items.json_any("fieldId", "status")')
        assert isinstance(expr, MethodCallExpr)
        assert expr.method == "json_any"
        assert len(expr.args) == 2

    # --- Boolean / traversal ---

    def test_and_inline(self):
        expr = parse_expression_str('system == "github" and type == "handle"')
        assert isinstance(expr, BinOp)
        assert expr.op == "AND"

    def test_traversal_comparison(self):
        expr = parse_expression_str("Repository.Team.display_name = 'Lovelace'")
        assert isinstance(expr, BinOp)
        assert isinstance(expr.left, FieldRef)
        assert expr.left.parts == ["Repository", "Team", "display_name"]
        assert expr.right == Lit("Lovelace")

    # --- Errors ---

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            parse_expression_str("")

    def test_bad_syntax_raises(self):
        with pytest.raises(ValueError):
            parse_expression_str("@@bad")


# ===========================================================================
# Group B — Filter SQL compilation (TestFilterCompileSql)
# ===========================================================================

class TestFilterCompileSql:
    # --- Comparisons ---

    def test_eq_with_alias(self):
        sql = compile_filter_yaml('issue_type == "Story"', alias="src")
        assert sql == "src.issue_type = 'Story'"

    def test_eq_no_alias(self):
        sql = compile_filter_yaml('issue_type == "Story"')
        assert sql == "issue_type = 'Story'"

    def test_neq_sql(self):
        sql = compile_filter_yaml('status != "closed"', alias="t")
        assert sql == "t.status != 'closed'"

    def test_comparison_number(self):
        sql = compile_filter_yaml("priority > 3")
        assert sql == "priority > 3"

    # --- IN / NOT IN ---

    def test_in_sql(self):
        sql = compile_filter_yaml('status in ["open", "active"]', alias="s")
        assert sql == "s.status IN ('open', 'active')"

    def test_not_in_sql(self):
        sql = compile_filter_yaml('status not_in ["closed"]')
        assert sql == "status NOT IN ('closed')"

    def test_in_numbers_sql(self):
        sql = compile_filter_yaml("priority in [1, 2, 3]")
        assert sql == "priority IN (1, 2, 3)"

    # --- Null checks ---

    def test_is_null_method(self):
        sql = compile_filter_yaml("col.is_null()")
        assert sql == "col IS NULL"

    def test_not_null_method(self):
        sql = compile_filter_yaml("col.not_null()")
        assert sql == "col IS NOT NULL"

    def test_is_null_postfix(self):
        sql = compile_filter_yaml("col.is_null()", alias="t")
        assert sql == "t.col IS NULL"

    def test_is_not_null_postfix(self):
        sql = compile_filter_yaml("col.not_null()", alias="t")
        assert sql == "t.col IS NOT NULL"

    # --- String methods ---

    def test_startswith_sql(self):
        sql = compile_filter_yaml('key.startswith("AD-")')
        assert sql == "key LIKE 'AD-%' ESCAPE '\\'"

    def test_startswith_with_alias(self):
        sql = compile_filter_yaml('key.startswith("AD-")', alias="s")
        assert sql == "s.key LIKE 'AD-%' ESCAPE '\\'"

    def test_endswith_sql(self):
        sql = compile_filter_yaml('name.endswith("Review")')
        assert sql == "name LIKE '%Review' ESCAPE '\\'"

    def test_startswith_escapes_percent(self):
        sql = compile_filter_yaml('key.startswith("50%off")')
        assert sql == "key LIKE '50\\%off%' ESCAPE '\\'"

    def test_startswith_escapes_underscore(self):
        sql = compile_filter_yaml('key.startswith("_x")')
        assert sql == "key LIKE '\\_x%' ESCAPE '\\'"

    # --- JSON methods ---

    def test_contains_sql(self):
        sql = compile_filter_yaml('labels.contains("bug")', alias="s")
        assert sql == "EXISTS (SELECT 1 FROM json_each(s.labels) WHERE value = 'bug')"

    def test_contains_any_sql(self):
        sql = compile_filter_yaml('labels.containsAny(["bug","defect"])', alias="s")
        assert sql == "EXISTS (SELECT 1 FROM json_each(s.labels) WHERE value IN ('bug', 'defect'))"

    def test_json_any_sql(self):
        sql = compile_filter_yaml('items.json_any("fieldId","status")', alias="sc")
        assert sql == "EXISTS (SELECT 1 FROM json_each(sc.items) WHERE json_extract(value, '$.fieldId') = 'status')"

    # --- Timestamp / duration ---

    def test_now_compiles_to_timestamp(self):
        sql = compile_filter_yaml("created_at > now()")
        # Should contain a quoted datetime string
        assert "created_at >" in sql
        assert re.search(r"'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'", sql)

    def test_now_minus_days_offset(self):
        sql = compile_filter_yaml("created_at > now() - 7d")
        # Should contain a datetime about 7 days in the past
        assert re.search(r"'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'", sql)

    def test_now_minus_weeks_offset(self):
        sql = compile_filter_yaml("created_at > now() - 4w")
        assert re.search(r"'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'", sql)

    # --- format_time / format_date in WHERE ---

    def test_format_time_week_in_filter(self):
        sql = compile_filter_yaml("format_time(merged_at, 'YYYY-WW') == '2025-05'")
        # Should compile via the function registry, not raw FORMAT_TIME
        assert "FORMAT_TIME" not in sql
        assert "strftime" in sql
        assert "merged_at" in sql
        assert "2025-05" in sql

    def test_format_time_day_in_filter(self):
        sql = compile_filter_yaml("format_time(occurred_at, 'YYYY-MM-DD') == '2025-03-15'")
        assert "FORMAT_TIME" not in sql
        assert "strftime" in sql
        assert "2025-03-15" in sql

    def test_format_date_in_filter(self):
        sql = compile_filter_yaml("format_date(created_at, 'YYYY-MM') == '2025-03'")
        assert "FORMAT_DATE" not in sql
        assert "strftime" in sql
        assert "2025-03" in sql

    def test_format_time_wrong_arg_count_raises(self):
        with pytest.raises(FilterExpressionError, match="requires exactly 2"):
            compile_filter_yaml("format_time(merged_at)")

    def test_format_time_non_literal_pattern_raises(self):
        with pytest.raises(FilterExpressionError, match="string literal"):
            compile_filter_yaml("format_time(merged_at, some_col) == '2025-05'")

    # --- Logical composition ---

    def test_and_dict(self):
        sql = compile_filter_yaml({"and": ['status == "open"', "priority > 3"]})
        assert sql == "(status = 'open' AND priority > 3)"

    def test_or_dict(self):
        sql = compile_filter_yaml({"or": ['status == "open"', 'status == "active"']})
        assert sql == "(status = 'open' OR status = 'active')"

    def test_not_dict(self):
        sql = compile_filter_yaml({"not": 'status == "closed"'})
        assert sql == "NOT (status = 'closed')"

    def test_nested_and_or(self):
        filt = {"and": ['x == "a"', {"or": ['y == "b"', 'y == "c"']}]}
        sql = compile_filter_yaml(filt)
        assert sql == "(x = 'a' AND (y = 'b' OR y = 'c'))"

    def test_and_inline_syntax(self):
        sql = compile_filter_yaml('system == "github" and type == "handle"')
        assert sql == "(system = 'github' AND type = 'handle')"

    # --- Path resolver ---

    def test_traversal_with_path_resolver(self):
        called_with = []

        def resolver(parts):
            called_with.append(parts)
            return "resolved_col"

        sql = compile_filter_yaml(
            "Repository.Team.display_name = 'Lovelace'",
            path_resolver=resolver,
        )
        assert called_with == [["Repository", "Team", "display_name"]]
        assert "resolved_col = 'Lovelace'" in sql


# ===========================================================================
# Group C — Filter error cases (TestFilterErrorCases)
# ===========================================================================

class TestFilterErrorCases:
    def test_agg_in_filter_raises(self):
        with pytest.raises(FilterExpressionError, match="[Aa]ggregat"):
            compile_filter_yaml("sum(amount) > 100")

    def test_avg_in_filter_raises(self):
        with pytest.raises(FilterExpressionError, match="[Aa]ggregat"):
            compile_filter_yaml("avg(score) > 5")

    def test_window_in_filter_raises(self):
        with pytest.raises(FilterExpressionError, match="[Ww]indow"):
            compile_filter_yaml("lag(score).over(key, date) > 0")

    def test_count_in_filter_raises(self):
        with pytest.raises(FilterExpressionError, match="[Aa]ggregat"):
            compile_filter_yaml("count(x) > 0")

    def test_unknown_method_raises(self):
        with pytest.raises(FilterExpressionError, match="frobnicate"):
            compile_filter_yaml('field.frobnicate("x")')

    def test_is_null_with_args_raises(self):
        with pytest.raises(FilterExpressionError):
            compile_filter_yaml('col.is_null("unexpected")')

    def test_list_standalone_not_valid(self):
        with pytest.raises(FilterExpressionError, match="List literal"):
            compile_filter_yaml('["a", "b"]')

    def test_invalid_filter_column_in_mapper(self):
        # A filter with an aggregation in it raises when compiled
        with pytest.raises(FilterExpressionError, match="[Aa]ggregat"):
            compile_filter_yaml("sum(x) > 0")

    def test_and_empty_list_raises(self):
        with pytest.raises(ValueError):
            compile_filter_yaml({"and": []})


# ===========================================================================
# Group D — collect_filter_column_refs (TestCollectFilterColumnRefs)
# ===========================================================================

class TestCollectFilterColumnRefs:
    def test_simple_comparison(self):
        refs = collect_filter_column_refs('issue_type == "Story"')
        assert "issue_type" in refs

    def test_in_expr(self):
        refs = collect_filter_column_refs('status in ["open"]')
        assert "status" in refs

    def test_method_not_null(self):
        refs = collect_filter_column_refs("labels.not_null()")
        assert "labels" in refs

    def test_method_contains(self):
        refs = collect_filter_column_refs('labels.contains("bug")')
        assert "labels" in refs

    def test_and_dict(self):
        refs = collect_filter_column_refs({"and": ['status == "open"', "priority > 3"]})
        assert "status" in refs
        assert "priority" in refs

    def test_or_dict(self):
        refs = collect_filter_column_refs({"or": ['status == "open"', 'status == "active"']})
        assert "status" in refs

    def test_not_dict(self):
        refs = collect_filter_column_refs({"not": 'status == "closed"'})
        assert "status" in refs

    def test_now_expression(self):
        # now() has no field refs — just the left side
        refs = collect_filter_column_refs("created_at > now()")
        assert "created_at" in refs
