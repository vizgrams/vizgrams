# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for engine.python_evaluator — Group F per ADR-121."""

import pytest

from engine.filter_compiler import FilterExpressionError
from engine.python_evaluator import evaluate, parse_and_evaluate
from semantic.expression import parse_expression_str

# ===========================================================================
# Group F — Python evaluator (TestPythonEvaluator)
# ===========================================================================

class TestPythonEvaluator:
    # --- Field resolution ---

    def test_two_part_field_ref(self):
        row = {"alias": {"col": "value"}}
        result = evaluate(parse_expression_str("alias.col"), row)
        assert result == "value"

    def test_missing_alias_returns_none(self):
        row = {"other": {"col": "value"}}
        result = evaluate(parse_expression_str("alias.col"), row)
        assert result is None

    def test_missing_col_returns_none(self):
        row = {"alias": {"other": "value"}}
        result = evaluate(parse_expression_str("alias.col"), row)
        assert result is None

    def test_single_part_field_ref_found(self):
        row = {"alias": {"col": "value"}}
        result = evaluate(parse_expression_str("col"), row)
        assert result == "value"

    def test_single_part_field_ref_not_found(self):
        row = {"alias": {"other": "value"}}
        result = evaluate(parse_expression_str("missing"), row)
        assert result is None

    # --- Literals ---

    def test_string_lit(self):
        result = evaluate(parse_expression_str('"hello"'), {})
        assert result == "hello"

    def test_number_lit(self):
        result = evaluate(parse_expression_str("42"), {})
        assert result == 42

    def test_null_lit(self):
        result = evaluate(parse_expression_str("null"), {})
        assert result is None

    # --- String functions ---

    def test_concat_strings(self):
        result = evaluate(parse_expression_str('CONCAT("a", "b")'), {})
        assert result == "ab"

    def test_concat_null_propagates(self):
        row = {"src": {"val": None}}
        result = evaluate(parse_expression_str('CONCAT("a", src.val, "b")'), row)
        assert result is None

    def test_concat_with_col_ref(self):
        row = {"alias": {"col": "value"}}
        result = evaluate(parse_expression_str('CONCAT("prefix_", alias.col)'), row)
        assert result == "prefix_value"

    def test_trim(self):
        result = evaluate(parse_expression_str('TRIM(" x ")'), {})
        assert result == "x"

    def test_lower(self):
        result = evaluate(parse_expression_str('LOWER("Hello")'), {})
        assert result == "hello"

    def test_title(self):
        result = evaluate(parse_expression_str('TITLE("hello world")'), {})
        assert result == "Hello World"

    def test_upper_snake(self):
        result = evaluate(parse_expression_str('UPPER_SNAKE("hello world")'), {})
        assert result == "HELLO_WORLD"

    def test_regex_extract_match(self):
        result = evaluate(parse_expression_str('REGEX_EXTRACT("abc123", "[0-9]+")'), {})
        assert result == "123"

    def test_regex_extract_no_match(self):
        result = evaluate(parse_expression_str('REGEX_EXTRACT("abc", "[0-9]+")'), {})
        assert result is None

    # --- Identity functions ---

    def test_ulid_deterministic(self):
        result1 = evaluate(parse_expression_str('ULID("seed")'), {})
        result2 = evaluate(parse_expression_str('ULID("seed")'), {})
        assert result1 == result2
        assert len(result1) == 26

    def test_ulid_distinct_inputs(self):
        result1 = evaluate(parse_expression_str('ULID("seed1")'), {})
        result2 = evaluate(parse_expression_str('ULID("seed2")'), {})
        assert result1 != result2

    def test_ulid_null_propagates(self):
        row = {"src": {"val": None}}
        result = evaluate(parse_expression_str("ULID(src.val)"), row)
        assert result is None

    # --- Conditional functions ---

    def test_coalesce_first_non_null(self):
        result = evaluate(parse_expression_str('COALESCE(null, "b")'), {})
        assert result == "b"

    def test_coalesce_all_null(self):
        result = evaluate(parse_expression_str("COALESCE(null, null)"), {})
        assert result is None

    def test_if_not_null_truthy(self):
        result = evaluate(parse_expression_str('IF_NOT_NULL("x", "result")'), {})
        assert result == "result"

    def test_if_not_null_null_check(self):
        result = evaluate(parse_expression_str('IF_NOT_NULL(null, "result")'), {})
        assert result is None

    def test_default_fallback(self):
        result = evaluate(parse_expression_str('DEFAULT(null, "fallback")'), {})
        assert result == "fallback"

    def test_default_no_fallback(self):
        result = evaluate(parse_expression_str('DEFAULT("val", "fallback")'), {})
        assert result == "val"

    # --- JSON functions ---

    def test_json_extract_key(self):
        import json
        row = {"r": {"data": json.dumps({"a": 1})}}
        result = evaluate(parse_expression_str('JSON_EXTRACT(r.data, "a")'), row)
        assert result == 1

    def test_json_find_match(self):
        import json
        items = [{"k": "x", "v": "y"}, {"k": "z", "v": "w"}]
        row = {"r": {"items": json.dumps(items)}}
        result = evaluate(parse_expression_str('JSON_FIND(r.items, "k", "x", "v")'), row)
        assert result == "y"

    def test_json_find_no_match(self):
        import json
        items = [{"k": "other", "v": "y"}]
        row = {"r": {"items": json.dumps(items)}}
        result = evaluate(parse_expression_str('JSON_FIND(r.items, "k", "x", "v")'), row)
        assert result is None

    # --- Complex expression ---

    def test_identity_key_expression(self):
        row = {"alias": {"login": "jdoe"}}
        expr_str = 'CONCAT("identity_", ULID(CONCAT("github", "|", "handle", "|", alias.login)))'
        result = evaluate(parse_expression_str(expr_str), row)
        assert isinstance(result, str)
        assert result.startswith("identity_")
        assert len(result) == len("identity_") + 26

    def test_identity_key_stability(self):
        row = {"alias": {"login": "jdoe"}}
        expr_str = 'CONCAT("identity_", ULID(CONCAT("github", "|", "handle", "|", alias.login)))'
        expr = parse_expression_str(expr_str)
        result1 = evaluate(expr, row)
        result2 = evaluate(expr, row)
        assert result1 == result2

    # --- No-agg guard ---

    def test_agg_in_evaluator_raises(self):
        with pytest.raises((FilterExpressionError, ValueError)):
            evaluate(parse_expression_str("sum(x)"), {"src": {"x": 1}})

    def test_window_in_evaluator_raises(self):
        with pytest.raises((FilterExpressionError, ValueError)):
            evaluate(
                parse_expression_str("lag(x).over(key, date)"),
                {"src": {"x": 1, "key": "k", "date": "2024-01-01"}},
            )

    # --- in / not_in in Python evaluator ---

    def test_in_evaluator_true(self):
        row = {"src": {"status": "open"}}
        result = evaluate(
            parse_expression_str('src.status in ["open", "active"]'), row
        )
        assert result is True

    def test_in_evaluator_false(self):
        row = {"src": {"status": "closed"}}
        result = evaluate(
            parse_expression_str('src.status in ["open", "active"]'), row
        )
        assert result is False

    def test_not_in_evaluator(self):
        row = {"src": {"status": "closed"}}
        result = evaluate(
            parse_expression_str('src.status not_in ["open", "active"]'), row
        )
        assert result is True

    # --- parse_and_evaluate convenience wrapper ---

    def test_parse_and_evaluate(self):
        row = {"src": {"name": "Alice"}}
        result = parse_and_evaluate('LOWER(src.name)', row)
        assert result == "alice"
