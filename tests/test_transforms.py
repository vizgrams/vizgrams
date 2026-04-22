# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for JSON expression functions and json_any filter in semantic.transforms."""

import json

from engine.filter_compiler import (
    FilterCompileContext,
    compile_filter_expr,
)
from engine.python_evaluator import evaluate
from semantic.expression import parse_expression_str


def parse_expression(expr_str):
    """Compat shim: parse using the new shared expression parser."""
    return parse_expression_str(expr_str)


def parse_filter_expression(expr_str):
    """Compat shim: parse filter expressions using the new shared parser."""
    return parse_expression_str(expr_str)


def compile_filter_sql(node, alias=None):
    """Compat shim: compile a parsed node to SQL using the new compiler."""
    return compile_filter_expr(node, FilterCompileContext(alias=alias))


# ---------------------------------------------------------------------------
# JSON_EXTRACT
# ---------------------------------------------------------------------------


class TestJsonExtract:
    def test_valid_json_object(self):
        node = parse_expression('JSON_EXTRACT(row.data, "name")')
        row = {"row": {"data": json.dumps({"name": "Alice", "age": 30})}}
        assert evaluate(node, row) == "Alice"

    def test_missing_key(self):
        node = parse_expression('JSON_EXTRACT(row.data, "missing")')
        row = {"row": {"data": json.dumps({"name": "Alice"})}}
        assert evaluate(node, row) is None

    def test_null_input(self):
        node = parse_expression('JSON_EXTRACT(row.data, "name")')
        row = {"row": {"data": None}}
        assert evaluate(node, row) is None


# ---------------------------------------------------------------------------
# JSON_FIND
# ---------------------------------------------------------------------------


class TestJsonFind:
    def test_find_matching_element(self):
        node = parse_expression('JSON_FIND(row.items, "fieldId", "status", "toString")')
        items = [
            {"fieldId": "priority", "toString": "High"},
            {"fieldId": "status", "toString": "In Progress"},
        ]
        row = {"row": {"items": json.dumps(items)}}
        assert evaluate(node, row) == "In Progress"

    def test_no_match(self):
        node = parse_expression('JSON_FIND(row.items, "fieldId", "status", "toString")')
        items = [{"fieldId": "priority", "toString": "High"}]
        row = {"row": {"items": json.dumps(items)}}
        assert evaluate(node, row) is None

    def test_null_input(self):
        node = parse_expression('JSON_FIND(row.items, "fieldId", "status", "toString")')
        row = {"row": {"items": None}}
        assert evaluate(node, row) is None

    def test_nested_with_upper_snake(self):
        node = parse_expression('UPPER_SNAKE(JSON_FIND(row.items, "fieldId", "status", "toString"))')
        items = [{"fieldId": "status", "toString": "In Progress"}]
        row = {"row": {"items": json.dumps(items)}}
        assert evaluate(node, row) == "IN_PROGRESS"


# ---------------------------------------------------------------------------
# json_any filter
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# CONCAT
# ---------------------------------------------------------------------------


class TestConcat:
    def test_basic_concat(self):
        node = parse_expression('CONCAT(r.repo, "#", r.number)')
        row = {"r": {"repo": "my-repo", "number": 42}}
        assert evaluate(node, row) == "my-repo#42"

    def test_two_args(self):
        node = parse_expression('CONCAT(r.first, r.last)')
        row = {"r": {"first": "Hello", "last": "World"}}
        assert evaluate(node, row) == "HelloWorld"

    def test_single_arg(self):
        node = parse_expression('CONCAT(r.name)')
        row = {"r": {"name": "solo"}}
        assert evaluate(node, row) == "solo"

    def test_null_propagation(self):
        node = parse_expression('CONCAT(r.repo, "#", r.number)')
        row = {"r": {"repo": "my-repo", "number": None}}
        assert evaluate(node, row) is None

    def test_all_literals(self):
        node = parse_expression('CONCAT("a", "b", "c")')
        row = {}
        assert evaluate(node, row) == "abc"

    def test_numeric_coercion(self):
        node = parse_expression('CONCAT(r.prefix, r.num)')
        row = {"r": {"prefix": "PR-", "num": 123}}
        assert evaluate(node, row) == "PR-123"



# ---------------------------------------------------------------------------
# json_any filter
# ---------------------------------------------------------------------------


class TestJsonAnyFilter:
    def test_compiles_to_correct_sql(self):
        node = parse_filter_expression('items.json_any("fieldId", "status")')
        sql = compile_filter_sql(node, alias="sc")
        assert sql == (
            "EXISTS (SELECT 1 FROM json_each(sc.items) "
            "WHERE json_extract(value, '$.fieldId') = 'status')"
        )

    def test_without_alias(self):
        node = parse_filter_expression('items.json_any("fieldId", "status")')
        sql = compile_filter_sql(node)
        assert sql == (
            "EXISTS (SELECT 1 FROM json_each(items) "
            "WHERE json_extract(value, '$.fieldId') = 'status')"
        )
