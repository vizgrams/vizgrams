# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Row-level filter compiler: compiles filter expressions to SQL WHERE clauses.

Uses the shared expression parser (parse_expression_str) rather than the
bespoke filter parser in semantic/transforms.py.
"""
import re as _re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from semantic.expression import (
    AggExpr,
    BinOp,
    CaseWhenExpr,
    DurationLit,
    FieldRef,
    FuncCallExpr,
    InExpr,
    ListLit,
    Lit,
    MethodCallExpr,
    UnaryExpr,
    WindowExpr,
    parse_expression_str,
)


class FilterExpressionError(ValueError):
    """Raised when an expression is invalid in a row-filter context."""


@dataclass
class FilterCompileContext:
    alias: str | None = None
    path_resolver: Callable | None = None  # (list[str]) -> str
    field_override_map: dict | None = None  # str -> str
    dialect: str = "sqlite"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_no_agg_window(expr) -> None:
    """Recursively walk the AST, raise FilterExpressionError on AggExpr/WindowExpr."""
    if isinstance(expr, AggExpr):
        raise FilterExpressionError(
            "Aggregation functions are not allowed in row filters"
        )
    if isinstance(expr, WindowExpr):
        raise FilterExpressionError(
            "Window functions are not allowed in row filters"
        )
    if isinstance(expr, BinOp):
        _validate_no_agg_window(expr.left)
        _validate_no_agg_window(expr.right)
    elif isinstance(expr, UnaryExpr):
        _validate_no_agg_window(expr.expr)
    elif isinstance(expr, FuncCallExpr):
        for a in expr.args:
            _validate_no_agg_window(a)
        for v in expr.kwargs.values():
            _validate_no_agg_window(v)
    elif isinstance(expr, CaseWhenExpr):
        _validate_no_agg_window(expr.when)
        _validate_no_agg_window(expr.then)
        if expr.else_ is not None:
            _validate_no_agg_window(expr.else_)
    elif isinstance(expr, InExpr):
        _validate_no_agg_window(expr.expr)
        for v in expr.values:
            _validate_no_agg_window(v)
    elif isinstance(expr, ListLit):
        for v in expr.values:
            _validate_no_agg_window(v)
    elif isinstance(expr, MethodCallExpr):
        _validate_no_agg_window(expr.expr)
        for a in expr.args:
            _validate_no_agg_window(a)


# ---------------------------------------------------------------------------
# SQL compiler
# ---------------------------------------------------------------------------

_DURATION_DAYS = {'d': 1, 'w': 7}


def _escape_like(s: str) -> str:
    """Escape LIKE wildcards in a string value."""
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _compile_like(col_sql: str, arg_expr, prefix: bool, dialect: str = "sqlite") -> str:
    """Compile col LIKE 'pattern%' or col LIKE '%pattern' with escaping.

    SQLite uses explicit ``ESCAPE '\\'``; ClickHouse treats backslash as the
    default escape character and does not support the ESCAPE clause at all.
    """
    if not isinstance(arg_expr, Lit) or not isinstance(arg_expr.value, str):
        raise FilterExpressionError(
            f"startswith/endswith argument must be a string literal, got {arg_expr!r}"
        )
    escaped = _escape_like(arg_expr.value)
    if dialect == "clickhouse":
        if prefix:
            return f"{col_sql} LIKE '{escaped}%'"
        else:
            return f"{col_sql} LIKE '%{escaped}'"
    if prefix:
        return f"{col_sql} LIKE '{escaped}%' ESCAPE '\\'"
    else:
        return f"{col_sql} LIKE '%{escaped}' ESCAPE '\\'"


def compile_filter_expr(expr, ctx: FilterCompileContext) -> str:
    """Compile a single filter Expr AST node to a SQL fragment."""
    if isinstance(expr, Lit):
        if expr.value is None:
            return "NULL"
        if isinstance(expr.value, bool):
            return "1" if expr.value else "0"
        if isinstance(expr.value, str):
            return "'{}'".format(expr.value.replace("'", "''"))
        return str(expr.value)

    if isinstance(expr, FieldRef):
        parts = expr.parts
        if len(parts) == 1:
            col = parts[0]
            if ctx.field_override_map and col in ctx.field_override_map:
                return ctx.field_override_map[col]
            if ctx.alias:
                return f"{ctx.alias}.{col}"
            return col
        else:
            if ctx.path_resolver:
                return ctx.path_resolver(parts)
            return ".".join(parts)

    if isinstance(expr, BinOp):
        op = expr.op
        # Handle now() +/- DurationLit as a compile-time timestamp computation
        if op in ("+", "-") and isinstance(expr.right, DurationLit):
            dur = expr.right
            days = dur.amount * _DURATION_DAYS[dur.unit]
            delta = timedelta(days=days)
            ts = (
                datetime.now(UTC) + delta if op == "+"
                else datetime.now(UTC) - delta
            ).strftime("%Y-%m-%dT%H:%M:%S")
            if isinstance(expr.left, FuncCallExpr) and expr.left.name == "now":
                return f"'{ts}'"
            # Fallback: compile left normally, then use computed ts
            left_sql = compile_filter_expr(expr.left, ctx)
            return f"({left_sql} {op} '{ts}')"
        left_sql = compile_filter_expr(expr.left, ctx)
        right_sql = compile_filter_expr(expr.right, ctx)
        if op in ("AND", "OR"):
            return f"({left_sql} {op} {right_sql})"
        # Normalise == to SQL =
        if op == "==":
            op = "="
        return f"{left_sql} {op} {right_sql}"

    if isinstance(expr, InExpr):
        _validate_no_agg_window(expr)
        expr_sql = compile_filter_expr(expr.expr, ctx)
        vals_sql = ", ".join(compile_filter_expr(v, ctx) for v in expr.values)
        neg = "NOT " if expr.negated else ""
        return f"{expr_sql} {neg}IN ({vals_sql})"

    if isinstance(expr, UnaryExpr):
        inner_sql = compile_filter_expr(expr.expr, ctx)
        if expr.op == "is_null":
            return f"{inner_sql} IS NULL"
        if expr.op == "is_not_null":
            return f"{inner_sql} IS NOT NULL"
        if expr.op == "not":
            return f"NOT ({inner_sql})"
        if expr.op == "-":
            return f"-{inner_sql}"
        raise FilterExpressionError(f"Unknown unary op: {expr.op!r}")

    if isinstance(expr, FuncCallExpr):
        if expr.name == "now":
            ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")
            return f"'{ts}'"
        if expr.name in ("format_date", "format_time"):
            from engine.function_registry import render_function
            if len(expr.args) != 2:
                raise FilterExpressionError(
                    f"{expr.name} requires exactly 2 arguments (field, pattern)"
                )
            if not isinstance(expr.args[1], Lit) or not isinstance(expr.args[1].value, str):
                raise FilterExpressionError(
                    f"{expr.name} second argument must be a string literal"
                )
            col_sql = compile_filter_expr(expr.args[0], ctx)
            pattern = expr.args[1].value
            return render_function(expr.name, [col_sql], {"pattern": pattern}, dialect=ctx.dialect)
        # Other function calls — pass through as SQL
        args_sql = ", ".join(compile_filter_expr(a, ctx) for a in expr.args)
        return f"{expr.name.upper()}({args_sql})"

    if isinstance(expr, MethodCallExpr):
        col_sql = compile_filter_expr(expr.expr, ctx)
        method = expr.method

        if method == "is_null":
            if expr.args:
                raise FilterExpressionError("is_null() takes no arguments")
            return f"{col_sql} IS NULL"

        if method == "not_null":
            if expr.args:
                raise FilterExpressionError("not_null() takes no arguments")
            return f"{col_sql} IS NOT NULL"

        if method == "startswith":
            if len(expr.args) != 1:
                raise FilterExpressionError("startswith() requires exactly 1 argument")
            return _compile_like(col_sql, expr.args[0], prefix=True, dialect=ctx.dialect)

        if method == "endswith":
            if len(expr.args) != 1:
                raise FilterExpressionError("endswith() requires exactly 1 argument")
            return _compile_like(col_sql, expr.args[0], prefix=False, dialect=ctx.dialect)

        if method == "contains":
            if len(expr.args) != 1:
                raise FilterExpressionError("contains() requires exactly 1 argument")
            val_sql = compile_filter_expr(expr.args[0], ctx)
            if ctx.dialect == "clickhouse":
                return f"has(JSONExtract(ifNull({col_sql}, '[]'), 'Array(String)'), {val_sql})"
            return f"EXISTS (SELECT 1 FROM json_each({col_sql}) WHERE value = {val_sql})"

        if method == "containsAny":
            if len(expr.args) != 1:
                raise FilterExpressionError("containsAny() requires exactly 1 argument")
            arg = expr.args[0]
            if ctx.dialect == "clickhouse":
                if isinstance(arg, ListLit):
                    vals_list = [compile_filter_expr(v, ctx) for v in arg.values]
                    array_lit = f"[{', '.join(vals_list)}]"
                    return f"hasAny(JSONExtract(ifNull({col_sql}, '[]'), 'Array(String)'), {array_lit})"
                else:
                    val_sql = compile_filter_expr(arg, ctx)
                    return f"has(JSONExtract(ifNull({col_sql}, '[]'), 'Array(String)'), {val_sql})"
            if isinstance(arg, ListLit):
                vals_sql = ", ".join(compile_filter_expr(v, ctx) for v in arg.values)
            else:
                vals_sql = compile_filter_expr(arg, ctx)
            return (
                f"EXISTS (SELECT 1 FROM json_each({col_sql}) WHERE value IN ({vals_sql}))"
            )

        if method == "json_any":
            if len(expr.args) != 2:
                raise FilterExpressionError("json_any() requires exactly 2 arguments")
            field_arg = expr.args[0]
            val_arg = expr.args[1]
            if isinstance(field_arg, Lit) and isinstance(field_arg.value, str):
                field_str = field_arg.value
            else:
                raise FilterExpressionError(
                    "json_any() first argument must be a string literal"
                )
            val_sql = compile_filter_expr(val_arg, ctx)
            if ctx.dialect == "clickhouse":
                return (
                    f"arrayExists(x -> JSONExtractString(x, '{field_str}') = {val_sql}, "
                    f"JSONExtractArrayRaw(ifNull({col_sql}, '[]')))"
                )
            return (
                f"EXISTS (SELECT 1 FROM json_each({col_sql}) "
                f"WHERE json_extract(value, '$.{field_str}') = {val_sql})"
            )

        raise FilterExpressionError(f"Unknown filter method: {method!r}")

    if isinstance(expr, CaseWhenExpr):
        when_sql = compile_filter_expr(expr.when, ctx)
        then_sql = compile_filter_expr(expr.then, ctx)
        if expr.else_ is not None:
            else_sql = compile_filter_expr(expr.else_, ctx)
            return f"CASE WHEN {when_sql} THEN {then_sql} ELSE {else_sql} END"
        return f"CASE WHEN {when_sql} THEN {then_sql} END"

    if isinstance(expr, ListLit):
        raise FilterExpressionError(
            "List literal is only valid inside 'in' / 'not_in'"
        )

    if isinstance(expr, DurationLit):
        raise FilterExpressionError(
            "Duration literal is only valid in 'now() +/- Nd' expressions"
        )

    if isinstance(expr, AggExpr):
        raise FilterExpressionError(
            "Aggregation functions are not allowed in row filters"
        )

    if isinstance(expr, WindowExpr):
        raise FilterExpressionError(
            "Window functions are not allowed in row filters"
        )

    raise FilterExpressionError(f"Cannot compile filter node: {type(expr).__name__}")


def compile_filter_yaml(
    filter_data,
    alias: str | None = None,
    path_resolver=None,
    field_override_map: dict | None = None,
    dialect: str = "sqlite",
) -> str:
    """Recursively compile a YAML filter structure to a SQL WHERE clause.

    Args:
        filter_data: str (leaf), dict with and/or/not, or nested structure
        alias: If provided, prefix bare column references with this alias.
        path_resolver: Optional callable(parts: list[str]) -> str for traversal refs.
        field_override_map: Optional mapping of bare field name -> SQL expression override.
        dialect: SQL dialect ("sqlite" or "clickhouse").
    """
    if isinstance(filter_data, str):
        expr = parse_expression_str(filter_data)
        _validate_no_agg_window(expr)
        ctx = FilterCompileContext(
            alias=alias,
            path_resolver=path_resolver,
            field_override_map=field_override_map,
            dialect=dialect,
        )
        return compile_filter_expr(expr, ctx)

    if isinstance(filter_data, dict):
        if "and" in filter_data:
            items = filter_data["and"]
            if not items:
                raise ValueError(f"Invalid filter structure: {filter_data!r}")
            clauses = [
                compile_filter_yaml(c, alias, path_resolver, field_override_map, dialect)
                for c in items
            ]
            return "(" + " AND ".join(clauses) + ")"
        if "or" in filter_data:
            items = filter_data["or"]
            if not items:
                raise ValueError(f"Invalid filter structure: {filter_data!r}")
            clauses = [
                compile_filter_yaml(c, alias, path_resolver, field_override_map, dialect)
                for c in items
            ]
            return "(" + " OR ".join(clauses) + ")"
        if "not" in filter_data:
            inner = compile_filter_yaml(
                filter_data["not"], alias, path_resolver, field_override_map, dialect
            )
            return f"NOT ({inner})"

    raise ValueError(f"Invalid filter structure: {filter_data!r}")


# ---------------------------------------------------------------------------
# Column reference collection
# ---------------------------------------------------------------------------

def _collect_field_refs_from_expr(expr) -> list[str]:
    """Walk an AST and collect single-part FieldRef names."""
    result = []
    if isinstance(expr, FieldRef):
        if len(expr.parts) == 1:
            result.append(expr.parts[0])
    elif isinstance(expr, MethodCallExpr):
        # Collect from receiver if it's a bare FieldRef
        if isinstance(expr.expr, FieldRef) and len(expr.expr.parts) == 1:
            result.append(expr.expr.parts[0])
        else:
            result.extend(_collect_field_refs_from_expr(expr.expr))
        for a in expr.args:
            result.extend(_collect_field_refs_from_expr(a))
    elif isinstance(expr, BinOp):
        result.extend(_collect_field_refs_from_expr(expr.left))
        result.extend(_collect_field_refs_from_expr(expr.right))
    elif isinstance(expr, UnaryExpr):
        result.extend(_collect_field_refs_from_expr(expr.expr))
    elif isinstance(expr, FuncCallExpr):
        for a in expr.args:
            result.extend(_collect_field_refs_from_expr(a))
        for v in expr.kwargs.values():
            result.extend(_collect_field_refs_from_expr(v))
    elif isinstance(expr, InExpr):
        result.extend(_collect_field_refs_from_expr(expr.expr))
        for v in expr.values:
            result.extend(_collect_field_refs_from_expr(v))
    elif isinstance(expr, ListLit):
        for v in expr.values:
            result.extend(_collect_field_refs_from_expr(v))
    elif isinstance(expr, CaseWhenExpr):
        result.extend(_collect_field_refs_from_expr(expr.when))
        result.extend(_collect_field_refs_from_expr(expr.then))
        if expr.else_ is not None:
            result.extend(_collect_field_refs_from_expr(expr.else_))
    return result


_PARAM_PLACEHOLDER_RE = _re.compile(r'\{(\w+)\}')


def apply_params(
    filters: list,
    param_defs: list,
    param_values: dict,
) -> list:
    """Substitute {param_name} placeholders in filter strings.

    For each filter string:
    - Placeholders with a supplied value (or default) are substituted.
    - Filters containing an optional param with no value and no default are dropped.
    - Non-string filter items are passed through unchanged.
    """
    param_map = {p.name: p for p in param_defs}
    result = []
    for item in filters:
        if not isinstance(item, str):
            result.append(item)
            continue
        names = _PARAM_PLACEHOLDER_RE.findall(item)
        if not names:
            result.append(item)
            continue
        resolved = item
        drop = False
        for name in names:
            value = param_values.get(name) or None  # treat "" as no value
            if value is None:
                param_def = param_map.get(name)
                if param_def is not None and param_def.default is not None:
                    value = param_def.default
                elif param_def is None or param_def.optional:
                    # Unknown param (not declared in params:) or optional with no value → drop filter
                    drop = True
                    break
            resolved = resolved.replace(f'{{{name}}}', str(value) if value is not None else '')
        if not drop:
            result.append(resolved)
    return result


def collect_filter_column_refs(filter_data) -> list[str]:
    """Extract bare column names from all leaf expressions in a filter."""
    if isinstance(filter_data, str):
        # Strip unresolved param placeholders ({param_name}) so the expression
        # parser doesn't choke on them — we only need field refs, not values.
        clean = _PARAM_PLACEHOLDER_RE.sub('0', filter_data)
        try:
            expr = parse_expression_str(clean)
        except ValueError:
            return []
        return _collect_field_refs_from_expr(expr)

    if isinstance(filter_data, dict):
        refs = []
        if "and" in filter_data:
            for item in filter_data["and"]:
                refs.extend(collect_filter_column_refs(item))
        if "or" in filter_data:
            for item in filter_data["or"]:
                refs.extend(collect_filter_column_refs(item))
        if "not" in filter_data:
            refs.extend(collect_filter_column_refs(filter_data["not"]))
        return refs

    return []
