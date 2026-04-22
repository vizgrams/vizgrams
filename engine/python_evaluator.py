# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Python row-by-row evaluator for mapper column expressions.

Operates on the shared expression AST from semantic.expression.parse_expression_str.
"""
import hashlib
import json
import re

from engine.filter_compiler import FilterExpressionError
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

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _deterministic_ulid(seed: str) -> str:
    """Generate a stable 26-char Crockford base32 ID from seed text."""
    raw = hashlib.sha256(seed.encode()).digest()[:16]
    n = int.from_bytes(raw, "big")
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[n & 0x1F])
        n >>= 5
    return "".join(reversed(chars))


def _upper_snake(s: str) -> str:
    """Replace non-alphanumeric runs with _, uppercase."""
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s.strip())
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    return s.upper().strip("_")


def _cast(value, type_name: str):
    """Cast a value to the given type."""
    if value is None:
        return None
    type_name = type_name.lower()
    if type_name == "integer":
        return int(value)
    if type_name == "float":
        return float(value)
    if type_name == "boolean":
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes")
        return bool(value)
    if type_name in ("date", "datetime", "string"):
        return str(value)
    raise ValueError(f"Unknown CAST type: {type_name!r}")


def _validate_no_agg_window(expr) -> None:
    """Raise FilterExpressionError if expr contains AggExpr or WindowExpr."""
    if isinstance(expr, AggExpr):
        raise FilterExpressionError(
            "Aggregation functions are not allowed in mapper column expressions"
        )
    if isinstance(expr, WindowExpr):
        raise FilterExpressionError(
            "Window functions are not allowed in mapper column expressions"
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


def evaluate(expr, row: dict, enums: dict | None = None) -> object:
    """Evaluate an expression AST node against a joined row.

    Args:
        expr: AST node from parse_expression_str()
        row: nested dict {alias: {column: value}}
        enums: dict of {name: EnumMapping} for ENUM() lookups
    """
    if enums is None:
        enums = {}

    if isinstance(expr, Lit):
        return expr.value

    if isinstance(expr, FieldRef):
        parts = expr.parts
        if len(parts) == 2:
            alias, col = parts
            return row.get(alias, {}).get(col)
        if len(parts) == 1:
            col = parts[0]
            # Search all aliases for first non-None value
            for alias_data in row.values():
                if isinstance(alias_data, dict) and col in alias_data:
                    val = alias_data[col]
                    if val is not None:
                        return val
            # Return None if not found or all None
            return None
        raise ValueError(
            f"FieldRef with {len(parts)} parts not supported in Python evaluator"
        )

    if isinstance(expr, BinOp):
        op = expr.op
        left = evaluate(expr.left, row, enums)
        right = evaluate(expr.right, row, enums)
        if op == "+":
            if left is None or right is None:
                return None
            return left + right
        if op == "-":
            if left is None or right is None:
                return None
            return left - right
        if op == "*":
            if left is None or right is None:
                return None
            return left * right
        if op == "/":
            if left is None or right is None:
                return None
            if right == 0:
                return None
            return left / right
        if op in ("AND", "and"):
            return bool(left) and bool(right)
        if op in ("OR", "or"):
            return bool(left) or bool(right)
        if op in ("=", "=="):
            return left == right
        if op == "!=":
            return left != right
        if op == "<":
            return left < right
        if op == "<=":
            return left <= right
        if op == ">":
            return left > right
        if op == ">=":
            return left >= right
        raise ValueError(f"Unknown binary operator: {op!r}")

    if isinstance(expr, AggExpr):
        raise FilterExpressionError(
            "Aggregation functions are not allowed in mapper column expressions"
        )

    if isinstance(expr, WindowExpr):
        raise FilterExpressionError(
            "Window functions are not allowed in mapper column expressions"
        )

    if isinstance(expr, FuncCallExpr):
        fn = expr.name.upper()

        if fn == "DEFAULT":
            if len(expr.args) != 2:
                raise ValueError("DEFAULT requires exactly 2 arguments")
            val = evaluate(expr.args[0], row, enums)
            if val is None:
                return evaluate(expr.args[1], row, enums)
            return val

        if fn == "ENUM":
            if len(expr.args) != 2:
                raise ValueError("ENUM requires exactly 2 arguments")
            val = evaluate(expr.args[0], row, enums)
            if val is None:
                return None
            mapping_name_expr = expr.args[1]
            # A bare identifier (FieldRef with one part) is the enum mapping name.
            # This preserves backward-compat with ENUM(src.col, mapping_name) syntax
            # where mapping_name is a bare identifier (not a field reference).
            if isinstance(mapping_name_expr, FieldRef) and len(mapping_name_expr.parts) == 1:
                mapping_name = mapping_name_expr.parts[0]
            else:
                mapping_name = evaluate(mapping_name_expr, row, enums)
            if mapping_name is None:
                raise ValueError("ENUM mapping name evaluated to None")
            enum_map = enums.get(str(mapping_name))
            if enum_map is None:
                raise ValueError(f"ENUM mapping {mapping_name!r} not found")
            return enum_map.reverse_lookup(str(val))

        if fn == "CAST":
            if len(expr.args) != 2:
                raise ValueError("CAST requires exactly 2 arguments")
            val = evaluate(expr.args[0], row, enums)
            type_name_expr = expr.args[1]
            # A bare identifier (FieldRef with one part) is the type name.
            if isinstance(type_name_expr, FieldRef) and len(type_name_expr.parts) == 1:
                type_name = type_name_expr.parts[0]
            else:
                type_name = evaluate(type_name_expr, row, enums)
            return _cast(val, str(type_name))

        if fn == "JSON_EXTRACT":
            if len(expr.args) != 2:
                raise ValueError("JSON_EXTRACT requires exactly 2 arguments")
            val = evaluate(expr.args[0], row, enums)
            if val is None:
                return None
            field_name = evaluate(expr.args[1], row, enums)
            obj = json.loads(val) if isinstance(val, str) else val
            return obj.get(field_name)

        if fn == "JSON_FIND":
            if len(expr.args) != 4:
                raise ValueError("JSON_FIND requires exactly 4 arguments")
            val = evaluate(expr.args[0], row, enums)
            if val is None:
                return None
            match_key = evaluate(expr.args[1], row, enums)
            match_value = evaluate(expr.args[2], row, enums)
            extract_key = evaluate(expr.args[3], row, enums)
            arr = json.loads(val) if isinstance(val, str) else val
            for element in arr:
                if element.get(match_key) == match_value:
                    return element.get(extract_key)
            return None

        if fn == "CONCAT":
            parts = [evaluate(a, row, enums) for a in expr.args]
            if any(p is None for p in parts):
                return None
            return "".join(str(p) for p in parts)

        if fn == "COALESCE":
            for arg in expr.args:
                val = evaluate(arg, row, enums)
                if val is not None:
                    return val
            return None

        if fn == "IF_NOT_NULL":
            if len(expr.args) != 2:
                raise ValueError("IF_NOT_NULL requires exactly 2 arguments")
            check = evaluate(expr.args[0], row, enums)
            return evaluate(expr.args[1], row, enums) if check is not None else None


        if fn == "REGEX_EXTRACT":
            if len(expr.args) != 2:
                raise ValueError("REGEX_EXTRACT requires exactly 2 arguments")
            val = evaluate(expr.args[0], row, enums)
            if val is None:
                return None
            pattern = evaluate(expr.args[1], row, enums)
            m = re.search(str(pattern), str(val))
            return m.group(0) if m else None

        # Single-arg functions with null propagation
        if fn in ("ULID", "TRIM", "UPPER_SNAKE", "TITLE", "LOWER"):
            if len(expr.args) != 1:
                raise ValueError(f"{fn} requires exactly 1 argument")
            val = evaluate(expr.args[0], row, enums)
            if val is None:
                return None
            if fn == "ULID":
                return _deterministic_ulid(str(val))
            if fn == "TRIM":
                return str(val).strip()
            if fn == "UPPER_SNAKE":
                return _upper_snake(str(val))
            if fn == "TITLE":
                return str(val).title()
            if fn == "LOWER":
                return str(val).lower()

        raise ValueError(f"Unknown function: {fn!r}")

    if isinstance(expr, UnaryExpr):
        op = expr.op
        if op == "is_null":
            return evaluate(expr.expr, row, enums) is None
        if op == "is_not_null":
            return evaluate(expr.expr, row, enums) is not None
        if op == "not":
            return not evaluate(expr.expr, row, enums)
        if op == "-":
            val = evaluate(expr.expr, row, enums)
            if val is None:
                return None
            return -val
        raise ValueError(f"Unknown unary op: {op!r}")

    if isinstance(expr, CaseWhenExpr):
        cond = evaluate(expr.when, row, enums)
        if cond:
            return evaluate(expr.then, row, enums)
        if expr.else_ is not None:
            return evaluate(expr.else_, row, enums)
        return None

    if isinstance(expr, MethodCallExpr):
        val = evaluate(expr.expr, row, enums)
        method = expr.method

        if method == "is_null":
            return val is None
        if method == "not_null":
            return val is not None

        if val is None:
            return None

        if method == "startswith":
            s = evaluate(expr.args[0], row, enums)
            return str(val).startswith(str(s)) if s is not None else False
        if method == "endswith":
            s = evaluate(expr.args[0], row, enums)
            return str(val).endswith(str(s)) if s is not None else False
        if method == "contains":
            v = evaluate(expr.args[0], row, enums)
            arr = json.loads(val) if isinstance(val, str) else val
            return v in arr if arr is not None else False
        if method == "containsAny":
            arg = expr.args[0]
            if isinstance(arg, ListLit):
                check_vals = [evaluate(v, row, enums) for v in arg.values]
            else:
                check_vals = evaluate(arg, row, enums)
                if not isinstance(check_vals, list):
                    check_vals = [check_vals]
            arr = json.loads(val) if isinstance(val, str) else val
            return any(v in arr for v in check_vals) if arr is not None else False
        if method == "json_any":
            field_name = evaluate(expr.args[0], row, enums)
            check_val = evaluate(expr.args[1], row, enums)
            arr = json.loads(val) if isinstance(val, str) else val
            if arr is None:
                return False
            return any(el.get(field_name) == check_val for el in arr)

        raise ValueError(f"Unknown method: {method!r}")

    if isinstance(expr, InExpr):
        lhs = evaluate(expr.expr, row, enums)
        vals = [evaluate(v, row, enums) for v in expr.values]
        result = lhs in vals
        return result ^ expr.negated if expr.negated else result

    if isinstance(expr, ListLit):
        return [evaluate(v, row, enums) for v in expr.values]

    if isinstance(expr, DurationLit):
        # In Python evaluator, DurationLit by itself isn't meaningful
        raise ValueError(
            "DurationLit is only valid in 'now() - Nd' expressions"
        )

    raise ValueError(f"Unknown AST node type: {type(expr).__name__}")


def parse_and_evaluate(
    expr_str: str, row: dict, enums: dict | None = None
) -> object:
    """Parse an expression string and evaluate it against a row.

    Convenience wrapper around parse_expression_str + evaluate.
    """
    expr = parse_expression_str(expr_str)
    return evaluate(expr, row, enums)


# ---------------------------------------------------------------------------
# Reference collection helpers (used by semantic/mapper.py validation)
# ---------------------------------------------------------------------------

def collect_refs(node) -> list[FieldRef]:
    """Extract all two-part FieldRef nodes (alias.column) from an AST.

    Returns FieldRef objects with .parts = [alias, column].
    """
    if isinstance(node, FieldRef):
        if len(node.parts) == 2:
            return [node]
        return []
    if isinstance(node, FuncCallExpr):
        refs = []
        for arg in node.args:
            refs.extend(collect_refs(arg))
        for v in node.kwargs.values():
            refs.extend(collect_refs(v))
        return refs
    if isinstance(node, BinOp):
        return collect_refs(node.left) + collect_refs(node.right)
    if isinstance(node, UnaryExpr):
        return collect_refs(node.expr)
    if isinstance(node, MethodCallExpr):
        refs = collect_refs(node.expr)
        for a in node.args:
            refs.extend(collect_refs(a))
        return refs
    if isinstance(node, CaseWhenExpr):
        refs = collect_refs(node.when) + collect_refs(node.then)
        if node.else_ is not None:
            refs.extend(collect_refs(node.else_))
        return refs
    if isinstance(node, InExpr):
        refs = collect_refs(node.expr)
        for v in node.values:
            refs.extend(collect_refs(v))
        return refs
    if isinstance(node, ListLit):
        refs = []
        for v in node.values:
            refs.extend(collect_refs(v))
        return refs
    return []


def collect_enum_refs(node) -> list[str]:
    """Extract ENUM mapping name references from an AST.

    Returns list of enum mapping name strings.
    """
    if isinstance(node, FuncCallExpr) and node.name.upper() == "ENUM":
        # ENUM(expression, mapping_name) — mapping_name is a bare FieldRef with 1 part
        names = []
        for arg in node.args:
            if isinstance(arg, FieldRef) and len(arg.parts) == 1:
                names.append(arg.parts[0])
            elif isinstance(arg, Lit) and isinstance(arg.value, str):
                names.append(arg.value)
            elif isinstance(arg, FuncCallExpr):
                names.extend(collect_enum_refs(arg))
        return names
    if isinstance(node, FuncCallExpr):
        names = []
        for arg in node.args:
            names.extend(collect_enum_refs(arg))
        for v in node.kwargs.values():
            names.extend(collect_enum_refs(v))
        return names
    if isinstance(node, BinOp):
        return collect_enum_refs(node.left) + collect_enum_refs(node.right)
    if isinstance(node, UnaryExpr):
        return collect_enum_refs(node.expr)
    if isinstance(node, MethodCallExpr):
        refs = collect_enum_refs(node.expr)
        for a in node.args:
            refs.extend(collect_enum_refs(a))
        return refs
    if isinstance(node, CaseWhenExpr):
        refs = collect_enum_refs(node.when) + collect_enum_refs(node.then)
        if node.else_ is not None:
            refs.extend(collect_enum_refs(node.else_))
        return refs
    if isinstance(node, InExpr):
        refs = collect_enum_refs(node.expr)
        for v in node.values:
            refs.extend(collect_enum_refs(v))
        return refs
    if isinstance(node, ListLit):
        refs = []
        for v in node.values:
            refs.extend(collect_enum_refs(v))
        return refs
    return []
