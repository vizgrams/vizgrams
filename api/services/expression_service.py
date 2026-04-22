# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Expression validation, preview, and function browser service."""

from __future__ import annotations

from pathlib import Path

from engine.expression_compiler import (
    CompileContext,
    _has_aggregation,
    _join_clauses_from_steps,
    _make_alias,
    compile_expr,
)
from engine.filter_compiler import FilterCompileContext, compile_filter_expr
from semantic.expression import parse_expression_str
from semantic.yaml_adapter import YAMLAdapter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_entities(model_dir: Path) -> dict:
    ontology_dir = model_dir / "ontology"
    return {e.name: e for e in YAMLAdapter.load_entities(ontology_dir)}


def _open_db(model_dir: Path):
    from core.db import get_backend
    backend = get_backend(model_dir)
    backend.connect()
    return backend


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------

def validate_expression(
    model_dir: Path,
    entity_name: str,
    expr_str: str,
    mode: str,  # "feature" | "measure" | "filter"
) -> dict:
    """Validate a single expression in context and return compiled SQL."""
    errors: list[dict] = []

    # 1. Parse
    try:
        expr_ast = parse_expression_str(expr_str)
    except Exception as e:
        return {"valid": False, "errors": [{"message": str(e)}], "compiled_sql": None}

    # 2. Mode checks — measures require an aggregation; features/filters have no restriction
    has_agg = _has_aggregation(expr_ast)
    if mode == "measure" and not has_agg:
        errors.append({"message": "Measures must use an aggregation function (sum, avg, count, etc.)"})

    if errors:
        return {"valid": False, "errors": errors, "compiled_sql": None}

    # 3. Compile
    from core.model_config import load_database_config
    dialect = load_database_config(model_dir).get("backend", "clickhouse")

    entities = _load_entities(model_dir)
    entity = entities.get(entity_name)
    if entity is None:
        return {
            "valid": False,
            "errors": [{"message": f"Entity '{entity_name}' not found"}],
            "compiled_sql": None,
        }

    used: set[str] = set()
    alias = _make_alias(entity_name, used)
    ctx = CompileContext(
        root_entity=entity,
        root_alias=alias,
        entities=entities,
        join_steps=[],
        joined={},
        dialect=dialect,
    )

    try:
        if mode == "filter":
            filter_ctx = FilterCompileContext(alias=alias, dialect=dialect)
            compiled_sql = compile_filter_expr(expr_ast, filter_ctx)
        else:
            compiled_sql = compile_expr(expr_ast, ctx)
    except Exception as e:
        return {"valid": False, "errors": [{"message": str(e)}], "compiled_sql": None}

    return {"valid": True, "errors": [], "compiled_sql": compiled_sql}


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

def preview_expression(
    model_dir: Path,
    entity_name: str,
    expr_str: str,
    entity_id: str | None = None,
) -> dict:
    """Evaluate an expression against a single real record."""
    validation = validate_expression(model_dir, entity_name, expr_str, mode="feature")
    if not validation["valid"]:
        raise ValueError(validation["errors"][0]["message"])

    from core.model_config import load_database_config
    dialect = load_database_config(model_dir).get("backend", "clickhouse")

    entities = _load_entities(model_dir)
    entity = entities[entity_name]

    # Compile again to get join_steps
    expr_ast = parse_expression_str(expr_str)
    used: set[str] = set()
    alias = _make_alias(entity_name, used)
    ctx = CompileContext(
        root_entity=entity,
        root_alias=alias,
        entities=entities,
        join_steps=[],
        joined={},
        dialect=dialect,
    )
    value_sql = compile_expr(expr_ast, ctx)
    join_clauses = _join_clauses_from_steps(ctx.join_steps)

    pk = entity.primary_key
    if pk is None:
        raise ValueError(f"Entity '{entity_name}' has no PRIMARY_KEY")

    backend = _open_db(model_dir)
    try:
        table = entity.table_name
        if not backend.table_exists(table):
            # Legacy SQLite models (pre-migration) store entity tables with a sem_ prefix.
            # Check for that before reporting the table as missing.
            if backend.table_exists(f"sem_{table}"):
                raise ValueError(
                    f"Entity table '{table}' not found. This model was materialized with an "
                    f"older format (sem_{table}). Please re-run materialisation to update it."
                )
            raise ValueError(
                f"Entity table '{table}' not found. Run materialisation for this model first."
            )

        # Fetch up to 10 sample IDs
        if entity_id:
            id_rows = backend.execute(
                f"SELECT {pk.name} FROM {table} WHERE {pk.name} = ? LIMIT 1",
                (entity_id,),
            )
        else:
            id_rows = backend.execute(
                f"SELECT {pk.name} FROM {table} LIMIT 10",
                (),
            )
        if not id_rows:
            raise ValueError("No records found")

        sample_ids = [str(r[0]) for r in id_rows]

        # Build evaluation query
        from_clause = f"{table} {alias}"
        join_part = ("\n" + "\n".join(join_clauses)) if join_clauses else ""
        if _has_aggregation(expr_ast):
            group_by_cols = [f"{alias}.{pk.name}"]
            if dialect == "clickhouse":
                from engine.expression_compiler import _collect_non_agg_root_refs
                root_col_names = {a.name for a in entity.all_base_columns}
                for col in sorted(_collect_non_agg_root_refs(expr_ast) & root_col_names):
                    if col != pk.name:
                        group_by_cols.append(f"{alias}.{col}")
            group_by = "\nGROUP BY " + ", ".join(group_by_cols)
        else:
            group_by = ""
        placeholders = ",".join(["?"] * len(sample_ids))
        sql = (
            f"SELECT {alias}.{pk.name} AS entity_id, {value_sql} AS value\n"
            f"FROM {from_clause}{join_part}\n"
            f"WHERE {alias}.{pk.name} IN ({placeholders}){group_by}"
        )

        result_rows = backend.execute(sql, sample_ids)
        results = [
            {"entity_id": str(r[0]), "value": r[1]}
            for r in result_rows
        ]
    finally:
        backend.close()

    return {
        "results": results,
        "sql": sql,
    }


# ---------------------------------------------------------------------------
# Function browser
# ---------------------------------------------------------------------------

_FUNCTION_DOCS = [
    {
        "name": "datetime_diff",
        "signature": "datetime_diff(start, end, unit=\"hours\")",
        "description": "Difference between two datetime fields. unit: hours, days, minutes, seconds, years.",
        "example": "datetime_diff(created_at, merged_at, unit=\"hours\")",
        "valid_modes": ["feature", "measure", "filter"],
        "category": "datetime",
    },
    {
        "name": "format_time",
        "signature": "format_time(col, pattern=\"YYYY-WW\")",
        "description": "Format a datetime as a string. Patterns: YYYY-MM, YYYY-WW, YYYY-wWWW.",
        "example": "format_time(created_at, pattern=\"YYYY-WW\")",
        "valid_modes": ["feature", "measure", "filter"],
        "category": "datetime",
    },
    {
        "name": "format_date",
        "signature": "format_date(col, pattern=\"yyyy-MM-dd\")",
        "description": "Format a date using Java-style tokens (yyyy, MM, dd, MMM, MMMM, etc.).",
        "example": "format_date(closed_at, pattern=\"MMM yyyy\")",
        "valid_modes": ["feature", "measure", "filter"],
        "category": "datetime",
    },
    {
        "name": "concat",
        "signature": "concat(a, b, ...)",
        "description": "Concatenate two or more strings.",
        "example": "concat(first_name, \" \", last_name)",
        "valid_modes": ["feature", "measure", "filter"],
        "category": "string",
    },
    {
        "name": "json_has_key",
        "signature": "json_has_key(json_field, key)",
        "description": "Returns true if a JSON field contains the given key.",
        "example": "json_has_key(metadata, \"priority\")",
        "valid_modes": ["feature", "filter"],
        "category": "json",
    },
    {
        "name": "sum",
        "signature": "sum(expr)",
        "description": "Aggregate: sum of values.",
        "example": "sum(story_points)",
        "valid_modes": ["measure"],
        "category": "aggregate",
    },
    {
        "name": "avg",
        "signature": "avg(expr)",
        "description": "Aggregate: average of values.",
        "example": "avg(cycle_time_hours)",
        "valid_modes": ["measure"],
        "category": "aggregate",
    },
    {
        "name": "count",
        "signature": "count(expr)",
        "description": "Aggregate: count of non-null values.",
        "example": "count(id)",
        "valid_modes": ["measure"],
        "category": "aggregate",
    },
    {
        "name": "count_distinct",
        "signature": "count_distinct(expr)",
        "description": "Aggregate: count of distinct values.",
        "example": "count_distinct(author_key)",
        "valid_modes": ["measure"],
        "category": "aggregate",
    },
    {
        "name": "min",
        "signature": "min(expr)",
        "description": "Aggregate: minimum value.",
        "example": "min(created_at)",
        "valid_modes": ["measure"],
        "category": "aggregate",
    },
    {
        "name": "max",
        "signature": "max(expr)",
        "description": "Aggregate: maximum value.",
        "example": "max(closed_at)",
        "valid_modes": ["measure"],
        "category": "aggregate",
    },
    {
        "name": "coalesce",
        "signature": "coalesce(a, b, ...)",
        "description": "Return the first non-null value.",
        "example": "coalesce(resolved_at, closed_at)",
        "valid_modes": ["feature", "measure", "filter"],
        "category": "logic",
    },
    {
        "name": "case when",
        "signature": "case when <condition> then <value> else <default> end",
        "description": "Conditional expression.",
        "example": "case when status == 'DONE' then 1 else 0 end",
        "valid_modes": ["feature", "measure", "filter"],
        "category": "logic",
    },
]


def list_functions(mode: str | None = None) -> list[dict]:
    """Return function documentation, optionally filtered to a mode."""
    if mode:
        return [f for f in _FUNCTION_DOCS if mode in f["valid_modes"]]
    return _FUNCTION_DOCS
