# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Query runner: unified SQL builder, join resolver, and output formatter for detail and aggregate queries."""

import csv
import io
import json
import re
import sqlite3
from collections import deque
from dataclasses import dataclass

from engine.filter_compiler import collect_filter_column_refs, compile_filter_yaml
from engine.function_registry import render_function
from semantic.expression import BinOp, FieldRef, FuncCallExpr, InExpr, Lit, MethodCallExpr, parse_expression_str
from semantic.feature import FeatureDef
from semantic.query import QueryAttribute, QueryDef, QueryMetric, RatioMetric, SliceDef, WindowDef, evaluate_threshold
from semantic.types import Cardinality, EntityDef

# ---------------------------------------------------------------------------
# Shared internal helpers
# ---------------------------------------------------------------------------

_SNAKE_RE = re.compile(r"(?<=[a-z0-9])([A-Z])")


def _to_snake(name: str) -> str:
    return _SNAKE_RE.sub(r"_\1", name).lower()


def _make_alias(entity_name: str, used: set[str]) -> str:
    """Generate a short unique alias from a PascalCase entity name."""
    snake = _to_snake(entity_name)
    parts = snake.split("_")
    base = "".join(p[0] for p in parts)
    alias = base
    n = 2
    while alias in used:
        alias = f"{base}{n}"
        n += 1
    used.add(alias)
    return alias


def _entity_qualified_alias(
    parts: list[str],
    root_entity: EntityDef,
    entities: dict[str, EntityDef],
) -> str:
    """Return 'EntityName.field' display alias for an attribute parts list.

    - Bare field ['start_date']                         → 'ProductSprint.start_date'
    - Entity-qualified ['ProductSprint', 'start_date']  → 'ProductSprint.start_date'
    - Traversal ['Product', 'display_name']             → 'Product.display_name'
    - Relation-name traversal ['is_authored_by', 'person', 'name'] → 'Person.name'
    """
    if len(parts) == 1:
        return f"{root_entity.name}.{parts[0]}"
    penultimate = parts[-2]
    # If the penultimate part is a relation name (lowercase), resolve it to its target entity name
    if penultimate and penultimate[0].islower():
        current = root_entity
        for part in parts[:-1]:
            rel = next((r for r in current.relations if r.name == part), None)
            if rel and rel.dynamic_field is not None:
                # Dynamic relation — use the relation name directly as the qualifier
                return f"{part}.{parts[-1]}"
            if rel and rel.target in entities:
                current = entities[rel.target]
            elif part in entities:
                current = entities[part]
        penultimate = current.name
    return f"{penultimate}.{parts[-1]}"


@dataclass
class JoinStep:
    from_alias: str
    from_col: str       # FK column on the from side
    target_table: str
    target_alias: str
    target_pk: str
    has_history: bool
    time_ref_col: str | None = None  # point-in-time SCD anchor (e.g. "pr.merged_at")
    extra_condition: str | None = None  # additional ON clause (e.g. for dynamic relations)


def _find_m2o_path(
    from_name: str,
    to_name: str,
    entities: dict[str, "EntityDef"],
) -> list[str] | None:
    """BFS over MANY_TO_ONE relations from from_name to to_name.

    Returns list of entity names (exclusive of start, inclusive of target),
    e.g. ["Product", "Domain"] for ProductVersion->Product->Domain.
    Returns None if no path exists.
    """
    if from_name == to_name:
        return []
    queue: deque[tuple[str, list[str]]] = deque([(from_name, [])])
    visited: set[str] = {from_name}
    while queue:
        cur_name, path = queue.popleft()
        cur_entity = entities.get(cur_name)
        if cur_entity is None:
            continue
        for rel in cur_entity.relations:
            if rel.cardinality != Cardinality.MANY_TO_ONE:
                continue
            if rel.target in visited:
                continue
            visited.add(rel.target)
            new_path = path + [rel.target]
            if rel.target == to_name:
                return new_path
            queue.append((rel.target, new_path))
    return None


# ---------------------------------------------------------------------------
# SCD / join helpers
# ---------------------------------------------------------------------------

def _build_join_clause(step: JoinStep) -> str:
    """Build a LEFT JOIN clause with the correct SCD2 validity condition."""
    cond = f"{step.target_alias}.{step.target_pk} = {step.from_alias}.{step.from_col}"
    if step.extra_condition:
        cond += f" AND {step.extra_condition}"
    if step.has_history:
        if step.time_ref_col:
            cond += (
                f" AND {step.target_alias}.valid_from <= {step.time_ref_col}"
                f" AND ({step.target_alias}.valid_to IS NULL"
                f" OR {step.target_alias}.valid_to = ''"
                f" OR {step.target_alias}.valid_to > {step.time_ref_col})"
            )
        else:
            # valid_to IS NULL covers SQLite (NULL) and ClickHouse String columns ('' empty string).
            cond += f" AND ({step.target_alias}.valid_to IS NULL OR {step.target_alias}.valid_to = '')"
    return f"LEFT JOIN {step.target_table} AS {step.target_alias} ON {cond}"


def _compile_dynamic_slice(
    rel,
    field_name: str,
    from_alias: str,
    entities: dict,
    join_steps_all: list,
    dyn_joined: dict,
    used: set,
) -> tuple[str, str]:
    """Generate dynamic relation JOINs and return (coalesce_col_ref, col_alias).

    col_alias uses the relation name: e.g. 'subject.name'.
    Returns NULL if no entity has the field.
    """
    type_col = f"{from_alias}.{rel.dynamic_field}"
    coalesce_parts: list[str] = []
    for entity_name, entity in sorted(entities.items()):
        entity_cols = {a.name for a in entity.all_base_columns}
        if field_name not in entity_cols:
            continue
        join_key = f"_dyn_{rel.name}_{entity_name}"
        if join_key not in dyn_joined:
            target_pk = rel.via_target or entity.primary_key.name
            dyn_alias = _make_alias(f"d{entity_name}", used)
            step = JoinStep(
                from_alias=from_alias,
                from_col=rel.via,
                target_table=entity.table_name,
                target_alias=dyn_alias,
                target_pk=target_pk,
                has_history=False,
                extra_condition=f"{type_col} = '{entity_name}'",
            )
            join_steps_all.append(step)
            dyn_joined[join_key] = dyn_alias
        coalesce_parts.append(f"{dyn_joined[join_key]}.{field_name}")

    col_alias = f"{rel.name}.{field_name}"
    if not coalesce_parts:
        return "NULL", col_alias
    if len(coalesce_parts) == 1:
        return coalesce_parts[0], col_alias
    return f"COALESCE({', '.join(coalesce_parts)})", col_alias


def _time_ref_from_slices(query: "QueryDef", root_alias: str) -> str | None:
    """Return the raw timestamp column (e.g. 'pr.merged_at') for the first time-sliced
    root-entity field in the query, used as the SCD point-in-time anchor."""
    for s in query.slices:
        if s.format_pattern and len(s.field.split(".")) == 1:
            return f"{root_alias}.{s.field}"
    return None


# ---------------------------------------------------------------------------
# Feature store routing helpers
# ---------------------------------------------------------------------------

_CAST_TYPE: dict[str, dict[str, str]] = {
    "sqlite":     {"STRING": "TEXT",            "FLOAT": "REAL",             "INTEGER": "INTEGER"},
    "clickhouse": {"STRING": "Nullable(String)", "FLOAT": "Nullable(Float64)", "INTEGER": "Nullable(Int64)"},
}


def _feature_col_ref(
    field_name: str,
    entity_name: str,
    root_alias: str,
    features_by_entity: dict[str, dict[str, "FeatureDef"]] | None,
    fv_joins: list[str],
    fv_counter: list[int],
    dialect: str = "sqlite",
) -> str:
    """Return a column expression, using a __feature_value JOIN if applicable."""
    if not features_by_entity:
        return f"{root_alias}.{field_name}"
    feature = features_by_entity.get(entity_name, {}).get(field_name)
    if feature is None:
        return f"{root_alias}.{field_name}"
    fv_alias = f"fv_{fv_counter[0]}"
    fv_counter[0] += 1
    fv_joins.append(
        f"LEFT JOIN __feature_value AS {fv_alias} "
        f"ON {fv_alias}.entity_id = {root_alias}.{feature.entity_key} "
        f"AND {fv_alias}.feature_id = '{feature.feature_id}'"
    )
    type_map = _CAST_TYPE.get(dialect, _CAST_TYPE["sqlite"])
    cast_type = type_map.get(feature.data_type, feature.data_type)
    return f"CAST({fv_alias}.value AS {cast_type})"


# ---------------------------------------------------------------------------
# Filter traversal path collection (used by detail queries)
# ---------------------------------------------------------------------------

def _collect_filter_traversal_paths(
    filters: list[str],
    root_entity_name: str,
) -> set[tuple[str, ...]]:
    """Parse each filter and return entity-name traversal prefixes requiring JOINs."""
    paths: set[tuple[str, ...]] = set()
    for filter_str in filters:
        try:
            node = parse_expression_str(filter_str)
        except Exception:
            continue
        _extract_paths_from_node(node, paths, root_entity_name)
    return paths


def _extract_paths_from_node(
    node, paths: set[tuple[str, ...]], root_entity_name: str
) -> None:
    """Walk an AST node and collect entity-name traversal prefixes."""
    if isinstance(node, BinOp):
        # BinOp covers comparisons (=, !=, <, >, etc.) and boolean ops (AND, OR)
        left = node.left
        if isinstance(left, FieldRef) and len(left.parts) > 1:
            entity_parts = left.parts[:-1]
            if entity_parts[0] != root_entity_name:
                paths.add(tuple(entity_parts))
        # Recurse into both sides for boolean ops
        _extract_paths_from_node(node.left, paths, root_entity_name)
        _extract_paths_from_node(node.right, paths, root_entity_name)
    elif isinstance(node, (InExpr, MethodCallExpr)):
        expr = node.expr
        if isinstance(expr, FieldRef) and len(expr.parts) > 1:
            parts = expr.parts
            if parts[0] != root_entity_name:
                paths.add(tuple(parts[:-1]))


# ---------------------------------------------------------------------------
# Detail query SQL building (was scan_runner)
# ---------------------------------------------------------------------------

def _compile_query_attr_expr(
    expr_str: str,
    root_alias: str,
    root_entity: EntityDef,
    features_by_entity: dict | None,
    fv_joins: list[str],
    fv_counter: list[int],
    dialect: str = "sqlite",
) -> str:
    """Compile a query attribute expression (e.g. format_date(field, 'YYYY-MM-DD')) to SQL.

    Field references are resolved via _feature_col_ref so that feature attributes
    correctly read from the __feature_value JOIN.
    """
    expr = parse_expression_str(expr_str)

    if isinstance(expr, FuncCallExpr) and expr.name == "format_date":
        if len(expr.args) != 2:
            raise ValueError(
                f"format_date requires 2 arguments (field, format), got {len(expr.args)}"
            )
        field_arg, fmt_arg = expr.args
        if not isinstance(field_arg, FieldRef):
            raise ValueError("format_date first argument must be a field reference")
        if not isinstance(fmt_arg, Lit) or not isinstance(fmt_arg.value, str):
            raise ValueError("format_date second argument must be a string literal")
        field_name = ".".join(field_arg.parts)
        col_ref = _feature_col_ref(
            field_name, root_entity.name, root_alias,
            features_by_entity, fv_joins, fv_counter, dialect=dialect,
        )
        return render_function("format_date", [col_ref], {"pattern": fmt_arg.value}, dialect=dialect)

    raise ValueError(f"Unsupported attribute expression: {expr_str!r}")


def build_detail_query(
    query: QueryDef,
    entities: dict[str, EntityDef],
    page: int,
    page_size: int,
    features_by_entity: dict | None = None,
    dialect: str = "sqlite",
) -> str:
    """Build the paginated SQL query for a detail (non-aggregate) query."""
    root_entity = entities[query.entity]
    used: set[str] = set()
    root_alias = _make_alias(root_entity.name, used)

    rel_path_to_alias: dict[str, str] = {}
    join_steps_all: list[JoinStep] = []
    fv_joins: list[str] = []
    fv_counter: list[int] = [0]

    def _ensure_join(entity_path: tuple[str, ...]) -> None:
        current_entity = root_entity
        current_alias = root_alias

        for i, entity_name in enumerate(entity_path):
            prefix_key = ".".join(entity_path[: i + 1])
            if prefix_key in rel_path_to_alias:
                current_entity = entities[entity_name]
                current_alias = rel_path_to_alias[prefix_key]
                continue

            rel = next(
                (r for r in current_entity.relations
                 if r.cardinality == Cardinality.MANY_TO_ONE and r.target == entity_name),
                None,
            )
            if rel is None:
                bfs_path = _find_m2o_path(current_entity.name, entity_name, entities)
                if bfs_path is None:
                    raise ValueError(
                        f"No MANY_TO_ONE relation path from '{current_entity.name}' to entity '{entity_name}'"
                    )
                hop_entity = current_entity
                hop_alias = current_alias
                for hop_name in bfs_path:
                    hop_rel = next(
                        r for r in hop_entity.relations
                        if r.cardinality == Cardinality.MANY_TO_ONE and r.target == hop_name
                    )
                    intermediate_key = hop_name
                    if intermediate_key not in rel_path_to_alias:
                        next_entity = entities[hop_name]
                        tgt_alias = _make_alias(hop_name, used)
                        step = JoinStep(
                            from_alias=hop_alias,
                            from_col=hop_rel.via,
                            target_table=next_entity.table_name,
                            target_alias=tgt_alias,
                            target_pk=next_entity.primary_key.name,
                            has_history=next_entity.history is not None,
                        )
                        join_steps_all.append(step)
                        rel_path_to_alias[intermediate_key] = tgt_alias
                    hop_alias = rel_path_to_alias[intermediate_key]
                    hop_entity = entities[hop_name]
                rel_path_to_alias[prefix_key] = rel_path_to_alias[entity_name]
                current_entity = entities[entity_name]
                current_alias = rel_path_to_alias[prefix_key]
                continue

            target_entity = entities[entity_name]
            tgt_alias = _make_alias(entity_name, used)
            step = JoinStep(
                from_alias=current_alias,
                from_col=rel.via,
                target_table=target_entity.table_name,
                target_alias=tgt_alias,
                target_pk=target_entity.primary_key.name,
                has_history=target_entity.history is not None,
            )
            join_steps_all.append(step)
            rel_path_to_alias[prefix_key] = tgt_alias
            current_entity = target_entity
            current_alias = tgt_alias

    for attr in query.attributes:
        if len(attr.parts) > 1 and attr.parts[0] != root_entity.name:
            _ensure_join(tuple(attr.parts[:-1]))

    filter_paths = _collect_filter_traversal_paths(query.filters, root_entity.name)
    for path_tuple in filter_paths:
        _ensure_join(path_tuple)

    def path_resolver(parts: list[str]) -> str:
        if len(parts) == 1:
            return f"{root_alias}.{parts[0]}"
        if parts[0] == root_entity.name:
            return f"{root_alias}.{parts[-1]}"
        prefix = ".".join(parts[:-1])
        tgt_alias = rel_path_to_alias.get(prefix, root_alias)
        return f"COALESCE({tgt_alias}.{parts[-1]}, '(unset)')"

    select_parts: list[str] = []
    for attr in query.attributes:
        if attr.expr_str:
            col_ref = _compile_query_attr_expr(
                attr.expr_str, root_alias, root_entity,
                features_by_entity, fv_joins, fv_counter, dialect=dialect,
            )
            display = f"{root_entity.name}.{attr.label or attr.parts[-1]}"
            select_parts.append(f'{col_ref} AS "{display}"')
            continue
        display = attr.label or _entity_qualified_alias(attr.parts, root_entity, entities)
        if len(attr.parts) > 1 and attr.parts[0] != root_entity.name:
            prefix = ".".join(attr.parts[:-1])
            tgt_alias = rel_path_to_alias.get(prefix, root_alias)
            col_ref = f"COALESCE({tgt_alias}.{attr.parts[-1]}, '(unset)')"
        else:
            field_name = attr.parts[-1]
            col_ref = _feature_col_ref(
                field_name, root_entity.name, root_alias,
                features_by_entity, fv_joins, fv_counter, dialect=dialect,
            )
        select_parts.append(f'{col_ref} AS "{display}"')

    from_clause = f"FROM {root_entity.table_name} AS {root_alias}"

    filter_field_map: dict[str, str] = {}
    if features_by_entity and query.filters:
        for filter_str in query.filters:
            for field_name in collect_filter_column_refs(filter_str):
                if "." not in field_name and field_name not in filter_field_map:
                    expr = _feature_col_ref(
                        field_name, root_entity.name, root_alias,
                        features_by_entity, fv_joins, fv_counter, dialect=dialect,
                    )
                    if expr != f"{root_alias}.{field_name}":
                        filter_field_map[field_name] = expr

    join_clauses: list[str] = [_build_join_clause(step) for step in join_steps_all]
    join_clauses.extend(fv_joins)

    filter_parts = []
    if root_entity.history:
        filter_parts.append(f"({root_alias}.valid_to IS NULL OR {root_alias}.valid_to = '')")
    if query.filters:
        for filter_str in query.filters:
            sql = compile_filter_yaml(
                filter_str,
                alias=root_alias,
                path_resolver=path_resolver,
                field_override_map=filter_field_map,
                dialect=dialect,
            )
            filter_parts.append(sql)
    where_clause = ("WHERE " + "\n  AND ".join(filter_parts)) if filter_parts else ""

    order_by_clause = ""

    offset = (page - 1) * page_size
    limit_clause = f"LIMIT {page_size} OFFSET {offset}"

    if not select_parts:
        raise ValueError(
            f"Detail query '{query.name}' has no attributes defined. "
            "All queries must explicitly list their output fields."
        )

    lines = [
        "SELECT",
        "  " + ",\n  ".join(select_parts),
        from_clause,
    ]
    lines.extend(join_clauses)
    if where_clause:
        lines.append(where_clause)
    if order_by_clause:
        lines.append(order_by_clause)
    lines.append(limit_clause)

    return "\n".join(lines)


def _build_count_query(
    query: QueryDef,
    entities: dict[str, EntityDef],
    features_by_entity: dict | None = None,
    dialect: str = "sqlite",
) -> str:
    """Build a COUNT(*) query with the same FROM/JOIN/WHERE as a detail query."""
    root_entity = entities[query.entity]
    used: set[str] = set()
    root_alias = _make_alias(root_entity.name, used)

    rel_path_to_alias: dict[str, str] = {}
    join_steps_all: list[JoinStep] = []
    fv_joins: list[str] = []
    fv_counter: list[int] = [0]

    def _ensure_join(entity_path: tuple[str, ...]) -> None:
        current_entity = root_entity
        current_alias = root_alias
        for i, entity_name in enumerate(entity_path):
            prefix_key = ".".join(entity_path[: i + 1])
            if prefix_key in rel_path_to_alias:
                current_entity = entities[entity_name]
                current_alias = rel_path_to_alias[prefix_key]
                continue
            rel = next(
                (r for r in current_entity.relations
                 if r.cardinality == Cardinality.MANY_TO_ONE and r.target == entity_name),
                None,
            )
            if rel is None:
                bfs_path = _find_m2o_path(current_entity.name, entity_name, entities)
                if bfs_path is None:
                    raise ValueError(
                        f"No MANY_TO_ONE relation path from '{current_entity.name}' to entity '{entity_name}'"
                    )
                hop_entity = current_entity
                hop_alias = current_alias
                for hop_name in bfs_path:
                    hop_rel = next(
                        r for r in hop_entity.relations
                        if r.cardinality == Cardinality.MANY_TO_ONE and r.target == hop_name
                    )
                    intermediate_key = hop_name
                    if intermediate_key not in rel_path_to_alias:
                        next_entity = entities[hop_name]
                        tgt_alias = _make_alias(hop_name, used)
                        step = JoinStep(
                            from_alias=hop_alias,
                            from_col=hop_rel.via,
                            target_table=next_entity.table_name,
                            target_alias=tgt_alias,
                            target_pk=next_entity.primary_key.name,
                            has_history=next_entity.history is not None,
                        )
                        join_steps_all.append(step)
                        rel_path_to_alias[intermediate_key] = tgt_alias
                    hop_alias = rel_path_to_alias[intermediate_key]
                    hop_entity = entities[hop_name]
                rel_path_to_alias[prefix_key] = rel_path_to_alias[entity_name]
                current_entity = entities[entity_name]
                current_alias = rel_path_to_alias[prefix_key]
                continue
            target_entity = entities[entity_name]
            tgt_alias = _make_alias(entity_name, used)
            step = JoinStep(
                from_alias=current_alias,
                from_col=rel.via,
                target_table=target_entity.table_name,
                target_alias=tgt_alias,
                target_pk=target_entity.primary_key.name,
                has_history=target_entity.history is not None,
            )
            join_steps_all.append(step)
            rel_path_to_alias[prefix_key] = tgt_alias
            current_entity = target_entity
            current_alias = tgt_alias

    for attr in query.attributes:
        if len(attr.parts) > 1 and attr.parts[0] != root_entity.name:
            _ensure_join(tuple(attr.parts[:-1]))
    filter_paths = _collect_filter_traversal_paths(query.filters, root_entity.name)
    for path_tuple in filter_paths:
        _ensure_join(path_tuple)

    def path_resolver(parts: list[str]) -> str:
        if len(parts) == 1:
            return f"{root_alias}.{parts[0]}"
        if parts[0] == root_entity.name:
            return f"{root_alias}.{parts[-1]}"
        prefix = ".".join(parts[:-1])
        tgt_alias = rel_path_to_alias.get(prefix, root_alias)
        return f"COALESCE({tgt_alias}.{parts[-1]}, '(unset)')"

    from_clause = f"FROM {root_entity.table_name} AS {root_alias}"

    filter_field_map: dict[str, str] = {}
    if features_by_entity and query.filters:
        for filter_str in query.filters:
            for field_name in collect_filter_column_refs(filter_str):
                if "." not in field_name and field_name not in filter_field_map:
                    expr = _feature_col_ref(
                        field_name, root_entity.name, root_alias,
                        features_by_entity, fv_joins, fv_counter, dialect=dialect,
                    )
                    if expr != f"{root_alias}.{field_name}":
                        filter_field_map[field_name] = expr

    join_clauses: list[str] = [_build_join_clause(step) for step in join_steps_all]
    join_clauses.extend(fv_joins)

    filter_parts = []
    if root_entity.history:
        filter_parts.append(f"({root_alias}.valid_to IS NULL OR {root_alias}.valid_to = '')")
    if query.filters:
        for filter_str in query.filters:
            sql = compile_filter_yaml(
                filter_str,
                alias=root_alias,
                path_resolver=path_resolver,
                field_override_map=filter_field_map,
                dialect=dialect,
            )
            filter_parts.append(sql)
    where_clause = ("WHERE " + "\n  AND ".join(filter_parts)) if filter_parts else ""

    lines = ["SELECT COUNT(*)", from_clause]
    lines.extend(join_clauses)
    if where_clause:
        lines.append(where_clause)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Aggregate query SQL building (was pivot_runner)
# ---------------------------------------------------------------------------

def _slice_expr(slice_def: "SliceDef", col_ref: str, dialect: str = "sqlite") -> str | None:
    """Return a SQL expression for a time-sliced column, or None if no time spec."""
    if slice_def.format_pattern:
        return render_function("format_time", [col_ref], {"pattern": slice_def.format_pattern}, dialect=dialect)
    return None


_ROLLUP_FN = {
    "sum": "SUM",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
    "count": "COUNT",
    "count_distinct": "COUNT",
}


def _build_agg(rollup: str, field_ref: str) -> str:
    """Return SQL aggregate expression for rollup + field."""
    if rollup == "count_distinct":
        return f"COUNT(DISTINCT {field_ref})"
    return f"{_ROLLUP_FN[rollup]}({field_ref})"


def _build_child_entity_agg(
    metric_def,
    root_entity: "EntityDef",
    entities: dict,
    root_alias: str,
    used: set,
    child_agg_joins: list,
    rollup_map: dict,
) -> "str | None":
    """If metric_def.field is 'ChildEntityName.col' with a ONE_TO_MANY relation from root,
    build a pre-aggregated subquery LEFT JOIN and return SUM(COALESCE(...)) as the metric.
    Returns None if the field is not a child entity aggregation.
    """
    parts = metric_def.field.split(".")
    if len(parts) != 2 or not parts[0][0].isupper() or parts[0] not in entities:
        return None
    entity_name, col_name = parts
    rel = next(
        (r for r in root_entity.relations
         if r.target == entity_name and r.cardinality == Cardinality.ONE_TO_MANY),
        None,
    )
    if rel is None:
        return None
    child_entity = entities[entity_name]
    # Build (parent_col, child_col) pairs.
    # New string form:  via="iata_code", via_target="airline_code"  → explicit columns
    # Legacy array form: via=["airline_code"]                       → child FK, parent is PK
    if isinstance(rel.via, list):
        join_pairs = [(col, col) for col in rel.via]
    else:
        join_pairs = [(rel.via, rel.via_target or root_entity.primary_key.name)]
    child_cols = [tc for _, tc in join_pairs]
    sub_alias = _make_alias(_to_snake(entity_name) + "_sub", used)
    cnt_col = "_cnt"
    fn = metric_def.rollup
    inner_agg = (
        f"COUNT(DISTINCT {col_name})" if fn == "count_distinct"
        else f"{rollup_map.get(fn, 'COUNT')}({col_name})"
    )
    via_csv = ", ".join(child_cols)
    join_cond = " AND ".join(f"{sub_alias}.{tc} = {root_alias}.{sc}" for sc, tc in join_pairs)
    child_agg_joins.append(
        f"LEFT JOIN (\n"
        f"  SELECT {via_csv}, {inner_agg} AS {cnt_col}\n"
        f"  FROM {child_entity.table_name}\n"
        f"  GROUP BY {via_csv}\n"
        f") AS {sub_alias} ON {join_cond}"
    )
    return f"SUM(COALESCE({sub_alias}.{cnt_col}, 0))"


def _slice_col_alias(
    slice_def: SliceDef,
    root_entity: EntityDef,
    entities: dict[str, EntityDef],
) -> str:
    """Return 'EntityName.field' display alias for a slice."""
    parts = slice_def.field.split(".")
    if len(parts) == 1:
        return f"{root_entity.name}.{parts[0]}"
    return _entity_qualified_alias(parts, root_entity, entities)


def _resolve_attribute(
    attr: QueryAttribute,
    root_entity: EntityDef,
    root_alias: str,
    entities: dict[str, EntityDef],
    joined_entities: dict[str, str],
    join_steps_all: list[JoinStep],
    used: set[str],
    time_ref_col: str | None = None,
) -> tuple[str, str]:
    """Resolve a QueryAttribute to (col_ref, col_alias), adding JOIN steps as needed."""
    if len(attr.parts) == 1 or attr.parts[0] == root_entity.name:
        col_ref = f"{root_alias}.{attr.parts[-1]}"
        col_alias = f"{root_entity.name}.{attr.parts[-1]}"
        return col_ref, col_alias

    current_entity = root_entity
    current_alias = root_alias
    for entity_name in attr.parts[:-1]:
        rel = next(
            (r for r in current_entity.relations
             if r.target == entity_name and r.cardinality == Cardinality.MANY_TO_ONE),
            None,
        )
        if rel is None:
            raise ValueError(
                f"No MANY_TO_ONE relation from '{current_entity.name}' to entity '{entity_name}'"
            )
        target_entity = entities[entity_name]
        if entity_name not in joined_entities:
            tgt_alias = _make_alias(entity_name, used)
            step = JoinStep(
                from_alias=current_alias,
                from_col=rel.via,
                target_table=target_entity.table_name,
                target_alias=tgt_alias,
                target_pk=target_entity.primary_key.name,
                has_history=target_entity.history is not None,
                time_ref_col=time_ref_col if target_entity.history is not None else None,
            )
            join_steps_all.append(step)
            joined_entities[entity_name] = tgt_alias
        current_alias = joined_entities[entity_name]
        current_entity = target_entity

    col_ref = f"{current_alias}.{attr.parts[-1]}"
    col_alias = f"{current_entity.name}.{attr.parts[-1]}"
    return col_ref, col_alias


def _attr_col_alias(attr: QueryAttribute, root_entity: EntityDef) -> str:
    """Return 'EntityName.field' display alias for a query attribute."""
    if len(attr.parts) == 1:
        return f"{root_entity.name}.{attr.parts[0]}"
    return f"{attr.parts[-2]}.{attr.parts[-1]}"


def _window_frame_sql(window: WindowDef) -> str:
    """Return the ROWS frame clause for a window definition."""
    if window.method == "cumulative":
        return "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
    if window.method in ("lag", "lead"):
        return ""
    return f"ROWS BETWEEN {window.frame - 1} PRECEDING AND CURRENT ROW"


def _window_over_clause(
    window: WindowDef,
    slices: list[SliceDef],
    slice_col_aliases: dict[str, str],
) -> str:
    """Build the OVER (...) clause for a window definition."""
    if window.unit == "rows":
        order_slice = slices[-1]
        partition_slices = slices[:-1]
    else:
        order_slice = next(s for s in slices if s.inferred_grain == window.unit)
        partition_slices = [s for s in slices if s is not order_slice]

    order_key = order_slice.alias or order_slice.field
    order_alias = f'"{slice_col_aliases[order_key]}"'
    partition_aliases = [f'"{slice_col_aliases[s.alias or s.field]}"' for s in partition_slices]

    frame_sql = _window_frame_sql(window)
    parts = []
    if partition_aliases:
        parts.append("PARTITION BY " + ", ".join(partition_aliases))
    parts.append("ORDER BY " + order_alias)
    if frame_sql:
        parts.append(frame_sql)
    return "OVER (" + " ".join(parts) + ")"


def _build_windowed_aggregate_query(
    query: QueryDef,
    entities: dict[str, EntityDef],
    features_by_entity: dict | None = None,
    dialect: str = "sqlite",
) -> str:
    """Build a CTE-based windowed aggregate query."""
    root_entity = entities[query.entity]
    used: set[str] = set()
    root_alias = _make_alias(root_entity.name, used)

    join_steps_all: list[JoinStep] = []
    joined_entities: dict[str, str] = {}
    fv_joins: list[str] = []
    fv_counter: list[int] = [0]
    time_ref_col = _time_ref_from_slices(query, root_alias)

    base_select_parts: list[str] = []
    group_by_parts: list[str] = []
    slice_col_aliases: dict[str, str] = {}

    for slice_def in query.slices:
        parts = slice_def.field.split(".")
        if len(parts) == 1:
            bare_field = parts[0]
            col_ref = _feature_col_ref(
                bare_field, root_entity.name, root_alias,
                features_by_entity, fv_joins, fv_counter, dialect=dialect,
            )
            col_alias = slice_def.alias or f"{root_entity.name}.{bare_field}"

            time_expr = _slice_expr(slice_def, col_ref, dialect=dialect)
            if time_expr is not None:
                base_select_parts.append(f'{time_expr} AS "{col_alias}"')
                group_by_parts.append(time_expr)
            else:
                base_select_parts.append(f'{col_ref} AS "{col_alias}"')
                group_by_parts.append(col_ref)

            slice_col_aliases[slice_def.alias or slice_def.field] = col_alias

        else:
            current_entity = root_entity
            current_alias = root_alias
            traversal_alias = root_alias
            dyn_joined: dict[str, str] = {}
            dynamic_result: tuple[str, str] | None = None

            for part in parts[:-1]:
                if part[0].isupper():
                    rel = next(
                        (r for r in current_entity.relations
                         if r.target == part and r.cardinality == Cardinality.MANY_TO_ONE),
                        None,
                    )
                    if rel is None:
                        raise ValueError(
                            f"No MANY_TO_ONE relation from '{current_entity.name}' to entity '{part}'"
                        )
                    target_entity = entities[part]
                else:
                    rel = next(
                        (r for r in current_entity.relations if r.name == part),
                        None,
                    )
                    if rel is None:
                        raise ValueError(
                            f"Relation '{part}' not found on entity '{current_entity.name}'"
                        )
                    if rel.dynamic_field is not None:
                        dynamic_result = _compile_dynamic_slice(
                            rel, parts[-1], current_alias, entities,
                            join_steps_all, dyn_joined, used,
                        )
                        break
                    target_entity = entities[rel.target]

                if rel.target not in joined_entities:
                    tgt_alias = _make_alias(rel.target, used)
                    step = JoinStep(
                        from_alias=current_alias,
                        from_col=rel.via,
                        target_table=target_entity.table_name,
                        target_alias=tgt_alias,
                        target_pk=target_entity.primary_key.name,
                        has_history=target_entity.history is not None,
                        time_ref_col=time_ref_col if target_entity.history is not None else None,
                    )
                    join_steps_all.append(step)
                    joined_entities[rel.target] = tgt_alias

                traversal_alias = joined_entities[rel.target]
                current_entity = target_entity
                current_alias = traversal_alias

            if dynamic_result is not None:
                col_ref, col_alias = dynamic_result
                # For dynamic slices, alias overrides the coalesce alias if provided
                if slice_def.alias:
                    col_alias = slice_def.alias
            else:
                final_field = parts[-1]
                col_ref = f"COALESCE({traversal_alias}.{final_field}, '(unset)')"
                col_alias = slice_def.alias or f"{current_entity.name}.{final_field}"

            base_select_parts.append(f'{col_ref} AS "{col_alias}"')
            group_by_parts.append(col_ref)
            slice_col_aliases[slice_def.alias or slice_def.field] = col_alias

    attr_col_aliases: list[str] = []
    for attr in query.attributes:
        if len(attr.parts) == 1 or (len(attr.parts) == 2 and attr.parts[0] == root_entity.name):
            bare = attr.parts[-1]
            col_ref = _feature_col_ref(
                bare, root_entity.name, root_alias,
                features_by_entity, fv_joins, fv_counter, dialect=dialect,
            )
            col_alias = f"{root_entity.name}.{bare}"
        else:
            col_ref, col_alias = _resolve_attribute(
                attr, root_entity, root_alias, entities, joined_entities, join_steps_all, used,
                time_ref_col=time_ref_col,
            )
        if col_alias in slice_col_aliases.values():
            col_alias += "_raw"
        attr_col_aliases.append(col_alias)
        base_select_parts.append(f'{col_ref} AS "{col_alias}"')
        group_by_parts.append(col_ref)

    rollup_map = {
        "sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX", "count": "COUNT",
    }

    for metric_name, metric_def in query.metrics.items():
        if isinstance(metric_def, RatioMetric):
            num_ref = _feature_col_ref(
                metric_def.numerator.field, root_entity.name, root_alias,
                features_by_entity, fv_joins, fv_counter, dialect=dialect,
            )
            den_ref = _feature_col_ref(
                metric_def.denominator.field, root_entity.name, root_alias,
                features_by_entity, fv_joins, fv_counter, dialect=dialect,
            )
            base_select_parts.append(
                f"CAST({_build_agg(metric_def.numerator.rollup, num_ref)} AS REAL)"
                f" AS __{metric_name}_num"
            )
            base_select_parts.append(
                f"{_build_agg(metric_def.denominator.rollup, den_ref)}"
                f" AS __{metric_name}_den"
            )
            continue
        field_ref = _feature_col_ref(
            metric_def.field, root_entity.name, root_alias,
            features_by_entity, fv_joins, fv_counter, dialect=dialect,
        )
        if metric_def.window is None:
            fn = metric_def.rollup
            if fn == "count_distinct":
                agg = f"COUNT(DISTINCT {field_ref})"
            else:
                agg = f"{rollup_map[fn]}({field_ref})"
            base_select_parts.append(f"{agg} AS {metric_name}")
        elif metric_def.window.method == "weighted":
            base_select_parts.append(f"SUM({field_ref}) AS __{metric_name}_sum")
            base_select_parts.append(f"COUNT({field_ref}) AS __{metric_name}_count")
        else:
            fn = metric_def.rollup
            if fn == "count_distinct":
                agg = f"COUNT(DISTINCT {field_ref})"
            else:
                agg = f"{rollup_map[fn]}({field_ref})"
            base_select_parts.append(f"{agg} AS __{metric_name}")

    from_clause = f"FROM {root_entity.table_name} AS {root_alias}"

    filter_field_map: dict[str, str] = {}
    if features_by_entity and query.filters:
        for filter_str in query.filters:
            for field_name in collect_filter_column_refs(filter_str):
                if "." not in field_name and field_name not in filter_field_map:
                    expr = _feature_col_ref(
                        field_name, root_entity.name, root_alias,
                        features_by_entity, fv_joins, fv_counter, dialect=dialect,
                    )
                    if expr != f"{root_alias}.{field_name}":
                        filter_field_map[field_name] = expr

    # Ensure JOINs exist for entities referenced in filter traversals but not yet joined
    for path_tuple in _collect_filter_traversal_paths(query.filters, root_entity.name):
        current_ent = root_entity
        current_al = root_alias
        for hop_name in path_tuple:
            if hop_name in joined_entities:
                current_ent = entities[hop_name]
                current_al = joined_entities[hop_name]
                continue
            rel = next(
                (r for r in current_ent.relations
                 if r.cardinality == Cardinality.MANY_TO_ONE and r.target == hop_name),
                None,
            )
            if rel is None:
                break
            target_ent = entities[hop_name]
            tgt_alias = _make_alias(hop_name, used)
            join_steps_all.append(JoinStep(
                from_alias=current_al, from_col=rel.via,
                target_table=target_ent.table_name, target_alias=tgt_alias,
                target_pk=target_ent.primary_key.name,
                has_history=target_ent.history is not None,
                time_ref_col=time_ref_col if target_ent.history is not None else None,
            ))
            joined_entities[hop_name] = tgt_alias
            current_ent = target_ent
            current_al = tgt_alias

    def filter_path_resolver(parts: list[str]) -> str:
        if len(parts) == 1 or parts[0] == root_entity.name:
            return f"{root_alias}.{parts[-1]}"
        current_al = root_alias
        for part in parts[:-1]:
            if part != root_entity.name and part in joined_entities:
                current_al = joined_entities[part]
        return f"COALESCE({current_al}.{parts[-1]}, '(unset)')"

    join_clauses = [_build_join_clause(step) for step in join_steps_all]
    join_clauses.extend(fv_joins)

    where_clause = ""
    if query.filters:
        filter_parts = []
        for filter_str in query.filters:
            sql = compile_filter_yaml(
                filter_str, alias=root_alias,
                path_resolver=filter_path_resolver,
                field_override_map=filter_field_map,
                dialect=dialect,
            )
            filter_parts.append(sql)
        where_clause = "WHERE " + "\n  AND ".join(filter_parts)

    group_by_clause = ""
    if group_by_parts:
        group_by_clause = "GROUP BY " + ", ".join(group_by_parts)

    base_lines = ["SELECT", "  " + ",\n  ".join(base_select_parts), from_clause]
    base_lines.extend(join_clauses)
    if where_clause:
        base_lines.append(where_clause)
    if group_by_clause:
        base_lines.append(group_by_clause)

    indented_base = "\n".join("  " + line for line in base_lines)
    base_cte = f"WITH base AS (\n{indented_base}\n)"

    outer_select_parts: list[str] = []

    for slice_def in query.slices:
        col_alias = slice_col_aliases[slice_def.alias or slice_def.field]
        outer_select_parts.append(f'"{col_alias}"')

    for col_alias in attr_col_aliases:
        outer_select_parts.append(f'"{col_alias}"')

    outer_rollup_fn = {
        "sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX",
        "count": "COUNT", "count_distinct": "COUNT",
    }

    for metric_name, metric_def in query.metrics.items():
        if isinstance(metric_def, RatioMetric):
            outer_select_parts.append(f"__{metric_name}_num / __{metric_name}_den AS {metric_name}")
            continue
        if metric_def.window is None:
            outer_select_parts.append(metric_name)
        else:
            window = metric_def.window
            over_clause = _window_over_clause(window, query.slices, slice_col_aliases)

            if window.method == "weighted":
                outer_select_parts.append(
                    f"SUM(__{metric_name}_sum) {over_clause} / "
                    f"SUM(__{metric_name}_count) {over_clause} AS {metric_name}"
                )
            elif window.method in ("lag", "lead"):
                fn = window.method.upper()
                outer_select_parts.append(
                    f"{fn}(__{metric_name}, {window.offset}) {over_clause} AS {metric_name}"
                )
            else:
                fn = outer_rollup_fn[metric_def.rollup]
                outer_select_parts.append(
                    f"{fn}(__{metric_name}) {over_clause} AS {metric_name}"
                )

    order_by_clause = ""
    if query.order_by:
        # New top-level order: block — use directly; col names are aliases as written
        order_parts = [f'"{col}" {direction.upper()}' for col, direction in query.order_by]
        order_by_clause = "ORDER BY " + ", ".join(order_parts)
    else:
        # Legacy: per-attribute order_position (deprecated, kept for backward compat)
        order_items = []
        for s in query.slices:
            if s.order_position is not None:
                key = s.alias or s.field
                col = slice_col_aliases.get(key, key)
                order_items.append((s.order_position, s.order_direction, col))
        for metric_alias, m in query.metrics.items():
            if m.order_position is not None:
                order_items.append((m.order_position, m.order_direction, metric_alias))
        order_items.sort(key=lambda x: x[0])
        if order_items:
            order_parts = [f'"{col}" {direction.upper()}' for _, direction, col in order_items]
            order_by_clause = "ORDER BY " + ", ".join(order_parts)

    outer_lines = [
        base_cte,
        "SELECT",
        "  " + ",\n  ".join(outer_select_parts),
        "FROM base",
    ]
    if order_by_clause:
        outer_lines.append(order_by_clause)

    return "\n".join(outer_lines)


def build_aggregate_query(
    query: QueryDef,
    entities: dict[str, EntityDef],
    features_by_entity: dict | None = None,
    dialect: str = "sqlite",
) -> str:
    """Build the SQL query for an aggregate (pivot-style) query."""
    has_windows = any(
        isinstance(m, QueryMetric) and m.window is not None
        for m in query.metrics.values()
    )
    if has_windows:
        return _build_windowed_aggregate_query(query, entities, features_by_entity, dialect=dialect)

    root_entity = entities[query.entity]
    used: set[str] = set()
    root_alias = _make_alias(root_entity.name, used)

    join_steps_all: list[JoinStep] = []
    joined_entities: dict[str, str] = {}
    fv_joins: list[str] = []
    fv_counter: list[int] = [0]
    time_ref_col = _time_ref_from_slices(query, root_alias)

    select_parts: list[str] = []
    group_by_parts: list[str] = []
    slice_col_aliases: dict[str, str] = {}
    attr_col_aliases: dict[str, str] = {}  # bare/qualified field name → SELECT alias

    for slice_def in query.slices:
        parts = slice_def.field.split(".")
        if len(parts) == 1:
            bare_field = parts[0]
            col_ref = _feature_col_ref(
                bare_field, root_entity.name, root_alias,
                features_by_entity, fv_joins, fv_counter, dialect=dialect,
            )
            col_alias = slice_def.alias or f"{root_entity.name}.{bare_field}"

            time_expr = _slice_expr(slice_def, col_ref, dialect=dialect)
            if time_expr is not None:
                select_parts.append(f'{time_expr} AS "{col_alias}"')
                group_by_parts.append(time_expr)
            else:
                select_parts.append(f'{col_ref} AS "{col_alias}"')
                group_by_parts.append(col_ref)

            slice_col_aliases[slice_def.alias or slice_def.field] = col_alias

        else:
            current_entity = root_entity
            current_alias = root_alias
            traversal_alias = root_alias
            dyn_joined: dict[str, str] = {}
            dynamic_result: tuple[str, str] | None = None

            for part in parts[:-1]:
                if part[0].isupper():
                    rel = next(
                        (r for r in current_entity.relations
                         if r.target == part and r.cardinality == Cardinality.MANY_TO_ONE),
                        None,
                    )
                    if rel is None:
                        raise ValueError(
                            f"No MANY_TO_ONE relation from '{current_entity.name}' to entity '{part}'"
                        )
                    target_entity = entities[part]
                else:
                    rel = next(
                        (r for r in current_entity.relations if r.name == part),
                        None,
                    )
                    if rel is None:
                        raise ValueError(
                            f"Relation '{part}' not found on entity '{current_entity.name}'"
                        )
                    if rel.dynamic_field is not None:
                        # Dynamic polymorphic relation — generate COALESCE over all entities
                        dynamic_result = _compile_dynamic_slice(
                            rel, parts[-1], current_alias, entities,
                            join_steps_all, dyn_joined, used,
                        )
                        break
                    target_entity = entities[rel.target]

                if rel.target not in joined_entities:
                    tgt_alias = _make_alias(rel.target, used)
                    step = JoinStep(
                        from_alias=current_alias,
                        from_col=rel.via,
                        target_table=target_entity.table_name,
                        target_alias=tgt_alias,
                        target_pk=target_entity.primary_key.name,
                        has_history=target_entity.history is not None,
                        time_ref_col=time_ref_col if target_entity.history is not None else None,
                    )
                    join_steps_all.append(step)
                    joined_entities[rel.target] = tgt_alias

                traversal_alias = joined_entities[rel.target]
                current_entity = target_entity
                current_alias = traversal_alias

            if dynamic_result is not None:
                col_ref, col_alias = dynamic_result
                # For dynamic slices, alias overrides the coalesce alias if provided
                if slice_def.alias:
                    col_alias = slice_def.alias
            else:
                final_field = parts[-1]
                col_ref = f"COALESCE({traversal_alias}.{final_field}, '(unset)')"
                col_alias = slice_def.alias or f"{current_entity.name}.{final_field}"

            select_parts.append(f'{col_ref} AS "{col_alias}"')
            group_by_parts.append(col_ref)
            slice_col_aliases[slice_def.alias or slice_def.field] = col_alias

    for attr in query.attributes:
        if len(attr.parts) == 1 or (len(attr.parts) == 2 and attr.parts[0] == root_entity.name):
            bare = attr.parts[-1]
            col_ref = _feature_col_ref(
                bare, root_entity.name, root_alias,
                features_by_entity, fv_joins, fv_counter, dialect=dialect,
            )
            col_alias = f"{root_entity.name}.{bare}"
        else:
            col_ref, col_alias = _resolve_attribute(
                attr, root_entity, root_alias, entities, joined_entities, join_steps_all, used,
                time_ref_col=time_ref_col,
            )
        if col_alias in slice_col_aliases.values():
            col_alias += "_raw"
        select_parts.append(f'{col_ref} AS "{col_alias}"')
        group_by_parts.append(col_ref)
        # Register for ORDER BY resolution: bare name and qualified name both map to col_alias
        attr_col_aliases[attr.parts[-1]] = col_alias
        attr_col_aliases[".".join(attr.parts)] = col_alias

    rollup_map = {
        "sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX",
        "count": "COUNT", "count_distinct": "COUNT(DISTINCT",
    }

    child_agg_joins: list[str] = []

    for metric_name, metric_def in query.metrics.items():
        if isinstance(metric_def, RatioMetric):
            num_ref = _feature_col_ref(
                metric_def.numerator.field, root_entity.name, root_alias,
                features_by_entity, fv_joins, fv_counter, dialect=dialect,
            )
            den_ref = _feature_col_ref(
                metric_def.denominator.field, root_entity.name, root_alias,
                features_by_entity, fv_joins, fv_counter, dialect=dialect,
            )
            num_agg = _build_agg(metric_def.numerator.rollup, num_ref)
            den_agg = _build_agg(metric_def.denominator.rollup, den_ref)
            select_parts.append(f"CAST({num_agg} AS REAL) / {den_agg} AS {metric_name}")
            continue
        child_agg = _build_child_entity_agg(
            metric_def, root_entity, entities, root_alias, used, child_agg_joins, rollup_map,
        )
        if child_agg is not None:
            select_parts.append(f"{child_agg} AS {metric_name}")
            continue
        field_ref = _feature_col_ref(
            metric_def.field, root_entity.name, root_alias,
            features_by_entity, fv_joins, fv_counter, dialect=dialect,
        )
        fn = metric_def.rollup
        if fn == "count_distinct":
            agg = f"COUNT(DISTINCT {field_ref})"
        else:
            agg = f"{rollup_map[fn]}({field_ref})"
        select_parts.append(f"{agg} AS {metric_name}")

    from_clause = f"FROM {root_entity.table_name} AS {root_alias}"

    filter_field_map: dict[str, str] = {}
    if features_by_entity and query.filters:
        for filter_str in query.filters:
            for field_name in collect_filter_column_refs(filter_str):
                if "." not in field_name and field_name not in filter_field_map:
                    expr = _feature_col_ref(
                        field_name, root_entity.name, root_alias,
                        features_by_entity, fv_joins, fv_counter, dialect=dialect,
                    )
                    if expr != f"{root_alias}.{field_name}":
                        filter_field_map[field_name] = expr

    # Ensure JOINs exist for entities referenced in filter traversals but not yet joined
    for path_tuple in _collect_filter_traversal_paths(query.filters, root_entity.name):
        current_ent = root_entity
        current_al = root_alias
        for hop_name in path_tuple:
            if hop_name in joined_entities:
                current_ent = entities[hop_name]
                current_al = joined_entities[hop_name]
                continue
            rel = next(
                (r for r in current_ent.relations
                 if r.cardinality == Cardinality.MANY_TO_ONE and r.target == hop_name),
                None,
            )
            if rel is None:
                break
            target_ent = entities[hop_name]
            tgt_alias = _make_alias(hop_name, used)
            join_steps_all.append(JoinStep(
                from_alias=current_al, from_col=rel.via,
                target_table=target_ent.table_name, target_alias=tgt_alias,
                target_pk=target_ent.primary_key.name,
                has_history=target_ent.history is not None,
                time_ref_col=time_ref_col if target_ent.history is not None else None,
            ))
            joined_entities[hop_name] = tgt_alias
            current_ent = target_ent
            current_al = tgt_alias

    def filter_path_resolver(parts: list[str]) -> str:
        if len(parts) == 1 or parts[0] == root_entity.name:
            return f"{root_alias}.{parts[-1]}"
        current_al = root_alias
        for part in parts[:-1]:
            if part != root_entity.name and part in joined_entities:
                current_al = joined_entities[part]
        return f"COALESCE({current_al}.{parts[-1]}, '(unset)')"

    join_clauses = [_build_join_clause(step) for step in join_steps_all]
    join_clauses.extend(fv_joins)
    join_clauses.extend(child_agg_joins)

    where_clause = ""
    if query.filters:
        filter_parts = []
        for filter_str in query.filters:
            sql = compile_filter_yaml(
                filter_str, alias=root_alias,
                path_resolver=filter_path_resolver,
                field_override_map=filter_field_map,
                dialect=dialect,
            )
            filter_parts.append(sql)
        where_clause = "WHERE " + "\n  AND ".join(filter_parts)

    group_by_clause = ""
    if group_by_parts:
        group_by_clause = "GROUP BY " + ", ".join(group_by_parts)

    order_by_clause = ""
    if query.order_by:
        # New top-level order: block — use directly; col names are aliases as written
        order_parts = [f'"{col}" {direction.upper()}' for col, direction in query.order_by]
        order_by_clause = "ORDER BY " + ", ".join(order_parts)
    else:
        # Legacy: per-attribute order_position (deprecated, kept for backward compat)
        order_items = []
        for s in query.slices:
            if s.order_position is not None:
                key = s.alias or s.field
                col = slice_col_aliases.get(key, key)
                order_items.append((s.order_position, s.order_direction, col))
        for metric_alias, m in query.metrics.items():
            if m.order_position is not None:
                order_items.append((m.order_position, m.order_direction, metric_alias))
        order_items.sort(key=lambda x: x[0])
        if order_items:
            order_parts = [f'"{col}" {direction.upper()}' for _, direction, col in order_items]
            order_by_clause = "ORDER BY " + ", ".join(order_parts)

    lines = [
        "SELECT",
        "  " + ",\n  ".join(select_parts),
        from_clause,
    ]
    lines.extend(join_clauses)
    if where_clause:
        lines.append(where_clause)
    if group_by_clause:
        lines.append(group_by_clause)
    if order_by_clause:
        lines.append(order_by_clause)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# SQLite execution
# ---------------------------------------------------------------------------

def _run_sql(db_url: str, sql: str) -> list[dict]:
    """Execute SQL against SQLite and return list of row dicts."""
    path = db_url.removeprefix("sqlite:///")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql)
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _run_scalar(db_url: str, sql: str) -> int:
    """Execute a COUNT(*) query and return the integer result."""
    path = db_url.removeprefix("sqlite:///")
    conn = sqlite3.connect(path)
    try:
        cur = conn.execute(sql)
        row = cur.fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _fmt_table(headers: list[str], rows: list[dict]) -> str:
    """Format rows as a fixed-width ASCII table with | separators."""
    def _cell(v) -> str:
        return "" if v is None else str(v)

    widths = {h: len(h) for h in headers}
    for row in rows:
        for h in headers:
            widths[h] = max(widths[h], len(_cell(row.get(h))))

    sep = "+-" + "-+-".join("-" * widths[h] for h in headers) + "-+"
    header_row = "| " + " | ".join(h.ljust(widths[h]) for h in headers) + " |"

    lines = [sep, header_row, sep]
    for row in rows:
        line = "| " + " | ".join(
            _cell(row.get(h)).ljust(widths[h]) for h in headers
        ) + " |"
        lines.append(line)
    lines.append(sep)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_query(
    query: QueryDef,
    entities: dict,
    db_url: str,
    output_csv: bool = False,
    output_json: bool = False,
    page: int | None = None,
    page_size: int | None = None,
    features_by_entity: dict | None = None,
) -> str:
    """Execute a query and return formatted output.

    Routes to _run_detail for non-aggregate queries and _run_aggregate for aggregate queries.
    Pagination applies to both paths.
    """
    if query.is_aggregate:
        return _run_aggregate(
            query, entities, db_url, output_csv, output_json,
            page, page_size, features_by_entity,
        )
    else:
        return _run_detail(
            query, entities, db_url, output_csv, output_json,
            page, page_size, features_by_entity,
        )


def _run_detail(
    query: QueryDef,
    entities: dict,
    db_url: str,
    output_csv: bool,
    output_json: bool,
    page: int | None,
    page_size: int | None,
    features_by_entity: dict | None,
) -> str:
    """Execute a detail (non-aggregate) query with pagination."""
    effective_page = page if page is not None else query.pagination.page
    effective_page_size = page_size if page_size is not None else query.pagination.page_size

    count_sql = _build_count_query(query, entities, features_by_entity)
    total = _run_scalar(db_url, count_sql)

    data_sql = build_detail_query(query, entities, effective_page, effective_page_size, features_by_entity)
    rows = _run_sql(db_url, data_sql)

    root_entity = entities[query.entity]
    headers = [_entity_qualified_alias(attr.parts, root_entity, entities) for attr in query.attributes]

    if output_json:
        return json.dumps(
            {
                "page": effective_page,
                "page_size": effective_page_size,
                "total": total,
                "rows": rows,
            },
            indent=2,
            default=str,
        )

    if output_csv:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        return buf.getvalue()

    table = _fmt_table(headers, rows)
    footer = f"Page {effective_page} · {len(rows)} of {total} rows"
    return f"{table}\n{footer}"


def _run_aggregate(
    query: QueryDef,
    entities: dict,
    db_url: str,
    output_csv: bool,
    output_json: bool,
    page: int | None,
    page_size: int | None,
    features_by_entity: dict | None,
) -> str:
    """Execute an aggregate (pivot-style) query with pagination."""
    effective_page = page if page is not None else query.pagination.page
    effective_page_size = page_size if page_size is not None else query.pagination.page_size

    inner_sql = build_aggregate_query(query, entities, features_by_entity)

    count_sql = f"SELECT COUNT(*) FROM (\n{inner_sql}\n)"
    total = _run_scalar(db_url, count_sql)

    offset = (effective_page - 1) * effective_page_size
    data_sql = f"SELECT * FROM (\n{inner_sql}\n) LIMIT {effective_page_size} OFFSET {offset}"
    rows = _run_sql(db_url, data_sql)

    # Apply threshold evaluation: replace numeric value with status string for threshold metrics
    for row in rows:
        for metric_name, metric in query.metrics.items():
            if metric.thresholds:
                row[metric_name] = evaluate_threshold(row.get(metric_name), metric.thresholds)

    root_entity = entities[query.entity]
    headers = []
    for slice_def in query.slices:
        headers.append(_slice_col_alias(slice_def, root_entity, entities))
    for attr in query.attributes:
        headers.append(_attr_col_alias(attr, root_entity))
    headers.extend(query.metrics)

    if output_json:
        return json.dumps(
            {
                "page": effective_page,
                "page_size": effective_page_size,
                "total": total,
                "rows": rows,
            },
            indent=2,
            default=str,
        )

    if output_csv:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        return buf.getvalue()

    table = _fmt_table(headers, rows)
    footer = f"Page {effective_page} · {len(rows)} of {total} rows"
    return f"{table}\n{footer}"
