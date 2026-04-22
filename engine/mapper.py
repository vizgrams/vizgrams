# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Execution engine for semantic layer mappers."""

from dataclasses import dataclass, field
from datetime import UTC, datetime

from core.db import DBBackend
from engine.filter_compiler import compile_filter_yaml
from engine.python_evaluator import evaluate
from semantic.expression import parse_expression_str as _parse_expression_str
from semantic.mapper_types import JoinCondition, MapperConfig, RowGroup, TargetDef
from semantic.types import EntityDef, SemanticHint

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class RowFailure:
    grain_key: str
    grain_value: object
    target_object: str
    reason: str
    source_values: dict


@dataclass
class TargetStats:
    object_name: str
    write_strategy: str  # "SCD2", "UPSERT", or "DEDUP"
    inserted_new: int = 0
    inserted_scd2: int = 0
    updated: int = 0
    skipped_no_change: int = 0
    skipped_duplicate: int = 0
    failed: int = 0


@dataclass
class MapperResult:
    mapper_name: str
    target_stats: list[TargetStats] = field(default_factory=list)
    failures: list[RowFailure] = field(default_factory=list)
    total_grain_rows: int = 0


class FanOutError(Exception):
    pass


class MapperError(Exception):
    pass


# ---------------------------------------------------------------------------
# Write context resolution from ontology
# ---------------------------------------------------------------------------

@dataclass
class _WriteContext:
    strategy: str  # "SCD2", "UPSERT", or "DEDUP"
    table_name: str
    key_col: str | None = None  # PK name for SCD2/UPSERT
    tracked_cols: list[str] | None = None  # for SCD2 change detection
    identity_cols: list[str] | None = None  # for DEDUP identity check
    managed_cols: set[str] = field(default_factory=set)  # SCD/INSERTED_AT cols
    initial_valid_from: str | None = None  # SCD2: backdate first insert valid_from


def _resolve_write_context(
    target: TargetDef, entities: list[EntityDef]
) -> _WriteContext:
    """Determine write strategy from the ontology."""
    name = target.entity_name

    # 1. Check top-level entities → SCD2 or UPSERT strategy
    for entity in entities:
        if entity.name == name:
            pk = entity.primary_key
            if not pk:
                raise MapperError(f"Entity {name!r} has no primary key")

            tracked = [a.name for a in entity.tracked_columns]

            if not entity.history:
                return _WriteContext(
                    strategy="UPSERT",
                    table_name=entity.table_name,
                    key_col=pk.name,
                    tracked_cols=tracked,
                )

            managed = set()
            for col in entity.history.columns:
                managed.add(col.name)

            return _WriteContext(
                strategy="SCD2",
                table_name=entity.table_name,
                key_col=pk.name,
                tracked_cols=tracked,
                managed_cols=managed,
                initial_valid_from=entity.history.initial_valid_from,
            )

    # 2. Check event sub-entities → DEDUP strategy
    for entity in entities:
        for event in entity.events:
            event_entity_name = _event_entity_name(entity, event)
            if event_entity_name == name:
                event_cols = entity.event_columns(event)
                identity = [
                    a.name for a in event_cols
                    if a.semantic != SemanticHint.INSERTED_AT
                ]
                managed = {
                    a.name for a in event_cols
                    if a.semantic == SemanticHint.INSERTED_AT
                }

                return _WriteContext(
                    strategy="DEDUP",
                    table_name=entity.event_table_name(event),
                    identity_cols=identity,
                    managed_cols=managed,
                )

    raise MapperError(f"Entity {name!r} not found in ontology (neither base nor event)")


def _event_entity_name(entity: EntityDef, event) -> str:
    """Build the PascalCase event entity name: ProductVersion + lifecycle → ProductVersionLifecycleEvent."""
    # Convert snake_case event name to PascalCase
    parts = event.name.split("_")
    pascal = "".join(p.capitalize() for p in parts)
    return f"{entity.name}{pascal}Event"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _source_table_expr(src, dialect: str = "sqlite") -> str:
    """Return the FROM/JOIN table expression for a source."""
    if src.union:
        col_list = ", ".join(src.columns)
        union_parts = [f"SELECT {col_list} FROM {table}" for table in src.union]
        union_sql = " UNION ALL ".join(union_parts)
        if src.deduplicate:
            group_cols = ", ".join(src.deduplicate)
            if dialect == "clickhouse":
                dedup_set = set(src.deduplicate)
                select_parts = [
                    col if col in dedup_set else f"any({col}) AS {col}"
                    for col in src.columns
                ]
                select_expr = ", ".join(select_parts)
            else:
                select_expr = col_list
            inner = f"SELECT {select_expr} FROM ({union_sql}) GROUP BY {group_cols}"
        else:
            inner = union_sql
        return f"({inner}) AS {src.alias}"
    if src.filter or src.deduplicate:
        if src.deduplicate and dialect == "clickhouse" and src.columns:
            # ClickHouse requires non-GROUP BY columns to be aggregated.
            # When filter is also present, push it into a subquery so the WHERE
            # runs on raw column values — not on the any()-aliased names — to
            # avoid "Aggregate function found in WHERE" (code 184).
            dedup_set = set(src.deduplicate)
            select_parts = [
                col if col in dedup_set else f"any({col}) AS {col}"
                for col in src.columns
            ]
            select_expr = ", ".join(select_parts)
            group_cols = ", ".join(src.deduplicate)
            if src.filter:
                where = compile_filter_yaml(src.filter, alias=None, dialect=dialect)
                from_expr = f"(SELECT * FROM {src.table} WHERE {where}) AS _pre"
            else:
                from_expr = src.table
            inner = f"SELECT {select_expr} FROM {from_expr} GROUP BY {group_cols}"
        else:
            parts = [f"SELECT * FROM {src.table}"]
            if src.filter:
                where = compile_filter_yaml(src.filter, alias=None, dialect=dialect)
                parts.append(f"WHERE {where}")
            if src.deduplicate:
                group_cols = ", ".join(src.deduplicate)
                parts.append(f"GROUP BY {group_cols}")
            inner = " ".join(parts)
        return f"({inner}) AS {src.alias}"
    return f"{src.table} AS {src.alias}"


def _ch_unnest_sac(src, array_col: str) -> str:
    """ClickHouse: expand a scalar-or-array column via arrayJoin for equi-join.

    ClickHouse rejects non-equality predicates (``has(...) OR ...``) in JOIN ON
    (code 403 INVALID_JOIN_ON_EXPRESSION).  By expanding the column here we get a
    plain ``left = right`` equality the join algorithm can use as a hash key.

    Behaviour per value:
    - JSON array ``["a","b"]`` → two rows with "a" and "b"
    - Plain string ``"a"``     → one row with "a"
    - NULL                     → zero rows (LEFT JOIN returns NULLs for src cols)

    NULL is checked first so ``assumeNotNull`` is only called on non-NULL values —
    ClickHouse rejects ``JSONExtractArrayRaw(Nullable(String))`` because it would
    return ``Nullable(Array(String))``, a nested Nullable type (code 43).
    """
    non_array = [c for c in src.columns if c != array_col]
    parts = [f"`{c}`" for c in non_array]
    parts.append(
        f"arrayJoin("
        f"if(`{array_col}` IS NULL, [], "
        f"if(length(JSONExtractArrayRaw(assumeNotNull(`{array_col}`))) > 0, "
        f"JSONExtract(assumeNotNull(`{array_col}`), 'Array(String)'), "
        f"[assumeNotNull(`{array_col}`)]"
        f")"
        f")) AS `{array_col}`"
    )
    inner = f"SELECT {', '.join(parts)} FROM {src.table}"
    return f"({inner}) AS {src.alias}"


def _ch_unnest_jac(src, jac_cond) -> tuple[str, str]:
    """ClickHouse: expand a json_array_contains right-side column for equi-join.

    Returns (table_expr, join_key_col_ref) where *join_key_col_ref* is
    ``alias._jk`` — an internal column added to the subquery that holds
    the individual expanded values.  The caller replaces the original
    ``json_array_contains`` condition with ``lhs = alias._jk``.

    Example: ``team.aliases`` with json_path ``$.jira`` →
        subquery adds ``arrayJoin(JSONExtract(..., 'jira', 'Array(String)')) AS _jk``
        condition becomes ``iss.jira_team_name = team._jk``
    """
    right_col = jac_cond.right.split(".")[-1]   # "team.aliases" → "aliases"
    alias = jac_cond.right.split(".")[0]         # "team.aliases" → "team"

    if jac_cond.json_path:
        path_key = jac_cond.json_path.lstrip("$.")
        jk_expr = (
            f"arrayJoin(JSONExtract(assumeNotNull(ifNull(`{right_col}`, '{{}}')), "
            f"'{path_key}', 'Array(String)'))"
        )
    else:
        jk_expr = (
            f"arrayJoin(if(`{right_col}` IS NULL, [], "
            f"if(length(JSONExtractArrayRaw(assumeNotNull(`{right_col}`))) > 0, "
            f"JSONExtract(assumeNotNull(`{right_col}`), 'Array(String)'), "
            f"[assumeNotNull(`{right_col}`)]"
            f")))"
        )

    select_cols = [f"`{c}`" for c in src.columns]
    select_cols.append(f"{jk_expr} AS `_jk`")
    inner = f"SELECT {', '.join(select_cols)} FROM {src.table}"
    table_expr = f"({inner}) AS {src.alias}"
    lhs = f"('{jac_cond.prefix}' || {jac_cond.left})" if jac_cond.prefix else jac_cond.left
    eq_cond = f"{lhs} = {alias}._jk"
    return table_expr, eq_cond


def _build_join_clause(
    join_src,
    join_conditions,
    join_type: str,
    dialect: str = "sqlite",
) -> str:
    """Build a single JOIN clause string, rewriting array-contains ops for ClickHouse.

    ClickHouse hash/sort-merge joins require equality conditions in ON.
    Both ``scalar_or_array_contains`` and ``json_array_contains`` produce
    non-equality predicates (``OR has(...)`` or plain ``has(...)``), which
    ClickHouse rejects with code 403.  We rewrite them as arrayJoin subqueries
    so the ON clause degrades to a plain equality the join engine can use.
    """
    if dialect == "clickhouse":
        sac_cond = next(
            (c for c in join_conditions if c.operator == "scalar_or_array_contains"), None
        )
        if sac_cond is not None:
            array_col = sac_cond.right.split(".")[-1]
            table_expr = _ch_unnest_sac(join_src, array_col)
            conditions = " AND ".join(
                f"{sac_cond.left} = {sac_cond.right}"
                if c.operator == "scalar_or_array_contains"
                else _compile_join_condition(c, dialect=dialect)
                for c in join_conditions
            )
            return f"{join_type} {table_expr} ON {conditions}"

        jac_cond = next(
            (c for c in join_conditions if c.operator == "json_array_contains"), None
        )
        if jac_cond is not None:
            table_expr, eq_cond = _ch_unnest_jac(join_src, jac_cond)
            conditions = " AND ".join(
                eq_cond if c.operator == "json_array_contains"
                else _compile_join_condition(c, dialect=dialect)
                for c in join_conditions
            )
            return f"{join_type} {table_expr} ON {conditions}"

    table_expr = _source_table_expr(join_src, dialect=dialect)
    conditions = " AND ".join(_compile_join_condition(c, dialect=dialect) for c in join_conditions)
    return f"{join_type} {table_expr} ON {conditions}"


def _compile_join_condition(c: JoinCondition, dialect: str = "sqlite") -> str:
    """Compile a single join ON condition to SQL."""
    if c.operator == "json_array_contains":
        lhs = f"('{c.prefix}' || {c.left})" if c.prefix else c.left
        if dialect == "clickhouse":
            if c.json_path:
                path_key = c.json_path.lstrip("$.")
                json_col = f"JSONExtract(ifNull({c.right}, '{{}}'), '{path_key}', 'Array(String)')"
            else:
                json_col = f"JSONExtract(ifNull({c.right}, '[]'), 'Array(String)')"
            return f"has({json_col}, ifNull({lhs}, ''))"
        json_col = c.right
        if c.json_path:
            json_col = f"json_extract({c.right}, '{c.json_path}')"
        return f"{lhs} IN (SELECT value FROM json_each({json_col}))"
    if c.operator == "scalar_or_array_contains":
        # Matches when right is a plain scalar string OR a JSON array containing left.
        if dialect == "clickhouse":
            # JSONExtract returns [] for non-array input, so no isValidJSON guard needed.
            return (
                f"({c.right} = {c.left} OR "
                f"has(JSONExtract(ifNull({c.right}, '[]'), 'Array(String)'), ifNull({c.left}, '')))"
            )
        return (
            f"({c.right} = {c.left} OR "
            f"(json_valid({c.right}) AND {c.left} IN (SELECT value FROM json_each({c.right}))))"
        )
    return f"{c.left} = {c.right}"


def _build_source_query(config: MapperConfig, dialect: str = "sqlite") -> str:
    """Build SELECT ... FROM ... LEFT JOIN ... SQL text."""
    grain_source = config.get_source(config.grain)
    if not grain_source:
        raise MapperError(f"Grain source {config.grain!r} not found")

    select_parts = []
    for src in config.sources:
        for col in src.columns:
            select_parts.append(f'{src.alias}.{col} AS "{src.alias}.{col}"')

    select_clause = ", ".join(select_parts)
    from_clause = _source_table_expr(grain_source, dialect=dialect)

    join_clauses = []
    for join in config.joins:
        join_src = config.get_source(join.to_alias)
        if not join_src:
            raise MapperError(f"Join target source {join.to_alias!r} not found")

        join_type = "LEFT JOIN" if join.join_type.value == "left" else "INNER JOIN"
        join_clauses.append(_build_join_clause(join_src, join.on, join_type, dialect=dialect))

    parts = [f"SELECT {select_clause}", f"FROM {from_clause}"]
    parts.extend(join_clauses)
    return "\n".join(parts)


def _build_row_group_query(group: RowGroup, config: MapperConfig, dialect: str = "sqlite") -> str:
    """Build SELECT ... FROM ... JOIN ... SQL for a single row group."""
    from_src = config.get_source(group.from_alias)
    if not from_src:
        raise MapperError(f"Row group from source {group.from_alias!r} not found")

    select_parts = []
    for col in from_src.columns:
        select_parts.append(f'{from_src.alias}.{col} AS "{from_src.alias}.{col}"')

    join_clauses = []
    for join_def in group.joins:
        to_src = config.get_source(join_def.to_alias)
        if not to_src:
            raise MapperError(f"Row group join target source {join_def.to_alias!r} not found")
        for col in to_src.columns:
            select_parts.append(f'{to_src.alias}.{col} AS "{to_src.alias}.{col}"')
        join_type = "LEFT JOIN" if join_def.join_type.value == "left" else "INNER JOIN"
        join_clauses.append(_build_join_clause(to_src, join_def.on, join_type, dialect=dialect))

    select_clause = ", ".join(select_parts)
    from_clause = _source_table_expr(from_src, dialect=dialect)

    parts = [f"SELECT {select_clause}", f"FROM {from_clause}"]
    parts.extend(join_clauses)
    return "\n".join(parts)


def _detect_fan_out(
    rows: list[dict], grain_alias: str, grain_columns: list[str]
) -> str | None:
    """Return error message if any grain key appears more than once."""
    seen = set()
    for row in rows:
        key_parts = tuple(row.get(f"{grain_alias}.{c}") for c in grain_columns)
        if key_parts in seen:
            return f"Fan-out detected: grain key {key_parts} appears more than once"
        seen.add(key_parts)
    return None


def _make_row_dict(row: dict) -> dict[str, dict[str, object]]:
    """Convert flat {"alias.col": val} to nested {alias: {col: val}}."""
    result = {}
    for key, val in row.items():
        if "." in key:
            alias, col = key.split(".", 1)
            result.setdefault(alias, {})[col] = val
    return result


def _fetch_rows(backend, query: str) -> list[dict]:
    """Execute a SELECT query and return rows as a list of dicts."""
    rows = backend.execute(query)
    if not rows:
        return []
    return [dict(zip(backend.last_columns, r)) for r in rows]


# ---------------------------------------------------------------------------
# Write functions
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Source backend routing helpers (split-database ClickHouse support)
# ---------------------------------------------------------------------------

def _collect_bare_source_tables(config: "MapperConfig") -> set[str]:
    """Return bare (un-prefixed) table names referenced by mapper sources."""
    tables: set[str] = set()
    for s in config.sources:
        if s.table and not s.table.startswith(("raw_", "sem_")):
            tables.add(s.table)
        if s.union:
            for t in s.union:
                if not t.startswith(("raw_", "sem_")):
                    tables.add(t)
    return tables


def _resolve_source_backend(config: "MapperConfig", source_backend, backend):
    """Return (read_backend, raw_tables) for executing source queries.

    ``source_backend`` is the raw-namespace backend (e.g. mymodel_raw).
    ``backend`` is the sem-namespace backend (e.g. mymodel).

    Mapper source tables fall into three categories:
      sem_<name>  — semantic table written by a prior mapper; lives in the sem
                    database.  _maybe_add_final strips the prefix so the sem
                    backend can resolve the bare name locally.
      raw_<name>  — raw table already qualified for cross-db queries; _maybe_add_final
                    expands it to raw_database.name on the sem backend.
      bare <name> — may live in either the raw or sem database; we check via
                    table_exists on source_backend to decide.

    Routing rules (applied in order):
      1. No source_backend (single-database setup) → always use backend.
      2. Any sem_-prefixed source → must use backend (sem) so that _maybe_add_final
         can strip the prefix and resolve it in the sem database.  Bare raw tables
         in the same query are added to the returned raw_tables set so they get
         prefixed with raw_ before the sem backend qualifies them cross-database.
      3. All tables are bare and all exist in the raw database → use source_backend
         directly (simpler, avoids cross-db overhead).
      4. Any bare table not found in raw (i.e. a sem entity named without prefix)
         → use backend with raw_ prefixing for the tables that are in raw.
    """
    if source_backend is None:
        return backend, set()

    has_sem_prefix = False
    bare: set[str] = set()
    for s in config.sources:
        tables = ([s.table] if s.table else []) + (s.union or [])
        for t in tables:
            if t.startswith("sem_"):
                has_sem_prefix = True
            elif not t.startswith("raw_"):
                bare.add(t)

    if has_sem_prefix:
        # Rule 2: sem_ sources require the sem backend.  Identify bare raw tables
        # so _prefix_raw_tables can add raw_ before _maybe_add_final sees them.
        raw_tables = {t for t in bare if source_backend.table_exists(t)}
        return backend, raw_tables

    if not bare:
        # Only raw_-prefixed tables (no sem_, no bare) — either backend works;
        # prefer source_backend for consistency.
        return source_backend, set()

    raw_tables = {t for t in bare if source_backend.table_exists(t)}
    sem_tables = bare - raw_tables

    if not sem_tables:
        # Rule 3: all bare tables are in the raw database.
        return source_backend, set()

    # Rule 4: some bare tables are sem entities.
    return backend, raw_tables


def _prefix_raw_tables(sql: str, raw_tables: set) -> str:
    """Rewrite FROM/JOIN <table> → FROM/JOIN raw_<table> for each name in raw_tables.

    This is applied before the sem backend's _maybe_add_final so that
    raw_ prefixed names get qualified as raw_database.tablename.
    """
    if not raw_tables:
        return sql
    import re  # noqa: PLC0415
    for table in raw_tables:
        sql = re.sub(
            rf'(\b(?:FROM|JOIN)\s+)({re.escape(table)})(\b)',
            r'\1raw_\2\3',
            sql,
            flags=re.IGNORECASE,
        )
    return sql


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run_mapper(
    config: MapperConfig,
    ontology_entities: list[EntityDef],
    backend: DBBackend,
    strict: bool = False,
    dry_run: bool = False,
    source_backend: DBBackend | None = None,
) -> MapperResult:
    """Execute a mapper against the database.

    ``backend`` is used for all entity writes (sem_ tables).

    ``source_backend``, when provided, is used for source reads (raw_ tables).
    This supports the split-database model where raw and sem data live in
    separate ClickHouse databases.  When ``None``, reads and writes both use
    ``backend``.
    """
    result = MapperResult(mapper_name=config.name)

    # Resolve write contexts for all targets
    write_contexts = {}
    for target in config.targets:
        write_contexts[target.entity_name] = _resolve_write_context(
            target, ontology_entities
        )

    # Build enum lookup
    enum_lookup = {e.name: e for e in config.enums}

    # Multi-group path: targets with `rows` instead of `columns`
    has_multi_group = any(target.rows for target in config.targets)
    if has_multi_group:
        # Pre-parse expressions for all row groups
        parsed_exprs = {}
        for target in config.targets:
            for group in target.rows:
                for col in group.columns:
                    parsed_exprs[(target.entity_name, group.from_alias, col.name)] = (
                        _parse_expression_str(col.expression)
                    )

        # Verify target tables exist
        for target in config.targets:
            ctx = write_contexts[target.entity_name]
            if not backend.table_exists(ctx.table_name):
                raise MapperError(
                    f"Table {ctx.table_name!r} does not exist. "
                    f"Run 'materialize --entity {target.entity_name}' first."
                )

        use_bulk = hasattr(backend, 'bulk_upsert')
        use_bulk_scd2 = hasattr(backend, 'bulk_scd2')
        # {entity_name: (ctx, stats, [candidate_dict, ...])}
        bulk_buffers: dict[str, tuple] = {}

        try:
            for target in config.targets:
                ctx = write_contexts[target.entity_name]
                stats = None
                for s in result.target_stats:
                    if s.object_name == target.entity_name:
                        stats = s
                        break
                if stats is None:
                    stats = TargetStats(object_name=target.entity_name, write_strategy=ctx.strategy)
                    result.target_stats.append(stats)

                for group in target.rows:
                    rg_backend, raw_tbls = _resolve_source_backend(config, source_backend, backend)
                    query = _build_row_group_query(group, config, dialect=getattr(rg_backend, "dialect", "sqlite"))
                    query = _prefix_raw_tables(query, raw_tbls)
                    row_dicts = _fetch_rows(rg_backend, query)

                    result.total_grain_rows += len(row_dicts)

                    from_src = config.get_source(group.from_alias)
                    fan_out = _detect_fan_out(row_dicts, group.from_alias, from_src.columns)
                    if fan_out:
                        raise FanOutError(fan_out)

                    for row_dict in row_dicts:
                        nested_row = _make_row_dict(row_dict)
                        candidate = {}
                        eval_failed = False
                        try:
                            for col in group.columns:
                                ast = parsed_exprs[(target.entity_name, group.from_alias, col.name)]
                                candidate[col.name] = evaluate(ast, nested_row, enum_lookup)
                        except Exception as e:
                            eval_failed = True
                            result.failures.append(RowFailure(
                                grain_key=group.from_alias,
                                grain_value=nested_row.get(group.from_alias, {}),
                                target_object=target.entity_name,
                                reason=str(e),
                                source_values=nested_row,
                            ))
                            stats.failed += 1
                            if strict:
                                raise MapperError(f"Expression evaluation failed: {e}") from e

                        if eval_failed:
                            continue

                        if use_bulk and ctx.strategy in ("UPSERT", "DEDUP") or use_bulk_scd2 and ctx.strategy == "SCD2":
                            if target.entity_name not in bulk_buffers:
                                bulk_buffers[target.entity_name] = (ctx, stats, [])
                            bulk_buffers[target.entity_name][2].append(candidate)
                        else:
                            try:
                                _write_target_row_backend(backend, ctx, candidate, stats)
                            except MapperError:
                                raise
                            except Exception as e:
                                result.failures.append(RowFailure(
                                    grain_key=group.from_alias,
                                    grain_value=nested_row.get(group.from_alias, {}),
                                    target_object=target.entity_name,
                                    reason=str(e),
                                    source_values=nested_row,
                                ))
                                stats.failed += 1
                                if strict:
                                    raise MapperError(f"Write failed for {target.entity_name}: {e}") from e

            # Flush bulk buffers — one insert per target entity instead of one per row.
            for entity_name, (ctx, stats, candidates) in bulk_buffers.items():
                if not candidates:
                    continue
                try:
                    if ctx.strategy == "SCD2":
                        inserted, versioned = backend.bulk_scd2(ctx.table_name, candidates, ctx)
                        stats.inserted_new += inserted
                        stats.inserted_scd2 += versioned
                    else:
                        backend.bulk_upsert(ctx.table_name, candidates)
                        stats.inserted_new += len(candidates)
                except Exception as e:
                    if strict:
                        raise MapperError(f"Bulk write failed for {entity_name}: {e}") from e
                    stats.failed += len(candidates)

        except Exception:
            raise

        return result

    # Single-group path
    # Pre-parse all expressions
    parsed_exprs = {}
    for target in config.targets:
        for col in target.columns:
            parsed_exprs[(target.entity_name, col.name)] = _parse_expression_str(col.expression)

    # Verify target tables exist before processing
    for target in config.targets:
        ctx = write_contexts[target.entity_name]
        if not backend.table_exists(ctx.table_name):
            raise MapperError(
                f"Table {ctx.table_name!r} does not exist. "
                f"Run 'materialize --entity {target.entity_name}' first."
            )

    # Route source reads to the correct backend.  Extractor-written (raw) tables
    # live in source_backend; mapper-written (sem) tables live in backend.
    # _resolve_source_backend inspects which tables actually exist in each db
    # and returns the appropriate backend plus any bare names that need a raw_
    # prefix so the sem backend's _maybe_add_final can cross-qualify them.
    read_backend, raw_tbls = _resolve_source_backend(config, source_backend, backend)
    source_dialect = getattr(read_backend, "dialect", "sqlite")
    query = _build_source_query(config, dialect=source_dialect)
    query = _prefix_raw_tables(query, raw_tbls)
    row_dicts = _fetch_rows(read_backend, query)
    result.total_grain_rows = len(row_dicts)

    grain_source = config.get_source(config.grain)
    fan_out = _detect_fan_out(row_dicts, config.grain, grain_source.columns)
    if fan_out:
        raise FanOutError(fan_out)

    try:
        _process_rows(
            backend, config, write_contexts, row_dicts, parsed_exprs,
            enum_lookup, result, strict, dry_run=dry_run,
        )
    except Exception:
        raise

    return result


def _process_rows(backend, config, write_contexts, row_dicts,
                  parsed_exprs, enum_lookup, result, strict, dry_run: bool = False):
    """Process grain rows: evaluate expressions and write targets.

    When the backend supports bulk_upsert (e.g. ClickHouse), all candidate rows
    are buffered in memory and flushed in a single insert per target entity at the
    end.  This reduces the number of ClickHouse parts from O(rows) to O(1),
    avoiding the explosive inactive-part accumulation that fills the VM disk.
    """
    use_bulk = hasattr(backend, 'bulk_upsert')
    use_bulk_scd2 = hasattr(backend, 'bulk_scd2')
    # {entity_name: (ctx, stats, [candidate_dict, ...])}
    bulk_buffers: dict[str, tuple] = {}

    for row_dict in row_dicts:
        nested_row = _make_row_dict(row_dict)

        for target in config.targets:
            ctx = write_contexts[target.entity_name]

            # Find or create stats
            stats = None
            for s in result.target_stats:
                if s.object_name == target.entity_name:
                    stats = s
                    break
            if stats is None:
                stats = TargetStats(
                    object_name=target.entity_name,
                    write_strategy=ctx.strategy,
                )
                result.target_stats.append(stats)

            # Evaluate expressions to build candidate row
            candidate = {}
            eval_failed = False
            try:
                for col in target.columns:
                    ast = parsed_exprs[(target.entity_name, col.name)]
                    candidate[col.name] = evaluate(ast, nested_row, enum_lookup)
            except Exception as e:
                eval_failed = True
                failure = RowFailure(
                    grain_key=config.grain,
                    grain_value=nested_row.get(config.grain, {}),
                    target_object=target.entity_name,
                    reason=str(e),
                    source_values=nested_row,
                )
                result.failures.append(failure)
                stats.failed += 1
                if strict:
                    raise MapperError(f"Expression evaluation failed: {e}") from e

            if eval_failed:
                continue

            if use_bulk and ctx.strategy in ("UPSERT", "DEDUP"):
                # Buffer for a single bulk insert at the end of the loop.
                if target.entity_name not in bulk_buffers:
                    bulk_buffers[target.entity_name] = (ctx, stats, [])
                bulk_buffers[target.entity_name][2].append(candidate)
            elif use_bulk_scd2 and ctx.strategy == "SCD2":
                # Buffer SCD2 candidates for a single read-diff-insert at the end.
                if target.entity_name not in bulk_buffers:
                    bulk_buffers[target.entity_name] = (ctx, stats, [])
                bulk_buffers[target.entity_name][2].append(candidate)
            else:
                try:
                    _write_target_row_backend(backend, ctx, candidate, stats)
                except MapperError:
                    raise
                except Exception as e:
                    failure = RowFailure(
                        grain_key=config.grain,
                        grain_value=nested_row.get(config.grain, {}),
                        target_object=target.entity_name,
                        reason=str(e),
                        source_values=nested_row,
                    )
                    result.failures.append(failure)
                    stats.failed += 1
                    if strict:
                        raise MapperError(f"Write failed for {target.entity_name}: {e}") from e

    # Flush bulk buffers — one insert per target entity instead of one per row.
    for entity_name, (ctx, stats, candidates) in bulk_buffers.items():
        if not candidates:
            continue
        if dry_run:
            # Estimate stats without writing; SCD2 skips are not tracked here.
            if ctx.strategy == "SCD2":
                stats.inserted_new += len(candidates)
            else:
                stats.inserted_new += len(candidates)
            continue
        try:
            if ctx.strategy == "SCD2":
                inserted, versioned = backend.bulk_scd2(ctx.table_name, candidates, ctx)
                stats.inserted_new += inserted
                stats.inserted_scd2 += versioned
            else:
                backend.bulk_upsert(ctx.table_name, candidates)
                stats.inserted_new += len(candidates)
        except Exception as e:
            if strict:
                raise MapperError(f"Bulk write failed for {entity_name}: {e}") from e
            stats.failed += len(candidates)


def _write_target_row_backend(backend, ctx: _WriteContext, candidate: dict, stats: TargetStats):
    """Dispatch a candidate row write using the backend's high-level API.

    UPSERT and DEDUP rows are written directly using backend.upsert()/append();
    deduplication and versioning are handled by the storage engine (e.g. ReplacingMergeTree).
    SCD2 is not supported by this path — it requires bulk_scd2.
    """
    if ctx.strategy == "DEDUP":
        backend.append(ctx.table_name, candidate)
        stats.inserted_new += 1
    elif ctx.strategy == "UPSERT":
        backend.upsert(ctx.table_name, candidate)
        stats.inserted_new += 1
    elif ctx.strategy == "SCD2":
        if hasattr(backend, 'bulk_scd2'):
            backend.bulk_scd2(ctx.table_name, [candidate], ctx)
            stats.inserted_new += 1
        else:
            raise MapperError(
                f"SCD2 write strategy is not supported for {type(backend).__name__}. "
                "Use a SQLite-backed model for entities with history tracking."
            )
    else:
        raise MapperError(f"Unknown write strategy: {ctx.strategy!r}")


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------

def topological_sort(mappers: list[MapperConfig]) -> list[MapperConfig]:
    """Sort mappers by depends_on. Raises ValueError on cycles."""
    name_map = {m.name: m for m in mappers}
    visited = set()
    in_stack = set()
    order = []

    def visit(name):
        if name in in_stack:
            raise ValueError(f"Dependency cycle detected involving {name!r}")
        if name in visited:
            return
        in_stack.add(name)
        mapper = name_map.get(name)
        if mapper:
            for dep in mapper.depends_on:
                visit(dep)
        in_stack.remove(name)
        visited.add(name)
        if mapper:
            order.append(mapper)

    for m in mappers:
        visit(m.name)

    return order


def build_execution_waves(mappers: list[MapperConfig]) -> list[list[MapperConfig]]:
    """Group mappers into parallel execution waves respecting depends_on.

    All mappers within a wave are mutually independent and can execute
    concurrently.  Wave N must fully complete before wave N+1 begins.

    Dependencies that reference non-existent mapper names are silently ignored
    (treated as satisfied), so stale depends_on entries do not block execution.

    Raises ValueError on dependency cycles.
    """
    name_set = {m.name for m in mappers}
    name_to_mapper = {m.name: m for m in mappers}
    depths: dict[str, int] = {}

    def compute_depth(name: str, visiting: frozenset[str]) -> int:
        if name in depths:
            return depths[name]
        if name not in name_to_mapper:
            return 0
        if name in visiting:
            raise ValueError(f"Dependency cycle detected involving {name!r}")
        mapper = name_to_mapper[name]
        valid_deps = [d for d in mapper.depends_on if d in name_set]
        visiting = visiting | {name}
        d = (1 + max(compute_depth(d, visiting) for d in valid_deps)) if valid_deps else 0
        depths[name] = d
        return d

    for m in mappers:
        compute_depth(m.name, frozenset())

    if not mappers:
        return []

    max_depth = max(depths.values())
    waves: list[list[MapperConfig]] = [[] for _ in range(max_depth + 1)]
    for m in mappers:
        waves[depths[m.name]].append(m)
    return [w for w in waves if w]
