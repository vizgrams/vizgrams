# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Expression compiler — compiles ExpressionFeatureDef to SQL (ADR-116 Phase 3)."""

from __future__ import annotations

from dataclasses import dataclass, field

from semantic.expression import (
    AggExpr,
    AggFunc,
    BinOp,
    CaseWhenExpr,
    Expr,
    ExpressionFeatureDef,
    FieldRef,
    FuncCallExpr,
    Lit,
    UnaryExpr,
    WindowExpr,
)
from semantic.types import Cardinality, EntityDef, RelationDef

# ---------------------------------------------------------------------------
# CompileContext
# ---------------------------------------------------------------------------

@dataclass
class CompileContext:
    root_entity: EntityDef
    root_alias: str
    entities: dict[str, EntityDef]
    join_steps: list[dict]   # accumulates needed JOINs as dicts
    joined: dict[str, str]   # entity_name → alias
    # When non-empty, bare single-part FieldRefs that match these names
    # resolve to root_alias.name without any join logic (outer window context).
    passthrough_cols: set = field(default_factory=set)
    dialect: str = "sqlite"


def _make_alias(name: str, used: set[str]) -> str:
    """Generate a unique short alias for an entity."""
    base = name[:3].lower()
    candidate = base
    i = 1
    while candidate in used:
        candidate = f"{base}{i}"
        i += 1
    used.add(candidate)
    return candidate


def _used_aliases(ctx: CompileContext) -> set[str]:
    return set(ctx.joined.values()) | {ctx.root_alias}


# ---------------------------------------------------------------------------
# compile_expr
# ---------------------------------------------------------------------------

def compile_expr(expr: Expr, ctx: CompileContext) -> str:
    """Recursively compile an Expr AST node to a SQL fragment."""
    if isinstance(expr, Lit):
        return _compile_lit(expr)
    if isinstance(expr, FieldRef):
        return _resolve_field(expr, ctx)
    if isinstance(expr, BinOp):
        return _compile_binop(expr, ctx)
    if isinstance(expr, AggExpr):
        return _compile_agg(expr, ctx)
    if isinstance(expr, CaseWhenExpr):
        return _compile_case_when(expr, ctx)
    if isinstance(expr, FuncCallExpr):
        return _compile_func(expr, ctx)
    if isinstance(expr, UnaryExpr):
        return _compile_unary(expr, ctx)
    if isinstance(expr, WindowExpr):
        return _compile_window_expr(expr, ctx)
    raise ValueError(f"Unknown expression node type: {type(expr).__name__}")


def _compile_lit(expr: Lit) -> str:
    if expr.value is None:
        return "NULL"
    if isinstance(expr.value, bool):
        return "1" if expr.value else "0"
    if isinstance(expr.value, str):
        if expr.value == "*":
            return "*"
        escaped = expr.value.replace("'", "''")
        return f"'{escaped}'"
    return str(expr.value)


def _add_one_to_many_join(
    rel: RelationDef,
    target_entity: EntityDef,
    ctx: CompileContext,
    from_alias: str | None = None,
    from_entity: EntityDef | None = None,
) -> str:
    """Add a LEFT JOIN for a ONE_TO_MANY relation; return the target alias.

    from_alias defaults to ctx.root_alias (the direct-child case).
    from_entity defaults to ctx.root_entity — pass the current entity when
    traversing multi-hop paths so the correct parent PK is used.

    For ONE_TO_MANY, the join is: target.child_col = parent.parent_col.
    New string form:  via="iata_code", via_target="airline_code"  → explicit columns.
    Legacy array form: via=["airline_code"]                       → child FK, parent is PK.
    """
    if not rel.via:
        raise ValueError(
            f"ONE_TO_MANY relation '{rel.name}' must define via columns"
        )
    join_from = from_alias if from_alias is not None else ctx.root_alias
    _from_entity = from_entity if from_entity is not None else ctx.root_entity
    # Build (parent_col, child_col) pairs.
    if isinstance(rel.via, list):
        join_pairs = [(col, col) for col in rel.via]
    else:
        join_pairs = [(rel.via, rel.via_target or _from_entity.primary_key.name)]
    used = _used_aliases(ctx)
    tgt_alias = _make_alias(rel.target, used)
    ctx.join_steps.append({
        "type": "one_to_many",
        "target_table": target_entity.table_name,
        "target_alias": tgt_alias,
        "from_alias": join_from,
        "join_pairs": join_pairs,
    })
    ctx.joined[rel.target] = tgt_alias
    return tgt_alias


def _resolve_field(expr: FieldRef, ctx: CompileContext) -> str:
    """Resolve a FieldRef, accumulating JOIN steps into ctx as needed.

    Single-part refs:
      1. Root entity columns (or passthrough_cols) → root alias
      2. ONE_TO_MANY entity columns (lazy join)    → target alias

    Multi-part refs — unified hop-by-hop traversal:
      Each intermediate part is resolved as ONE_TO_MANY then MANY_TO_ONE
      from the *current* entity (not always root), enabling explicit
      multi-hop paths like ProductVersion.ProductVersionLifecycleEvent.field.
    """
    parts = expr.parts

    # Single-part: bare field
    if len(parts) == 1:
        field_name = parts[0]
        root_col_names = {a.name for a in ctx.root_entity.all_base_columns}
        # passthrough_cols wins (outer window context: feature aliases + raw cols from inner)
        if field_name in root_col_names or field_name in ctx.passthrough_cols:
            return f"{ctx.root_alias}.{field_name}"
        # Try ONE_TO_MANY entities (lazy join)
        return _resolve_bare_in_one_to_many(field_name, ctx)

    # First part is the root entity itself — strip it and continue
    if parts[0] == ctx.root_entity.name:
        if len(parts) == 2:
            return f"{ctx.root_alias}.{parts[1]}"
        parts = parts[1:]

    # Unified multi-hop traversal: walk parts[:-1] as entity names,
    # following ONE_TO_MANY or MANY_TO_ONE at each step.
    current_entity = ctx.root_entity
    current_alias = ctx.root_alias

    for entity_name in parts[:-1]:
        # ONE_TO_MANY takes priority (explicit path traversal).
        # Match by target entity name OR by relation name (e.g. "routes" → Route).
        o2m_rel = next(
            (r for r in current_entity.relations
             if (r.target == entity_name or r.name == entity_name)
             and r.cardinality == Cardinality.ONE_TO_MANY),
            None,
        )
        if o2m_rel is not None:
            # When matched by relation name, look up the actual target entity
            target_entity = ctx.entities.get(o2m_rel.target) or ctx.entities.get(entity_name)
            if target_entity is None:
                raise ValueError(f"Unknown entity '{o2m_rel.target}' referenced in expression")
            join_key = entity_name  # use the path segment as the join key to avoid collisions
            if join_key not in ctx.joined:
                _add_one_to_many_join(o2m_rel, target_entity, ctx, from_alias=current_alias, from_entity=current_entity)
                # _add_one_to_many_join keys by target name; remap to relation name if different
                if entity_name != o2m_rel.target and o2m_rel.target in ctx.joined:
                    ctx.joined[entity_name] = ctx.joined[o2m_rel.target]
            current_alias = ctx.joined[join_key]
            current_entity = target_entity
            continue

        # Infer ONE_TO_MANY from inverse MANY_TO_ONE declared on the target entity.
        # e.g. ProductVersion has `product: MANY_TO_ONE via product_key` → infer the
        # Product→ProductVersion LEFT JOIN without requiring an explicit `versions:`
        # relation on Product.
        target_entity_candidate = ctx.entities.get(entity_name)
        if target_entity_candidate is not None:
            inverse_rel = next(
                (r for r in target_entity_candidate.relations
                 if r.target == current_entity.name and r.cardinality == Cardinality.MANY_TO_ONE),
                None,
            )
            if inverse_rel is not None:
                via_col = inverse_rel.via
                if not isinstance(via_col, str):
                    raise ValueError(
                        f"Inferred ONE_TO_MANY from '{entity_name}.{inverse_rel.name}' "
                        f"requires a single via column, got {via_col!r}"
                    )
                if entity_name not in ctx.joined:
                    used = _used_aliases(ctx)
                    tgt_alias = _make_alias(entity_name, used)
                    ctx.join_steps.append({
                        "type": "one_to_many",
                        "target_table": target_entity_candidate.table_name,
                        "target_alias": tgt_alias,
                        "from_alias": current_alias,
                        "join_pairs": [(current_entity.primary_key.name, via_col)],
                    })
                    ctx.joined[entity_name] = tgt_alias
                current_alias = ctx.joined[entity_name]
                current_entity = target_entity_candidate
                continue

        # MANY_TO_ONE
        m2o_rel = next(
            (r for r in current_entity.relations
             if r.target == entity_name and r.cardinality == Cardinality.MANY_TO_ONE),
            None,
        )
        if m2o_rel is not None:
            target_entity = ctx.entities.get(entity_name)
            if target_entity is None:
                raise ValueError(f"Unknown entity '{entity_name}' referenced in expression")
            if entity_name not in ctx.joined:
                used = _used_aliases(ctx)
                tgt_alias = _make_alias(entity_name, used)
                ctx.join_steps.append({
                    "from_alias": current_alias,
                    "from_col": m2o_rel.via,
                    "target_table": target_entity.table_name,
                    "target_alias": tgt_alias,
                    "target_pk": m2o_rel.via_target or target_entity.primary_key.name,
                    "has_history": target_entity.history is not None,
                })
                ctx.joined[entity_name] = tgt_alias
            current_alias = ctx.joined[entity_name]
            current_entity = target_entity
            continue

        # Dynamic relation — matched by relation name (e.g., "subject")
        dyn_rel = next(
            (r for r in current_entity.relations
             if r.name == entity_name and r.dynamic_field is not None),
            None,
        )
        if dyn_rel is not None:
            # Must be the last hop before the field
            field_name = parts[-1]
            return _compile_dynamic_relation(dyn_rel, field_name, current_alias, ctx)

        # MANY_TO_ONE by relation name (e.g. "is_authored_by" instead of "Identity")
        m2o_by_name = next(
            (r for r in current_entity.relations
             if r.name == entity_name and r.cardinality == Cardinality.MANY_TO_ONE),
            None,
        )
        if m2o_by_name is not None:
            target_name = m2o_by_name.target
            target_entity = ctx.entities.get(target_name)
            if target_entity is None:
                raise ValueError(f"Unknown entity '{target_name}' referenced in expression")
            join_key = entity_name  # keep relation name as key to avoid alias collision
            if join_key not in ctx.joined:
                used = _used_aliases(ctx)
                tgt_alias = _make_alias(target_name, used)
                ctx.join_steps.append({
                    "from_alias": current_alias,
                    "from_col": m2o_by_name.via,
                    "target_table": target_entity.table_name,
                    "target_alias": tgt_alias,
                    "target_pk": m2o_by_name.via_target or target_entity.primary_key.name,
                    "has_history": target_entity.history is not None,
                })
                ctx.joined[join_key] = tgt_alias
            current_alias = ctx.joined[join_key]
            current_entity = target_entity
            continue

        raise ValueError(
            f"No relation from '{current_entity.name}' to '{entity_name}' "
            f"in expression path {'.'.join(expr.parts)!r}"
        )

    return f"{current_alias}.{parts[-1]}"


def _compile_dynamic_relation(
    rel: RelationDef,
    field_name: str,
    from_alias: str,
    ctx: CompileContext,
) -> str:
    """Compile a dynamic polymorphic relation traversal to a COALESCE SQL expression.

    For each known entity that has field_name, emits a conditional LEFT JOIN:
        LEFT JOIN sem_entity alias ON alias.pk = from.via AND from.subject_type = 'EntityName'
    Returns COALESCE(alias1.field, alias2.field, ...) or NULL if none have the field.
    """
    type_col = f"{from_alias}.{rel.dynamic_field}"
    via_col = rel.via

    coalesce_parts: list[str] = []
    for entity_name, entity in sorted(ctx.entities.items()):
        entity_cols = {a.name for a in entity.all_base_columns}
        if field_name not in entity_cols:
            continue
        join_key = f"_dyn_{rel.name}_{entity_name}"
        if join_key not in ctx.joined:
            used = _used_aliases(ctx)
            dyn_alias = _make_alias(f"d{entity_name}", used)
            target_pk = rel.via_target or entity.primary_key.name
            ctx.join_steps.append({
                "type": "dynamic",
                "target_table": entity.table_name,
                "target_alias": dyn_alias,
                "from_alias": from_alias,
                "from_col": via_col,
                "target_pk": target_pk,
                "type_col": type_col,
                "entity_name": entity_name,
            })
            ctx.joined[join_key] = dyn_alias
        coalesce_parts.append(f"{ctx.joined[join_key]}.{field_name}")

    if not coalesce_parts:
        return "NULL"
    if len(coalesce_parts) == 1:
        return coalesce_parts[0]
    return f"COALESCE({', '.join(coalesce_parts)})"


def _resolve_bare_in_one_to_many(field_name: str, ctx: CompileContext) -> str:
    """Resolve a bare field by scanning ONE_TO_MANY entities for a unique match."""
    candidates: list[tuple[RelationDef, EntityDef]] = []
    for rel in ctx.root_entity.relations:
        if rel.cardinality != Cardinality.ONE_TO_MANY:
            continue
        target = ctx.entities.get(rel.target)
        if target is None:
            continue
        col_names = {a.name for a in target.all_base_columns}
        if field_name in col_names:
            candidates.append((rel, target))

    if len(candidates) == 0:
        # Check if field_name is a ONE_TO_MANY relation name itself
        # (e.g. count(routes) where `routes` is a relation, not a column).
        # Resolve to target PK so COUNT(pk) counts related rows correctly.
        rel_by_name = next(
            (r for r in ctx.root_entity.relations
             if r.name == field_name and r.cardinality == Cardinality.ONE_TO_MANY),
            None,
        )
        if rel_by_name is not None:
            target_entity = ctx.entities.get(rel_by_name.target)
            if target_entity is not None:
                if rel_by_name.target not in ctx.joined:
                    _add_one_to_many_join(rel_by_name, target_entity, ctx)
                pk = target_entity.primary_key
                pk_col = pk.name if pk else list(target_entity.all_base_columns)[0].name
                return f"{ctx.joined[rel_by_name.target]}.{pk_col}"
        # Fall through to root alias — SQL will error if the column doesn't exist
        return f"{ctx.root_alias}.{field_name}"
    if len(candidates) > 1:
        entity_names = [c[0].target for c in candidates]
        raise ValueError(
            f"Ambiguous bare field '{field_name}' — "
            f"found in multiple ONE_TO_MANY entities: {entity_names}. "
            "Use an explicit entity prefix (e.g. SprintIssue.{field_name})."
        )

    rel, target_entity = candidates[0]
    if rel.target not in ctx.joined:
        _add_one_to_many_join(rel, target_entity, ctx)
    return f"{ctx.joined[rel.target]}.{field_name}"


def _compile_binop(expr: BinOp, ctx: CompileContext) -> str:
    left_sql = compile_expr(expr.left, ctx)
    right_sql = compile_expr(expr.right, ctx)
    if expr.op == "/":
        # ADR 5.13.3: protect against division by zero
        return f"({left_sql} / NULLIF({right_sql}, 0))"
    return f"({left_sql} {expr.op} {right_sql})"


def _compile_agg(expr: AggExpr, ctx: CompileContext) -> str:
    inner_sql = compile_expr(expr.expr, ctx)
    if expr.func == AggFunc.COUNT_DISTINCT:
        return f"COUNT(DISTINCT {inner_sql})"
    if expr.func == AggFunc.COUNT and inner_sql == "*":
        return "COUNT(*)"
    return f"{expr.func.value.upper()}({inner_sql})"


def _compile_case_when(expr: CaseWhenExpr, ctx: CompileContext) -> str:
    when_sql = compile_expr(expr.when, ctx)
    then_sql = compile_expr(expr.then, ctx)
    if expr.else_ is not None:
        else_sql = compile_expr(expr.else_, ctx)
        return f"CASE WHEN {when_sql} THEN {then_sql} ELSE {else_sql} END"
    return f"CASE WHEN {when_sql} THEN {then_sql} END"


_DATETIME_DIFF_UNITS = {"seconds", "minutes", "hours", "days", "years"}


_SHORT_MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
_FULL_MONTHS  = ["January","February","March","April","May","June",
                 "July","August","September","October","November","December"]
_SHORT_DAYS   = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]   # strftime %w: 0=Sun
_FULL_DAYS    = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]


def _sqlite_case(strftime_tok: str, col: str, names: list[str], zero_based: bool = False) -> str:
    whens = " ".join(
        f"WHEN '{i if zero_based else i+1:02d}' THEN '{name}'"
        for i, name in enumerate(names)
    )
    return f"CASE strftime('{strftime_tok}', {col}) {whens} END"


# Ordered longest-first so overlapping tokens (MM vs MMM vs MMMM, E vs EEEE) match correctly.
_JAVA_TOKENS: list[tuple[str, object]] = [
    ("MMMM", lambda col: _sqlite_case("%m", col, _FULL_MONTHS)),
    ("MMM",  lambda col: _sqlite_case("%m", col, _SHORT_MONTHS)),
    ("EEEE", lambda col: _sqlite_case("%w", col, _FULL_DAYS,  zero_based=True)),
    ("yyyy", lambda col: f"strftime('%Y', {col})"),
    ("DDD",  lambda col: f"strftime('%j', {col})"),
    ("MM",   lambda col: f"strftime('%m', {col})"),
    ("HH",   lambda col: f"strftime('%H', {col})"),
    ("dd",   lambda col: f"strftime('%d', {col})"),
    ("mm",   lambda col: f"strftime('%M', {col})"),
    ("ss",   lambda col: f"strftime('%S', {col})"),
    ("yy",   lambda col: f"substr(strftime('%Y', {col}), 3, 2)"),
    ("E",    lambda col: _sqlite_case("%w", col, _SHORT_DAYS, zero_based=True)),
]


def _sqlite_format_date(col_sql: str, fmt: str) -> str:
    """Build a SQLite SQL expression implementing a Java-style date format string.

    Timestamps with timezone offsets are normalised to 19 chars (YYYY-MM-DDTHH:MM:SS)
    before formatting so that strftime handles them correctly.
    """
    # Normalise: strip milliseconds and timezone offset, keep up to seconds.
    norm = f"substr({col_sql}, 1, 19)"

    parts: list[str] = []
    i = 0
    while i < len(fmt):
        for token, sql_fn in _JAVA_TOKENS:
            if fmt[i:i + len(token)] == token:
                parts.append(sql_fn(norm))
                i += len(token)
                break
        else:
            # Literal character — wrap in single quotes, escape any embedded quote.
            ch = fmt[i].replace("'", "''")
            parts.append(f"'{ch}'")
            i += 1

    return parts[0] if len(parts) == 1 else " || ".join(parts)


def _sqlite_strftime_fmt(fmt: str) -> str:
    """Kept for import compatibility; delegates to _sqlite_format_date."""
    # This shim is no longer used internally but may be imported by query_runner.
    return fmt


# Ordered longest-first so that "YYYY" matches before "YY" etc.
_FORMAT_TIME_SQLITE_TOKENS = [
    ("YYYY", "%G"),   # ISO year (4 digits)
    ("WW",   "%V"),   # ISO week number (01-53)
    ("MM",   "%m"),   # month (01-12)
    ("DD",   "%d"),   # day (01-31)
    ("HH",   "%H"),   # hour (00-23)
]


def _compile_func(expr: FuncCallExpr, ctx: CompileContext) -> str:
    from engine.function_registry import render_function

    if expr.name == "argmax":
        return _compile_argmax(expr, ctx)

    if expr.name == "datetime_diff":
        if len(expr.args) != 2:
            raise ValueError(
                f"datetime_diff requires exactly 2 positional arguments (start, end), "
                f"got {len(expr.args)}"
            )
        unit_expr = expr.kwargs.get("unit")
        if unit_expr is None:
            raise ValueError("datetime_diff requires a 'unit' keyword argument")
        if not isinstance(unit_expr, Lit) or not isinstance(unit_expr.value, str):
            raise ValueError("datetime_diff 'unit' must be a string literal")
        unit = unit_expr.value.lower()
        if unit not in _DATETIME_DIFF_UNITS:
            raise ValueError(
                f"datetime_diff unit must be one of {sorted(_DATETIME_DIFF_UNITS)}, got {unit!r}"
            )
        args_sql = [compile_expr(a, ctx) for a in expr.args]
        return render_function("datetime_diff", args_sql, {"unit": unit}, dialect=ctx.dialect)

    if expr.name in ("format_date", "format_time"):
        if len(expr.args) != 2:
            raise ValueError(
                f"{expr.name} requires exactly 2 positional arguments (field, pattern), "
                f"got {len(expr.args)}"
            )
        if not isinstance(expr.args[1], Lit) or not isinstance(expr.args[1].value, str):
            raise ValueError(f"{expr.name} second argument must be a string literal")
        col_sql = compile_expr(expr.args[0], ctx)
        pattern = expr.args[1].value
        return render_function(expr.name, [col_sql], {"pattern": pattern}, dialect=ctx.dialect)

    if expr.name == "json_has_key":
        if len(expr.args) != 2:
            raise ValueError(
                f"json_has_key requires exactly 2 arguments (json_field, key), "
                f"got {len(expr.args)}"
            )
        args_sql = [compile_expr(a, ctx) for a in expr.args]
        return render_function("json_has_key", args_sql, {}, dialect=ctx.dialect)

    args_sql_str = ", ".join(compile_expr(a, ctx) for a in expr.args)
    return f"{expr.name.upper()}({args_sql_str})"


def _find_leaf_step_for_argmax(key_arg: FieldRef, ctx: CompileContext) -> dict:
    """Return the ONE_TO_MANY join step for the leaf entity in key_arg's traversal path.

    key_arg must be a qualified FieldRef (e.g. Entity.column or A.B.column) whose
    second-to-last part names the leaf entity.  That entity must already be joined.
    """
    entity_parts = list(key_arg.parts[:-1])  # strip the column name

    # Strip root entity prefix if present
    if entity_parts and entity_parts[0] == ctx.root_entity.name:
        entity_parts = entity_parts[1:]

    if not entity_parts:
        raise ValueError(
            "argmax: key must traverse at least one ONE_TO_MANY relation. "
            "Bare root-entity field references are not supported."
        )

    leaf_entity_name = entity_parts[-1]

    if leaf_entity_name not in ctx.joined:
        raise ValueError(
            f"argmax: leaf entity '{leaf_entity_name}' has not been joined. "
            "Ensure the key expression is compiled before calling this helper."
        )

    leaf_alias = ctx.joined[leaf_entity_name]
    leaf_step = next(
        (s for s in ctx.join_steps
         if s.get("type") == "one_to_many" and s.get("target_alias") == leaf_alias),
        None,
    )
    if leaf_step is None:
        raise ValueError(
            f"argmax: key must reach '{leaf_entity_name}' via a ONE_TO_MANY relation."
        )
    return leaf_step


def _compile_argmax(expr: FuncCallExpr, ctx: CompileContext) -> str:
    """Compile argmax(value_field, key_field) to a derived-table self-join + MAX(CASE WHEN).

    Emits:
        MAX(CASE WHEN leaf.key_col = _am.__max_key THEN leaf.value_col END)

    and appends a LEFT JOIN step:
        LEFT JOIN (SELECT fk_col, MAX(key_col) AS __max_key
                   FROM leaf_table GROUP BY fk_col) _am
                ON _am.fk_col = parent.fk_col
    """
    if len(expr.args) != 2:
        raise ValueError(
            f"argmax requires exactly 2 arguments (value, key), got {len(expr.args)}"
        )
    value_arg, key_arg = expr.args

    if not isinstance(key_arg, FieldRef):
        raise ValueError("argmax: key argument must be a field reference")

    key_col_name = key_arg.parts[-1]  # raw column name on the leaf entity

    # Compile both args — accumulates traversal JOIN steps into ctx
    value_sql = compile_expr(value_arg, ctx)
    key_sql = compile_expr(key_arg, ctx)

    # Identify the leaf entity join step from the key's traversal path
    leaf_step = _find_leaf_step_for_argmax(key_arg, ctx)
    leaf_table = leaf_step["target_table"]
    parent_alias = leaf_step["from_alias"]
    join_pairs = leaf_step["join_pairs"]

    # Subquery: SELECT fk_cols, MAX(key_col) AS __max_key FROM leaf_table GROUP BY fk_cols
    fk_cols = [tc for _sc, tc in join_pairs]
    fk_cols_str = ", ".join(fk_cols)
    max_col = "__max_key"
    subquery = (
        f"SELECT {fk_cols_str}, MAX({key_col_name}) AS {max_col} "
        f"FROM {leaf_table} GROUP BY {fk_cols_str}"
    )

    used = _used_aliases(ctx)
    argmax_alias = _make_alias("_am", used)

    ctx.join_steps.append({
        "type": "subquery",
        "subquery": subquery,
        "alias": argmax_alias,
        "parent_alias": parent_alias,
        "join_pairs": [(tc, tc) for _sc, tc in join_pairs],
    })

    return f"MAX(CASE WHEN {key_sql} = {argmax_alias}.{max_col} THEN {value_sql} END)"



def _compile_unary(expr: UnaryExpr, ctx: CompileContext) -> str:
    inner_sql = compile_expr(expr.expr, ctx)
    if expr.op == "is_null":
        return f"({inner_sql} IS NULL)"
    if expr.op == "is_not_null":
        return f"({inner_sql} IS NOT NULL)"
    if expr.op == "not":
        return f"(NOT {inner_sql})"
    if expr.op == "-":
        return f"(-{inner_sql})"
    raise ValueError(f"Unknown unary op: {expr.op!r}")


def _replace_windows(expr: Expr, collected: list) -> Expr:
    """Replace WindowExpr leaf nodes with FieldRef aliases, collecting (alias, WindowExpr).

    Used by _compile_window_feature_to_sql to produce a two-level query where
    window functions are evaluated in an intermediate SELECT, and the outermost
    SELECT only applies scalar functions over named window-result columns.
    This is required for ClickHouse, which does not allow window function calls
    to be nested inside scalar function arguments (e.g. toDateTime(LAG(...) OVER (...))).
    """
    if isinstance(expr, WindowExpr):
        alias = f"_w{len(collected)}"
        collected.append((alias, expr))
        return FieldRef(parts=[alias])
    if isinstance(expr, BinOp):
        return BinOp(
            left=_replace_windows(expr.left, collected),
            op=expr.op,
            right=_replace_windows(expr.right, collected),
        )
    if isinstance(expr, FuncCallExpr):
        return FuncCallExpr(
            name=expr.name,
            args=[_replace_windows(a, collected) for a in expr.args],
            kwargs={k: _replace_windows(v, collected) for k, v in expr.kwargs.items()},
        )
    if isinstance(expr, AggExpr):
        return AggExpr(func=expr.func, expr=_replace_windows(expr.expr, collected))
    if isinstance(expr, CaseWhenExpr):
        return CaseWhenExpr(
            when=_replace_windows(expr.when, collected),
            then=_replace_windows(expr.then, collected),
            else_=_replace_windows(expr.else_, collected) if expr.else_ is not None else None,
        )
    if isinstance(expr, UnaryExpr):
        return UnaryExpr(op=expr.op, expr=_replace_windows(expr.expr, collected))
    return expr  # Lit, FieldRef, ListLit, InExpr — return as-is


# ClickHouse uses different names for some standard SQL window functions.
_CH_WINDOW_FUNC_MAP = {
    "LAG":        "lagInFrame",
    "LEAD":       "leadInFrame",
    "ROW_NUMBER": "rowNumberInAllBlocks",
}


def _compile_window_expr(expr: WindowExpr, ctx: CompileContext) -> str:
    """Compile a LAG/LEAD/RANK-style window function to SQL."""
    arg_sql = compile_expr(expr.arg, ctx) if expr.arg is not None else ""
    part_sqls = [compile_expr(e, ctx) for e in expr.partition_by]
    ord_sqls = [compile_expr(e, ctx) for e in expr.order_by]
    over = (
        "OVER (PARTITION BY " + ", ".join(part_sqls)
        + " ORDER BY " + ", ".join(ord_sqls) + ")"
    )
    func = expr.func.upper()
    if ctx.dialect == "clickhouse":
        func = _CH_WINDOW_FUNC_MAP.get(func, func)
    if arg_sql:
        return f"{func}({arg_sql}) {over}"
    return f"{func}() {over}"


# ---------------------------------------------------------------------------
# compile_feature_to_sql
# ---------------------------------------------------------------------------

def _join_clauses_from_steps(join_steps: list[dict]) -> list[str]:
    """Convert accumulated join step dicts to SQL LEFT JOIN clauses."""
    clauses = []
    for step in join_steps:
        if step.get("type") == "one_to_many":
            conditions = " AND ".join(
                f"{step['target_alias']}.{tc} = {step['from_alias']}.{sc}"
                for sc, tc in step["join_pairs"]
            )
            clauses.append(
                f"LEFT JOIN {step['target_table']} AS {step['target_alias']} ON {conditions}"
            )
        elif step.get("type") == "subquery":
            join_cond = " AND ".join(
                f"{step['alias']}.{jc} = {step['parent_alias']}.{pc}"
                for jc, pc in step["join_pairs"]
            )
            clauses.append(
                f"LEFT JOIN ({step['subquery']}) AS {step['alias']} ON {join_cond}"
            )
        elif step.get("type") == "dynamic":
            cond = (
                f"{step['target_alias']}.{step['target_pk']}"
                f" = {step['from_alias']}.{step['from_col']}"
                f" AND {step['type_col']} = '{step['entity_name']}'"
            )
            clauses.append(
                f"LEFT JOIN {step['target_table']} AS {step['target_alias']} ON {cond}"
            )
        else:
            cond = (
                f"{step['target_alias']}.{step['target_pk']}"
                f" = {step['from_alias']}.{step['from_col']}"
            )
            if step["has_history"]:
                cond += f" AND {step['target_alias']}.valid_to IS NULL"
            clauses.append(
                f"LEFT JOIN {step['target_table']} AS {step['target_alias']} ON {cond}"
            )
    return clauses


def compile_feature_to_sql(
    feat: ExpressionFeatureDef,
    entities: dict[str, EntityDef],
    features: dict | None = None,
    dialect: str = "sqlite",
) -> str:
    """Compile an ExpressionFeatureDef to a SELECT SQL statement.

    For features containing window functions, features dict is required so that
    feature-to-feature references can be resolved and a two-phase SQL generated.

    Returns:
        SELECT <entity_key> AS entity_id, <expr> AS value
        FROM <root_table> <alias>
        [LEFT JOIN ...]
        [GROUP BY <entity_key>]
    """
    if _has_window(feat.expression):
        return _compile_window_feature_to_sql(feat, entities, features or {}, dialect=dialect)

    root_entity = entities.get(feat.entity_type)
    if root_entity is None:
        raise ValueError(f"Unknown entity type '{feat.entity_type}'")

    used: set[str] = set()
    root_alias = _make_alias(feat.entity_type, used)

    ctx = CompileContext(
        root_entity=root_entity,
        root_alias=root_alias,
        entities=entities,
        join_steps=[],
        joined={},
        dialect=dialect,
    )

    value_sql = compile_expr(feat.expression, ctx)

    parts_sql = [
        f"SELECT {root_alias}.{feat.entity_key} AS entity_id,",
        f"       {value_sql} AS value",
        f"FROM {root_entity.table_name} {root_alias}",
    ]
    parts_sql.extend(_join_clauses_from_steps(ctx.join_steps))

    if _has_aggregation(feat.expression):
        group_by = [f"{root_alias}.{feat.entity_key}"]
        if dialect == "clickhouse":
            # ClickHouse does not allow non-aggregate column refs in SELECT alongside
            # aggregates (unlike SQLite).  Collect root-entity columns that appear
            # outside any AggExpr and add them to GROUP BY.
            root_col_names = {a.name for a in root_entity.all_base_columns}
            for col in sorted(_collect_non_agg_root_refs(feat.expression) & root_col_names):
                if col != feat.entity_key:
                    group_by.append(f"{root_alias}.{col}")
        parts_sql.append(f"GROUP BY {', '.join(group_by)}")

    return "\n".join(parts_sql)


def _collect_non_agg_root_refs(expr: Expr) -> set[str]:
    """Return single-part FieldRef names that appear outside any AggExpr node."""
    result: set[str] = set()
    _walk_non_agg(expr, result, inside_agg=False)
    return result


def _walk_non_agg(expr: Expr, result: set, inside_agg: bool) -> None:
    if isinstance(expr, FieldRef):
        if len(expr.parts) == 1 and not inside_agg:
            result.add(expr.parts[0])
    elif isinstance(expr, AggExpr):
        pass  # stop — everything inside an aggregate is fine
    elif isinstance(expr, BinOp):
        _walk_non_agg(expr.left, result, inside_agg)
        _walk_non_agg(expr.right, result, inside_agg)
    elif isinstance(expr, FuncCallExpr):
        for a in expr.args:
            _walk_non_agg(a, result, inside_agg)
        for v in expr.kwargs.values():
            _walk_non_agg(v, result, inside_agg)
    elif isinstance(expr, CaseWhenExpr):
        _walk_non_agg(expr.when, result, inside_agg)
        _walk_non_agg(expr.then, result, inside_agg)
        if expr.else_ is not None:
            _walk_non_agg(expr.else_, result, inside_agg)
    elif isinstance(expr, UnaryExpr):
        _walk_non_agg(expr.expr, result, inside_agg)


def _has_aggregation(expr: Expr) -> bool:
    """Return True if the expression tree contains any AggExpr node."""
    if isinstance(expr, AggExpr):
        return True
    if isinstance(expr, CaseWhenExpr):
        return (
            _has_aggregation(expr.when)
            or _has_aggregation(expr.then)
            or (expr.else_ is not None and _has_aggregation(expr.else_))
        )
    if isinstance(expr, BinOp):
        return _has_aggregation(expr.left) or _has_aggregation(expr.right)
    if isinstance(expr, FuncCallExpr):
        if expr.name == "argmax":
            return True
        return (
            any(_has_aggregation(a) for a in expr.args)
            or any(_has_aggregation(v) for v in expr.kwargs.values())
        )
    if isinstance(expr, UnaryExpr):
        return _has_aggregation(expr.expr)
    return False


def _has_window(expr: Expr) -> bool:
    """Return True if the expression tree contains any WindowExpr node."""
    if isinstance(expr, WindowExpr):
        return True
    if isinstance(expr, BinOp):
        return _has_window(expr.left) or _has_window(expr.right)
    if isinstance(expr, FuncCallExpr):
        return (
            any(_has_window(a) for a in expr.args)
            or any(_has_window(v) for v in expr.kwargs.values())
        )
    if isinstance(expr, AggExpr):
        return _has_window(expr.expr)
    if isinstance(expr, CaseWhenExpr):
        return (
            _has_window(expr.when)
            or _has_window(expr.then)
            or (expr.else_ is not None and _has_window(expr.else_))
        )
    if isinstance(expr, UnaryExpr):
        return _has_window(expr.expr)
    return False


def _collect_feature_refs(expr: Expr, feature_names: set) -> set:
    """Find all single-part FieldRefs whose name matches a known feature short-name."""
    result: set = set()
    _walk_feature_refs(expr, feature_names, result)
    return result


def _walk_feature_refs(expr: Expr, feature_names: set, result: set) -> None:
    if isinstance(expr, FieldRef):
        if len(expr.parts) == 1 and expr.parts[0] in feature_names:
            result.add(expr.parts[0])
    elif isinstance(expr, WindowExpr):
        if expr.arg:
            _walk_feature_refs(expr.arg, feature_names, result)
        for e in expr.partition_by + expr.order_by:
            _walk_feature_refs(e, feature_names, result)
    elif isinstance(expr, BinOp):
        _walk_feature_refs(expr.left, feature_names, result)
        _walk_feature_refs(expr.right, feature_names, result)
    elif isinstance(expr, FuncCallExpr):
        for a in expr.args:
            _walk_feature_refs(a, feature_names, result)
        for v in expr.kwargs.values():
            _walk_feature_refs(v, feature_names, result)
    elif isinstance(expr, AggExpr):
        _walk_feature_refs(expr.expr, feature_names, result)
    elif isinstance(expr, CaseWhenExpr):
        _walk_feature_refs(expr.when, feature_names, result)
        _walk_feature_refs(expr.then, feature_names, result)
        if expr.else_ is not None:
            _walk_feature_refs(expr.else_, feature_names, result)
    elif isinstance(expr, UnaryExpr):
        _walk_feature_refs(expr.expr, feature_names, result)


def _collect_raw_col_refs(expr: Expr, feature_names: set) -> set:
    """Find all single-part FieldRefs that are NOT feature names (raw root-entity columns)."""
    result: set = set()
    _walk_raw_col_refs(expr, feature_names, result)
    return result


def _walk_raw_col_refs(expr: Expr, feature_names: set, result: set) -> None:
    if isinstance(expr, FieldRef):
        if len(expr.parts) == 1 and expr.parts[0] not in feature_names:
            result.add(expr.parts[0])
    elif isinstance(expr, WindowExpr):
        if expr.arg:
            _walk_raw_col_refs(expr.arg, feature_names, result)
        for e in expr.partition_by + expr.order_by:
            _walk_raw_col_refs(e, feature_names, result)
    elif isinstance(expr, BinOp):
        _walk_raw_col_refs(expr.left, feature_names, result)
        _walk_raw_col_refs(expr.right, feature_names, result)
    elif isinstance(expr, FuncCallExpr):
        for a in expr.args:
            _walk_raw_col_refs(a, feature_names, result)
        for v in expr.kwargs.values():
            _walk_raw_col_refs(v, feature_names, result)
    elif isinstance(expr, AggExpr):
        _walk_raw_col_refs(expr.expr, feature_names, result)
    elif isinstance(expr, CaseWhenExpr):
        _walk_raw_col_refs(expr.when, feature_names, result)
        _walk_raw_col_refs(expr.then, feature_names, result)
        if expr.else_ is not None:
            _walk_raw_col_refs(expr.else_, feature_names, result)
    elif isinstance(expr, UnaryExpr):
        _walk_raw_col_refs(expr.expr, feature_names, result)


def _compile_window_feature_to_sql(
    feat: ExpressionFeatureDef,
    entities: dict[str, EntityDef],
    features: dict,
    dialect: str = "sqlite",
) -> str:
    """Compile a window-function feature to a two-phase SELECT SQL.

    Phase 1 (inner subquery): aggregates per-entity values and includes any raw
    columns needed for the window PARTITION BY / ORDER BY clauses.
    Phase 2 (outer SELECT): applies window functions over the inner result set.
    """
    root_entity = entities.get(feat.entity_type)
    if root_entity is None:
        raise ValueError(f"Unknown entity type '{feat.entity_type}'")

    # Build short_name → ExpressionFeatureDef for same-entity expression features.
    # Two calling conventions are supported:
    #   - nested: {entity_type: {short_name: fd}}  (service layer / _load_features_by_entity)
    #   - flat:   {feature_id: fd}                 (direct calls / tests)
    same_entity_feats: dict[str, ExpressionFeatureDef] = {}
    my_short_name = feat.feature_id.split(".", 1)[1] if "." in feat.feature_id else feat.feature_id
    entity_bucket = features.get(feat.entity_type)
    if isinstance(entity_bucket, dict):
        # Nested format
        same_entity_feats.update({
            short_name: f
            for short_name, f in entity_bucket.items()
            if isinstance(f, ExpressionFeatureDef) and short_name != my_short_name
        })
    else:
        # Flat format
        for fid, f in features.items():
            if (
                isinstance(f, ExpressionFeatureDef)
                and f.entity_type == feat.entity_type
                and fid != feat.feature_id
            ):
                short_name = fid.split(".", 1)[1] if "." in fid else fid
                same_entity_feats[short_name] = f

    feature_names = set(same_entity_feats.keys())
    feature_ref_names = _collect_feature_refs(feat.expression, feature_names)
    raw_col_refs = _collect_raw_col_refs(feat.expression, feature_names)

    # Validate: any bare ref that isn't a raw column on the root entity must be
    # a feature reference. If it's in raw_col_refs but not on the entity, it's
    # an unresolved feature reference (missing from the features dict).
    root_col_names = {a.name for a in root_entity.all_base_columns}
    unresolved = raw_col_refs - root_col_names - {feat.entity_key}
    if unresolved:
        for name in sorted(unresolved):
            raise ValueError(
                f"Feature '{feat.feature_id}' references feature '{name}' "
                f"which is not an expression feature on entity '{feat.entity_type}'"
            )

    # ---------- inner query ----------
    used: set[str] = set()
    inner_alias = _make_alias(feat.entity_type, used)
    inner_ctx = CompileContext(
        root_entity=root_entity,
        root_alias=inner_alias,
        entities=entities,
        join_steps=[],
        joined={},
        dialect=dialect,
    )

    # Compile each referenced feature's expression in the inner context
    feature_col_sqls: list[str] = []
    for fname in sorted(feature_ref_names):
        if fname not in same_entity_feats:
            raise ValueError(
                f"Feature '{feat.feature_id}' references feature '{fname}' "
                f"which is not an expression feature on entity '{feat.entity_type}'"
            )
        ref_expr_sql = compile_expr(same_entity_feats[fname].expression, inner_ctx)
        feature_col_sqls.append(f"  {ref_expr_sql} AS {fname}")

    inner_select_cols = [f"  {inner_alias}.{feat.entity_key}"]
    for col in sorted(raw_col_refs):
        inner_select_cols.append(f"  {inner_alias}.{col}")
    inner_select_cols.extend(feature_col_sqls)

    inner_has_agg = (
        any(_has_aggregation(same_entity_feats[f].expression) for f in feature_ref_names)
        or any(s.get("type") == "one_to_many" for s in inner_ctx.join_steps)
    )
    group_by_cols = [f"{inner_alias}.{feat.entity_key}"] + [
        f"{inner_alias}.{col}" for col in sorted(raw_col_refs)
    ]

    inner_parts = [
        "SELECT\n" + ",\n".join(inner_select_cols),
        f"FROM {root_entity.table_name} {inner_alias}",
    ]
    inner_parts.extend(_join_clauses_from_steps(inner_ctx.join_steps))
    if inner_has_agg:
        inner_parts.append("GROUP BY " + ", ".join(group_by_cols))

    inner_sql = "\n".join(inner_parts)
    indented_inner = "\n".join("  " + line for line in inner_sql.split("\n"))

    # ---------- outer query ----------
    passthrough = {feat.entity_key} | raw_col_refs | feature_ref_names

    # ClickHouse does not allow window function calls nested inside scalar function
    # arguments (e.g. toDateTime(LAG(...) OVER (...)) raises UNKNOWN_AGGREGATE_FUNCTION).
    # Fix: extract window expressions to an intermediate SELECT as named columns (_w0, _w1, …),
    # then apply scalar wrappers in the outermost SELECT over those aliases.
    if dialect == "clickhouse":
        window_replacements: list = []  # list of (alias_str, WindowExpr)
        replaced_expr = _replace_windows(feat.expression, window_replacements)

        if window_replacements:
            # Middle SELECT: explicit passthrough columns + window function columns.
            # We avoid SELECT * because ClickHouse misparses window functions when
            # combined with SELECT * in some contexts (treats LAG as aggregate).
            inner_base_alias = "inner_base"
            passthrough_col_sqls = (
                [f"  {inner_base_alias}.{feat.entity_key}"]
                + [f"  {inner_base_alias}.{col}" for col in sorted(raw_col_refs)]
                + [f"  {inner_base_alias}.{fname}" for fname in sorted(feature_ref_names)]
            )
            window_col_sqls = []
            for win_alias, win_expr in window_replacements:
                win_ctx = CompileContext(
                    root_entity=root_entity,
                    root_alias=inner_base_alias,
                    entities=entities,
                    join_steps=[],
                    joined={},
                    passthrough_cols=passthrough,
                    dialect=dialect,
                )
                window_col_sqls.append(
                    f"  {_compile_window_expr(win_expr, win_ctx)} AS {win_alias}"
                )

            all_middle_cols = passthrough_col_sqls + window_col_sqls
            middle_lines = (
                ["SELECT"] + [c + "," for c in all_middle_cols[:-1]] + [all_middle_cols[-1]] +
                ["FROM (", *["  " + ln for ln in inner_sql.split("\n")], f") {inner_base_alias}"]
            )
            indented_middle = "\n".join("  " + ln for ln in "\n".join(middle_lines).split("\n"))

            # Outer SELECT: scalar functions only, window results referenced by alias
            outer_alias = "w"
            outer_passthrough = passthrough | {a for a, _ in window_replacements}
            outer_ctx = CompileContext(
                root_entity=root_entity,
                root_alias=outer_alias,
                entities=entities,
                join_steps=[],
                joined={},
                passthrough_cols=outer_passthrough,
                dialect=dialect,
            )
            value_sql = compile_expr(replaced_expr, outer_ctx)
            return "\n".join([
                f"SELECT {outer_alias}.{feat.entity_key} AS entity_id,",
                f"       {value_sql} AS value",
                "FROM (",
                indented_middle,
                f") {outer_alias}",
            ])

    # Non-ClickHouse (or ClickHouse feature with no window nesting): two-level path
    outer_alias = "base"
    outer_ctx = CompileContext(
        root_entity=root_entity,
        root_alias=outer_alias,
        entities=entities,
        join_steps=[],
        joined={},
        passthrough_cols=passthrough,
        dialect=dialect,
    )

    value_sql = compile_expr(feat.expression, outer_ctx)

    return "\n".join([
        f"SELECT {outer_alias}.{feat.entity_key} AS entity_id,",
        f"       {value_sql} AS value",
        "FROM (",
        indented_inner,
        f") {outer_alias}",
    ])
