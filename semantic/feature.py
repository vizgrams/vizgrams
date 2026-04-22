# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Feature YAML and definition SQL validation + reconcile engine (ADR-113)."""

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

from core.validation import ValidationError, validate_schema

# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

@dataclass
class FeatureDef:
    feature_id: str
    name: str
    entity_type: str
    entity_key: str
    data_type: str             # 'STRING' | 'INTEGER' | 'FLOAT'
    materialization_mode: str  # 'materialized' | 'dynamic'
    raw_sql: str
    description: str | None = None
    dependencies: list[str] = field(default_factory=list)
    ttl_seconds: int | None = None
    version: int = 1
    feature_type: str = "raw_sql"


# ---------------------------------------------------------------------------
# Storage layer
# ---------------------------------------------------------------------------

def create_feature_tables(conn: sqlite3.Connection) -> None:
    """Create feature store tables (idempotent)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS __attribute_registry (
            attribute_name  TEXT,
            entity_type     TEXT,
            source_type     TEXT,
            source_ref      TEXT,
            data_type       TEXT,
            PRIMARY KEY (attribute_name, entity_type)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS __feature_definition (
            feature_id             TEXT PRIMARY KEY,
            name                   TEXT,
            description            TEXT,
            entity_type            TEXT,
            entity_key             TEXT,
            event_timestamp_column TEXT,
            data_type              TEXT,
            definition             TEXT,
            materialization_mode   TEXT,
            ttl_seconds            INTEGER,
            version                INTEGER,
            created_at             TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS __feature_value (
            feature_id  TEXT,
            entity_id   TEXT,
            value       TEXT,
            computed_at TEXT,
            PRIMARY KEY (feature_id, entity_id)
        )
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# SQL validation
# ---------------------------------------------------------------------------

def validate___feature_definition_sql(sql: str) -> list[ValidationError]:
    """Validate a feature definition SQL string against ADR-113 rules.

    Rules:
    - Must not contain ORDER BY
    - Must not contain LIMIT
    - Must output a column aliased as 'entity_id'
    - Must output a column aliased as 'value'
    - Must only reference entity tables or __feature_value (enforcement deferred to runtime)
    """
    errors = []

    # no ORDER BY
    if re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE):
        errors.append(ValidationError(
            path="definition",
            message="definition SQL must not contain ORDER BY",
            rule="definition_no_order_by",
        ))

    # no LIMIT
    if re.search(r"\bLIMIT\b", sql, re.IGNORECASE):
        errors.append(ValidationError(
            path="definition",
            message="definition SQL must not contain LIMIT",
            rule="definition_no_limit",
        ))

    # must alias entity_id and value output columns
    if not re.search(r"\bAS\s+entity_id\b", sql, re.IGNORECASE):
        errors.append(ValidationError(
            path="definition",
            message="definition SQL must output a column aliased as 'entity_id'",
            rule="definition_column_entity_id",
        ))
    if not re.search(r"\bAS\s+value\b", sql, re.IGNORECASE):
        errors.append(ValidationError(
            path="definition",
            message="definition SQL must output a column aliased as 'value'",
            rule="definition_column_value",
        ))

    return errors


def validate_feature_yaml(path: str | Path) -> list[ValidationError]:
    """Validate a feature YAML config file (schema + SQL definition rules)."""
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)

    # Phase 1: structural (JSON Schema)
    errors = validate_schema(data, "feature")
    if errors:
        return errors

    # Phase 2: definition SQL rules (raw_sql / definition keys; skip expression features)
    if "expr" not in data:
        definition = (data.get("raw_sql") or data.get("definition") or "").strip()
        if definition:
            errors.extend(validate___feature_definition_sql(definition))

    return errors


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------

def parse_feature_dict(data: dict):
    """Parse a single feature dict (already YAML-loaded) into a FeatureDef or ExpressionFeatureDef."""
    from semantic.expression import ExpressionFeatureDef, parse_expression_str

    common = dict(
        feature_id=data["feature_id"],
        name=data["name"],
        entity_type=data["entity_type"],
        entity_key=data["entity_key"],
        data_type=data["data_type"],
        materialization_mode=data["materialization_mode"],
        description=data.get("description"),
        dependencies=data.get("dependencies", []),
        ttl_seconds=data.get("ttl_seconds"),
    )

    if "expr" in data:
        expr = parse_expression_str(str(data["expr"]))
        return ExpressionFeatureDef(expression=expr, **common)
    else:
        raw_sql = data.get("raw_sql") or data.get("definition") or ""
        return FeatureDef(raw_sql=raw_sql, **common)


def load_feature_yamls(features_dir: str | Path) -> list:
    """Load all feature YAML files from a directory.

    Returns a mixed list of FeatureDef (feature_type='raw_sql') and
    ExpressionFeatureDef (feature_type='expression') depending on which
    key is present in each YAML file.
    """
    features_dir = Path(features_dir)
    result = []
    for path in sorted(features_dir.glob("*.yaml")):
        with open(path) as f:
            data = yaml.safe_load(f)
        try:
            result.append(parse_feature_dict(data))
        except Exception:
            pass
    return result


# ---------------------------------------------------------------------------
# Feature SQL resolution
# ---------------------------------------------------------------------------

def _get_feature_sql(fd, entities: dict, all_features: dict | None = None, dialect: str = "sqlite") -> str:
    """Return the runnable SQL for a feature def.

    - raw_sql features: return fd.raw_sql directly.
    - expression features: compile the expression to SQL using the given dialect.
      all_features is required for window features (feature-to-feature references).
    """
    if getattr(fd, "feature_type", "raw_sql") == "expression":
        from engine.expression_compiler import compile_feature_to_sql
        return compile_feature_to_sql(fd, entities, features=all_features, dialect=dialect)
    return fd.raw_sql


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------

def _topo_sort(feature_defs: list[FeatureDef]) -> list[FeatureDef]:
    """Topological sort of feature defs by their dependencies field."""
    id_to_def = {fd.feature_id: fd for fd in feature_defs}
    visited: set[str] = set()
    result: list[FeatureDef] = []

    def _visit(feature_id: str) -> None:
        if feature_id in visited:
            return
        visited.add(feature_id)
        fd = id_to_def.get(feature_id)
        if fd is None:
            return
        for dep_id in fd.dependencies:
            _visit(dep_id)
        result.append(fd)

    for fd in feature_defs:
        _visit(fd.feature_id)

    return result


# ---------------------------------------------------------------------------
# Reconcile engine
# ---------------------------------------------------------------------------

def reconcile(
    feature_defs: list[FeatureDef],
    entities: dict,
    db_url: str,
    dry_run: bool = False,
    materialize_ids: set[str] | None = None,
) -> None:
    """Reconcile feature definitions and materialise feature values.

    Steps:
    1. create_feature_tables (idempotent)
    2. validate all definition SQL — abort on error
    3. load existing __feature_definition rows
    4. diff: find new / changed definitions
    5. for new/changed: increment version, upsert __feature_definition
    6. upsert __attribute_registry for all features (source_type='feature')
    7. upsert __attribute_registry for all base entity cols (source_type='column')
    8. topological sort feature_defs by .dependencies
    9. for each materialized feature (in order): execute definition SQL → UPSERT __feature_value

    --dry-run: print diff and which features would recompute; no writes.
    """
    path = db_url.removeprefix("sqlite:///")
    conn = sqlite3.connect(path)
    try:
        # 1. Create tables
        create_feature_tables(conn)

        # 2. Validate all definition SQL (raw_sql features only;
        #    expression features are validated by the compiler)
        for fd in feature_defs:
            if getattr(fd, "feature_type", "raw_sql") == "raw_sql":
                sql_errors = validate___feature_definition_sql(fd.raw_sql.strip())
                if sql_errors:
                    msg = "; ".join(e.message for e in sql_errors)
                    raise ValueError(
                        f"Feature '{fd.feature_id}' has invalid definition SQL: {msg}"
                    )

        # 3. Load existing __feature_definition rows
        existing: dict[str, dict] = {}
        for row in conn.execute(
            "SELECT feature_id, definition, version FROM __feature_definition"
        ):
            existing[row[0]] = {"definition": row[1], "version": row[2]}

        # Pre-compute the runnable SQL for each feature (compiles expressions).
        # Build a lookup by feature_id for cross-feature references (window features).
        features_by_id = {fd.feature_id: fd for fd in feature_defs}
        feature_sqls: dict[str, str] = {
            fd.feature_id: _get_feature_sql(fd, entities, features_by_id)
            for fd in feature_defs
        }

        # 4. Diff: find new / changed definitions
        new_or_changed: list[tuple] = []
        for fd in feature_defs:
            fd_sql = feature_sqls[fd.feature_id].strip()
            if fd.feature_id not in existing:
                new_or_changed.append((fd, True))
            elif existing[fd.feature_id]["definition"] != fd_sql:
                new_or_changed.append((fd, False))

        if dry_run:
            print("=== dry-run: no writes ===")
            for fd, is_new in new_or_changed:
                action = "NEW" if is_new else "CHANGED"
                print(f"  {action}: {fd.feature_id}")
            for fd in feature_defs:
                if fd.materialization_mode == "materialized":
                    print(f"  WOULD RECOMPUTE: {fd.feature_id}")
            return

        now = datetime.now(UTC).isoformat()

        # 5. Upsert __feature_definition for new/changed
        for fd, is_new in new_or_changed:
            new_version = 1 if is_new else (existing[fd.feature_id]["version"] + 1)
            fd_sql = feature_sqls[fd.feature_id].strip()
            conn.execute(
                """
                INSERT INTO __feature_definition (
                    feature_id, name, description, entity_type, entity_key,
                    data_type, definition, materialization_mode, ttl_seconds,
                    version, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(feature_id) DO UPDATE SET
                    name=excluded.name,
                    description=excluded.description,
                    definition=excluded.definition,
                    materialization_mode=excluded.materialization_mode,
                    ttl_seconds=excluded.ttl_seconds,
                    version=excluded.version
                """,
                (
                    fd.feature_id, fd.name, fd.description, fd.entity_type, fd.entity_key,
                    fd.data_type, fd_sql, fd.materialization_mode,
                    fd.ttl_seconds, new_version, now,
                ),
            )

        # 6. Upsert __attribute_registry for all features
        for fd in feature_defs:
            attr_name = fd.feature_id.split(".")[-1]
            conn.execute(
                """
                INSERT INTO __attribute_registry
                    (attribute_name, entity_type, source_type, source_ref, data_type)
                VALUES (?, ?, 'feature', ?, ?)
                ON CONFLICT(attribute_name, entity_type) DO UPDATE SET
                    source_type='feature',
                    source_ref=excluded.source_ref,
                    data_type=excluded.data_type
                """,
                (attr_name, fd.entity_type, fd.feature_id, fd.data_type),
            )

        # 7. Upsert __attribute_registry for all base entity cols
        for entity in entities.values():
            for attr in entity.all_base_columns:
                conn.execute(
                    """
                    INSERT INTO __attribute_registry
                        (attribute_name, entity_type, source_type, source_ref, data_type)
                    VALUES (?, ?, 'column', ?, ?)
                    ON CONFLICT(attribute_name, entity_type) DO UPDATE SET
                        source_type='column',
                        source_ref=excluded.source_ref,
                        data_type=excluded.data_type
                    """,
                    (attr.name, entity.name, attr.name, attr.col_type.value),
                )

        conn.commit()

        # 8. Topological sort
        sorted_defs = _topo_sort(feature_defs)

        # 9. Materialise each feature
        for fd in sorted_defs:
            if fd.materialization_mode != "materialized":
                continue
            if materialize_ids is not None and fd.feature_id not in materialize_ids:
                continue

            rows = list(conn.execute(feature_sqls[fd.feature_id].strip()))
            total = len(rows)
            null_count = sum(1 for _, v in rows if v is None)

            conn.executemany(
                """
                INSERT INTO __feature_value (feature_id, entity_id, value, computed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(feature_id, entity_id) DO UPDATE SET
                    value=excluded.value,
                    computed_at=excluded.computed_at
                """,
                [
                    (fd.feature_id, entity_id, None if value is None else str(value), now)
                    for entity_id, value in rows
                ],
            )
            conn.commit()

            null_rate = f"{null_count / total * 100:.1f}%" if total else "N/A"
            print(f"  {fd.feature_id}: {total} rows, null_rate={null_rate}")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Column/PK specs for feature store tables — shared by SQLite and CH paths
# ---------------------------------------------------------------------------

_FEATURE_TABLE_COLUMNS: dict[str, dict[str, str]] = {
    "__feature_definition": {
        "feature_id": "TEXT",
        "name": "TEXT",
        "description": "TEXT",
        "entity_type": "TEXT",
        "entity_key": "TEXT",
        "data_type": "TEXT",
        "definition": "TEXT",
        "materialization_mode": "TEXT",
        "ttl_seconds": "INTEGER",
        "version": "INTEGER",
        "created_at": "TEXT",
    },
    "__feature_value": {
        "feature_id": "TEXT",
        "entity_id": "TEXT",
        "value": "TEXT",
        "computed_at": "TEXT",
    },
    "__attribute_registry": {
        "attribute_name": "TEXT",
        "entity_type": "TEXT",
        "source_type": "TEXT",
        "source_ref": "TEXT",
        "data_type": "TEXT",
    },
}

_FEATURE_TABLE_PKS: dict[str, list[str]] = {
    "__feature_definition": ["feature_id"],
    "__feature_value": ["feature_id", "entity_id"],
    "__attribute_registry": ["attribute_name", "entity_type"],
}


def create_feature_tables_in_backend(backend) -> None:
    """Create feature store tables in any DBBackend (idempotent)."""
    for table, columns in _FEATURE_TABLE_COLUMNS.items():
        backend.create_table(table, columns, _FEATURE_TABLE_PKS[table])
        backend.add_columns(table, columns)


def reconcile_with_backend(
    feature_defs: list,
    entities: dict,
    backend,
    dry_run: bool = False,
    materialize_ids: set[str] | None = None,
) -> None:
    """Backend-agnostic feature reconcile: create tables, diff defs, compute values.

    Mirrors ``reconcile()`` but works with any DBBackend (SQLite or ClickHouse)
    instead of a raw sqlite3 connection.  Callers are responsible for calling
    ``backend.connect()`` / ``backend.close()`` around this function.

    Steps:
    1. create_feature_tables_in_backend (idempotent)
    2. Validate raw_sql features — abort on error
    3. Load existing __feature_definition rows
    4. Diff: find new / changed definitions
    5. (dry_run: print and return)
    6. Upsert new/changed __feature_definition rows
    7. Upsert __attribute_registry for all features + entity columns
    8. Topological sort
    9. Execute each materialized feature → bulk_upsert __feature_value
    """
    # 1. Ensure tables exist
    create_feature_tables_in_backend(backend)

    # 2. Validate raw_sql features
    for fd in feature_defs:
        if getattr(fd, "feature_type", "raw_sql") == "raw_sql":
            sql_errors = validate___feature_definition_sql(fd.raw_sql.strip())
            if sql_errors:
                msg = "; ".join(e.message for e in sql_errors)
                raise ValueError(
                    f"Feature '{fd.feature_id}' has invalid definition SQL: {msg}"
                )

    features_by_id = {fd.feature_id: fd for fd in feature_defs}
    feature_sqls: dict[str, str] = {
        fd.feature_id: _get_feature_sql(fd, entities, features_by_id, dialect=getattr(backend, "dialect", "sqlite"))
        for fd in feature_defs
    }

    # 3. Load existing __feature_definition rows
    existing: dict[str, dict] = {}
    try:
        rows = backend.execute(
            "SELECT feature_id, definition, version FROM __feature_definition"
        )
        for row in rows:
            existing[row[0]] = {"definition": row[1], "version": row[2]}
    except Exception:
        pass  # table empty on first run

    # 4. Diff
    new_or_changed: list[tuple] = []
    for fd in feature_defs:
        fd_sql = feature_sqls[fd.feature_id].strip()
        if fd.feature_id not in existing:
            new_or_changed.append((fd, True))
        elif existing[fd.feature_id]["definition"] != fd_sql:
            new_or_changed.append((fd, False))

    if dry_run:
        print("=== dry-run: no writes ===")
        for fd, is_new in new_or_changed:
            action = "NEW" if is_new else "CHANGED"
            print(f"  {action}: {fd.feature_id}")
        for fd in feature_defs:
            if fd.materialization_mode == "materialized":
                print(f"  WOULD RECOMPUTE: {fd.feature_id}")
        return

    now = datetime.now(UTC).isoformat()

    # 5. Upsert __feature_definition for new/changed
    for fd, is_new in new_or_changed:
        new_version = 1 if is_new else (existing[fd.feature_id]["version"] + 1)
        backend.upsert("__feature_definition", {
            "feature_id": fd.feature_id,
            "name": fd.name,
            "description": fd.description or "",
            "entity_type": fd.entity_type,
            "entity_key": fd.entity_key,
            "data_type": fd.data_type,
            "definition": feature_sqls[fd.feature_id].strip(),
            "materialization_mode": fd.materialization_mode,
            "ttl_seconds": fd.ttl_seconds,
            "version": new_version,
            "created_at": now,
        })

    # 6. Upsert __attribute_registry for features
    for fd in feature_defs:
        attr_name = fd.feature_id.split(".")[-1]
        backend.upsert("__attribute_registry", {
            "attribute_name": attr_name,
            "entity_type": fd.entity_type,
            "source_type": "feature",
            "source_ref": fd.feature_id,
            "data_type": fd.data_type,
        })

    # 7. Upsert __attribute_registry for entity columns
    for entity in entities.values():
        for attr in entity.all_base_columns:
            backend.upsert("__attribute_registry", {
                "attribute_name": attr.name,
                "entity_type": entity.name,
                "source_type": "column",
                "source_ref": attr.name,
                "data_type": attr.col_type.value,
            })

    # 8. Topological sort
    sorted_defs = _topo_sort(feature_defs)

    # 9. Compute and store feature values
    for fd in sorted_defs:
        if fd.materialization_mode != "materialized":
            continue
        if materialize_ids is not None and fd.feature_id not in materialize_ids:
            continue

        # Skip features whose entity table hasn't been materialized yet.
        # This happens when materializing one entity at a time — other entities'
        # features reference tables that don't exist in the sem backend yet.
        # Only check when the entity is present in the ontology (known table name);
        # skip the guard when entities dict is empty (e.g. in tests).
        entity = entities.get(fd.entity_type)
        if entity is not None and not backend.table_exists(entity.table_name):
            logger.warning(
                "Skipping feature %s — entity table %r not yet materialized",
                fd.feature_id, entity.table_name,
            )
            continue

        try:
            rows = backend.execute(feature_sqls[fd.feature_id].strip())
        except Exception as exc:
            # A joined table may not exist yet (entity not yet materialized).
            # Detect ClickHouse "Unknown table" (code 60) or any similar error
            # and skip gracefully — the feature will be computed on the next
            # full materialisation once all referenced tables exist.
            msg = str(exc)
            if "UNKNOWN_TABLE" in msg or "Unknown table" in msg or "code: 60" in msg:
                logger.warning(
                    "Skipping feature %s — a referenced table is not yet materialized: %s",
                    fd.feature_id, msg.split("\n")[0],
                )
                continue
            raise
        feature_rows = [
            {
                "feature_id": fd.feature_id,
                "entity_id": str(row[0]),
                "value": None if row[1] is None else str(row[1]),
                "computed_at": now,
            }
            for row in rows
        ]
        if hasattr(backend, "bulk_upsert"):
            backend.bulk_upsert("__feature_value", feature_rows)
        else:
            for r in feature_rows:
                backend.upsert("__feature_value", r)
        total = len(feature_rows)
        null_count = sum(1 for r in feature_rows if r["value"] is None)
        null_rate = f"{null_count / total * 100:.1f}%" if total else "N/A"
        print(f"  {fd.feature_id}: {total} rows, null_rate={null_rate}")
