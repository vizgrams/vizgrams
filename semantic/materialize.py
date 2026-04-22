# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Backend-agnostic materializer for the semantic layer."""

from __future__ import annotations

from semantic.types import ColumnType, EntityDef, SemanticHint

# ---------------------------------------------------------------------------
# Backend-agnostic materialize (works for SQLite, DuckDB, ClickHouse, etc.)
# ---------------------------------------------------------------------------

_BACKEND_TYPE_MAP = {
    ColumnType.STRING: "TEXT",
    ColumnType.INTEGER: "INTEGER",
    ColumnType.FLOAT: "FLOAT",
}


def _entity_table_specs(
    entity: EntityDef,
    all_entities: list[EntityDef] | None = None,
) -> list[tuple[str, dict[str, str], list[str], dict[str, str], list[str] | None]]:
    """Return [(table_name, columns_dict, primary_keys, foreign_keys, order_by)] for an entity's tables.

    primary_keys:
      - UPSERT entities (no SCD2 history): entity's PK column — enforces uniqueness,
        and satisfies SQLite FK references from other tables.
      - SCD2 entities: empty list — multiple rows per key are required for history
        tracking, so no UNIQUE constraint is added.

    foreign_keys:
      - Only generated for RELATION columns that reference UPSERT entities.
        SCD2 targets allow multiple rows per key; their PK column is never unique,
        so referencing it would raise "foreign key mismatch" in SQLite.

    order_by:
      - SCD2 entities: [pk_col, valid_from_col] — ClickHouse ORDER BY so that
        re-inserting a closed row (same key + valid_from, higher _version) correctly
        supersedes the open row via ReplacingMergeTree deduplication.
      - All others: None (backend picks its own ordering strategy).
    """
    specs = []
    pk = entity.primary_key

    # UPSERT entities get a PK/UNIQUE constraint; SCD2 entities must not.
    is_scd2 = entity.history is not None
    base_pks = [] if is_scd2 else ([pk.name] if pk else [])

    # SCD2 ORDER BY hint for ClickHouse: (entity_key, valid_from)
    scd2_order_by: list[str] | None = None
    if is_scd2 and pk:
        scd_from_col = next(
            (c for c in entity.history.columns if c.semantic == SemanticHint.SCD_FROM),
            None,
        )
        if scd_from_col:
            scd2_order_by = [pk.name, scd_from_col.name]

    base_cols = {
        attr.name: _BACKEND_TYPE_MAP[attr.col_type]
        for attr in entity.all_base_columns
    }

    # FK constraints: only for RELATION columns referencing UPSERT entities.
    base_fks: dict[str, str] = {}
    if all_entities:
        upsert_set = {e.name for e in all_entities if not e.history}
        for attr in entity.all_base_columns:
            if attr.semantic == SemanticHint.RELATION and attr.references and attr.references in upsert_set:
                    ref = next(e for e in all_entities if e.name == attr.references)
                    if ref.primary_key:
                        base_fks[attr.name] = (
                            f"{ref.table_name}({ref.primary_key.name})"
                        )

    specs.append((entity.table_name, base_cols, base_pks, base_fks, scd2_order_by))

    # Event tables — no PK constraint: multiple events share the same parent key.
    for event in entity.events:
        event_cols = {
            attr.name: _BACKEND_TYPE_MAP[attr.col_type]
            for attr in entity.event_columns(event)
        }
        specs.append((entity.event_table_name(event), event_cols, [], {}, None))

    return specs


def materialize_with_backend(
    entities: list[EntityDef],
    backend,  # DBBackend — avoid circular import
) -> list[str]:
    """Create or migrate tables using a DBBackend instance.

    Uses backend.create_table (IF NOT EXISTS) then backend.add_columns
    (IF NOT EXISTS) to handle both initial creation and schema migrations
    without SQLAlchemy.

    Returns list of table names touched.
    """
    created: list[str] = []
    for entity in entities:
        for table_name, columns, primary_keys, foreign_keys, order_by in _entity_table_specs(
            entity, all_entities=entities
        ):
            backend.create_table(table_name, columns, primary_keys, foreign_keys or None, order_by)
            backend.add_columns(table_name, columns)
            created.append(table_name)
    return created
