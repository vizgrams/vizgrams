# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Explore service: fetch entity records and traverse relationships by ID."""

from pathlib import Path

from core.db import DBBackend, get_backend
from semantic.types import Cardinality
from semantic.yaml_adapter import YAMLAdapter


def _load_entities(model_dir: Path) -> dict:
    ontology_dir = model_dir / "ontology"
    return {e.name: e for e in YAMLAdapter.load_entities(ontology_dir)}


def _current_filter(entity) -> str:
    """Return a WHERE/AND clause fragment that restricts to current SCD2 rows, or empty string."""
    return " AND valid_to IS NULL" if entity.history else ""


def _open_backend(model_dir: Path) -> DBBackend:
    backend = get_backend(model_dir)
    backend.connect()
    return backend


def get_entity_record(model_dir: Path, entity_name: str, entity_id: str) -> dict:
    """Return a single entity record by primary key, with relationship stubs."""
    entities = _load_entities(model_dir)
    entity = entities.get(entity_name)
    if entity is None:
        raise KeyError(f"Entity '{entity_name}' not found.")

    pk = entity.primary_key
    if pk is None:
        raise ValueError(f"Entity '{entity_name}' has no PRIMARY_KEY.")

    backend = _open_backend(model_dir)
    try:
        rows = backend.execute(
            f"SELECT * FROM {entity.table_name} WHERE {pk.name} = ?{_current_filter(entity)}",
            (entity_id,),
        )
        columns = backend.last_columns
        if not rows:
            raise KeyError(f"{entity_name} '{entity_id}' not found.")
        row = dict(zip(columns, rows[0]))

        __feature_values: dict = {}
        try:
            fv_rows = backend.execute(
                "SELECT feature_id, value, computed_at FROM __feature_value WHERE entity_id = ?",
                (entity_id,),
            )
            for fid, val, computed_at in fv_rows:
                __feature_values[fid] = {"value": val, "computed_at": computed_at}
        except Exception:
            pass  # __feature_value table may not exist

        relationship_stubs = {}
        for rel in entity.relations:
            if rel.dynamic_field:
                continue  # skip dynamic relations — target unknown without data
            if rel.cardinality in (Cardinality.MANY_TO_ONE, Cardinality.ONE_TO_ONE):
                via_col = rel.via if isinstance(rel.via, str) else (rel.via[0] if rel.via else None)
                relationship_stubs[rel.name] = {
                    "target": rel.target,
                    "cardinality": rel.cardinality.value,
                    "id": row.get(via_col) if via_col else None,
                }
            else:  # ONE_TO_MANY / MANY_TO_MANY
                target_entity = entities.get(rel.target)
                count = None
                if target_entity and rel.via:
                    # via_target (from "local_col > target_col" syntax) gives the FK column
                    # on the target table; otherwise via IS the target column name.
                    via_cols = rel.via if isinstance(rel.via, list) else [rel.via]
                    target_cols = (
                        [rel.via_target] if rel.via_target
                        else via_cols
                    )
                    where_parts, params = [], []
                    for src_col, tgt_col in zip(via_cols, target_cols):
                        val = row.get(src_col)
                        if val is not None:
                            where_parts.append(f"{tgt_col} = ?")
                            params.append(val)
                    if where_parts:
                        count_rows = backend.execute(
                            f"SELECT COUNT(*) FROM {target_entity.table_name}"
                            f" WHERE {' AND '.join(where_parts)}{_current_filter(target_entity)}",
                            params,
                        )
                        count = count_rows[0][0] if count_rows else 0
                relationship_stubs[rel.name] = {
                    "target": rel.target,
                    "cardinality": rel.cardinality.value,
                    "count": count,
                }
    finally:
        backend.close()

    return {
        "entity": entity_name,
        "id": entity_id,
        "properties": row,
        "relationships": relationship_stubs,
        "__feature_values": __feature_values,
    }


def get_related_entities(
    model_dir: Path,
    entity_name: str,
    entity_id: str,
    relationship_name: str,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """Return the entities related to a given record via a named relationship."""
    entities = _load_entities(model_dir)
    source_entity = entities.get(entity_name)
    if source_entity is None:
        raise KeyError(f"Entity '{entity_name}' not found.")

    rel = next((r for r in source_entity.relations if r.name == relationship_name), None)
    if rel is None:
        raise KeyError(f"Relationship '{relationship_name}' not found on '{entity_name}'.")

    target_entity = entities.get(rel.target)
    if target_entity is None:
        raise KeyError(f"Target entity '{rel.target}' not found.")

    pk = source_entity.primary_key
    if pk is None:
        raise ValueError(f"Entity '{entity_name}' has no PRIMARY_KEY.")

    backend = _open_backend(model_dir)
    try:
        # Fetch the source record to resolve FK values
        src_rows = backend.execute(
            f"SELECT * FROM {source_entity.table_name} WHERE {pk.name} = ?{_current_filter(source_entity)}",
            (entity_id,),
        )
        if not src_rows:
            raise KeyError(f"{entity_name} '{entity_id}' not found.")
        source_row = dict(zip(backend.last_columns, src_rows[0]))

        if rel.cardinality in (Cardinality.MANY_TO_ONE, Cardinality.ONE_TO_ONE):
            # via is a FK col on the source; target record has that as its PK
            via_col = rel.via if isinstance(rel.via, str) else (rel.via[0] if rel.via else None)
            target_id = source_row.get(via_col) if via_col else None
            target_pk = target_entity.primary_key
            if not target_pk or target_id is None:
                all_rows, columns = [], []
            else:
                all_rows_raw = backend.execute(
                    f"SELECT * FROM {target_entity.table_name} WHERE {target_pk.name} = ?"
                    f"{_current_filter(target_entity)}",
                    (target_id,),
                )
                columns = backend.last_columns
                all_rows = [list(r) for r in all_rows_raw]
        else:
            # ONE_TO_MANY / MANY_TO_MANY: the target column(s) are either via_target
            # (when "local_col > target_col" syntax was used) or via itself.
            # The value to match comes from the source row's via column (or PK when
            # no via_target mapping — the original simple case).
            via_cols = rel.via if isinstance(rel.via, list) else ([rel.via] if rel.via else [])
            where_parts, params = [], []
            for vc in via_cols:
                if rel.via_target:
                    # Mapped: source row's vc value → target table's via_target column
                    val = source_row.get(vc)
                    if val is not None:
                        where_parts.append(f"{rel.via_target} = ?")
                        params.append(val)
                else:
                    # Simple: via col is on the target; match source PK
                    source_pk_val = source_row.get(pk.name)
                    if source_pk_val is not None:
                        where_parts.append(f"{vc} = ?")
                        params.append(source_pk_val)
            if not where_parts:
                all_rows, columns = [], []
            else:
                all_rows_raw = backend.execute(
                    f"SELECT * FROM {target_entity.table_name}"
                    f" WHERE {' AND '.join(where_parts)}{_current_filter(target_entity)}",
                    params,
                )
                columns = backend.last_columns
                all_rows = [list(r) for r in all_rows_raw]
    finally:
        backend.close()

    total = len(all_rows)
    page = all_rows[offset: offset + limit]

    target_pk_name = target_entity.primary_key.name if target_entity.primary_key else None

    return {
        "entity": entity_name,
        "id": entity_id,
        "relationship": relationship_name,
        "target": rel.target,
        "target_pk": target_pk_name,
        "cardinality": rel.cardinality.value,
        "columns": columns,
        "rows": page,
        "row_count": len(page),
        "total_row_count": total,
        "truncated": (offset + limit) < total,
    }
