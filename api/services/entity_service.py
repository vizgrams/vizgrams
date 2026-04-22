# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Entity service: ontology inspection and validation."""

import re
from pathlib import Path

import yaml

from core import metadata_db
from semantic.types import EntityDef
from semantic.yaml_adapter import YAMLAdapter


def _to_snake(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


def _entity_db_key(model_dir: Path, entity_name: str) -> str:
    """Return whichever DB key exists for this entity (PascalCase or snake_case).

    Entities created via the run.py pipeline are stored under their original name
    (e.g. "Airport"), while entities created via the API use snake_case ("airport").
    This function checks the DB and returns the key that actually exists, falling back
    to snake_case (the canonical write key) for new entities.
    """
    if metadata_db.get_current_content(model_dir, "entity", entity_name) is not None:
        return entity_name
    snake = _to_snake(entity_name)
    if metadata_db.get_current_content(model_dir, "entity", snake) is not None:
        return snake
    return entity_name  # default for new entities: keep caller's casing


def _load_entities(model_dir: Path) -> dict[str, EntityDef]:
    ontology_dir = model_dir / "ontology"
    return {e.name: e for e in YAMLAdapter.load_entities(ontology_dir)}


def _feature_counts_by_entity(model_dir: Path) -> dict[str, int]:
    features_dir = model_dir / "features"
    counts: dict[str, int] = {}
    for f in YAMLAdapter.load_features(features_dir):
        counts[f.entity_type] = counts.get(f.entity_type, 0) + 1
    return counts


def _features_for_entity(model_dir: Path, entity_name: str) -> list[dict]:
    results = []
    for name in metadata_db.list_artifact_names(model_dir, "feature"):
        content = metadata_db.get_current_content(model_dir, "feature", name)
        if not content:
            continue
        raw = yaml.safe_load(content)
        if raw.get("entity_type") != entity_name:
            continue
        results.append({
            "feature_id": raw.get("feature_id", name),
            "name": raw.get("name", name),
            "description": raw.get("description"),
            "data_type": raw.get("data_type", "FLOAT"),
            "expr": raw.get("expr", ""),
        })
    return results


def list_entities(model_dir: Path) -> list[dict]:
    entities = _load_entities(model_dir)
    feature_counts = _feature_counts_by_entity(model_dir)
    results = []
    for name, e in entities.items():
        stats = _entity_db_stats(model_dir, e.table_name)
        results.append({
            "name": name,
            "table_name": e.table_name,
            "attribute_count": len(e.attributes) if hasattr(e, "attributes") else 0,
            "relation_count": len(e.relations) if hasattr(e, "relations") else 0,
            "feature_count": feature_counts.get(name, 0),
            "row_count": stats["row_count"] if stats["present"] else None,
            "table_exists": stats["present"],
        })
    return results


def get_entity(model_dir: Path, entity_name: str) -> dict:
    entities = _load_entities(model_dir)
    if entity_name not in entities:
        raise KeyError(f"Entity '{entity_name}' not found in ontology.")
    e = entities[entity_name]

    attributes = []
    for attr in list(e.identity or []) + list(e.attributes if hasattr(e, "attributes") else []):
        attributes.append({
            "name": attr.name,
            "type": attr.col_type.value if hasattr(attr.col_type, "value") else str(attr.col_type),
            "semantic": attr.semantic.value if hasattr(attr.semantic, "value") else str(attr.semantic),
        })

    relations = []
    for rel in (e.relations if hasattr(e, "relations") else []):
        relations.append({
            "name": rel.name if hasattr(rel, "name") else None,
            "target": rel.target,
            "cardinality": rel.cardinality.value if hasattr(rel.cardinality, "value") else str(rel.cardinality),
            "via": rel.via if isinstance(rel.via, list) else ([rel.via] if rel.via else []),
        })

    raw_yaml = metadata_db.get_current_content(model_dir, "entity", _entity_db_key(model_dir, entity_name))

    return {
        "name": entity_name,
        "table_name": e.table_name,
        "attributes": attributes,
        "relations": relations,
        "features": _features_for_entity(model_dir, entity_name),
        "database": _entity_db_stats_out(model_dir, e.table_name),
        "display_list": list(e.display_list),
        "display_detail": list(e.display_detail),
        "display_order": [{"column": col, "direction": dir_} for col, dir_ in e.display_order],
        "raw_yaml": raw_yaml,
    }


def validate_entity(model_dir: Path, entity_name: str) -> dict:
    """Validate an entity YAML file using the ontology validator."""
    import tempfile

    from semantic.ontology import validate_ontology_yaml

    content = metadata_db.get_current_content(model_dir, "entity", _entity_db_key(model_dir, entity_name))
    if content is None:
        raise KeyError(f"No YAML found in DB for entity '{entity_name}'.")
    tmp_path = Path(tempfile.mktemp(suffix=".yaml"))
    try:
        tmp_path.write_text(content)
        errors = validate_ontology_yaml(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    # Also check that the entity loads cleanly and cross-references resolve
    try:
        entities = _load_entities(model_dir)
        if entity_name not in entities:
            errors_list = [{"path": "name", "message": f"Entity '{entity_name}' not found after loading."}]
            return {"valid": False, "errors": errors_list}
        e = entities[entity_name]
        for rel in (e.relations if hasattr(e, "relations") else []):
            if rel.dynamic_field is not None:
                continue  # dynamic relations have no static target
            if rel.target not in entities:
                errors.append(type("E", (), {
                    "path": f"relations[target={rel.target}]",
                    "message": f"Relation target '{rel.target}' not found in ontology.",
                })())
    except Exception as exc:
        return {"valid": False, "errors": [{"path": "", "message": str(exc)}]}

    return {
        "valid": len(errors) == 0,
        "errors": [{"path": e.path, "message": e.message} for e in errors],
    }


class EntityValidationError(Exception):
    """Raised when entity YAML fails schema or semantic validation."""
    def __init__(self, errors: list[dict]):
        self.errors = errors
        super().__init__(f"{len(errors)} validation error(s)")


def create_entity(model_dir: Path, data: dict) -> dict:
    """Write a new entity YAML to the DB and create the DB table. Raises FileExistsError if already present."""
    name = data["name"]

    if metadata_db.get_current_content(model_dir, "entity", _entity_db_key(model_dir, name)) is not None:
        raise FileExistsError(f"Entity '{name}' already exists.")

    _write_entity_yaml(model_dir, name, data)
    _materialize_entity(model_dir, name)
    return get_entity(model_dir, name)


def upsert_entity(model_dir: Path, entity_name: str, data: dict) -> dict:
    """Create or fully replace an entity YAML in DB and sync the DB table (additive ALTER only)."""
    name = data.get("name", entity_name)

    created = metadata_db.get_current_content(model_dir, "entity", _entity_db_key(model_dir, entity_name)) is None
    _write_entity_yaml(model_dir, entity_name, data)
    _materialize_entity(model_dir, name)
    return get_entity(model_dir, name), created


def create_entity_write_only(model_dir: Path, data: dict) -> None:
    """Validate and write new entity YAML to DB (no materialization). Raises on conflict/validation."""
    name = data["name"]
    if metadata_db.get_current_content(model_dir, "entity", _entity_db_key(model_dir, name)) is not None:
        raise FileExistsError(f"Entity '{name}' already exists.")
    errors = _validate_entity_data(data, model_dir)
    if errors:
        raise EntityValidationError(errors)
    _write_entity_yaml(model_dir, name, data)


def upsert_entity_write_only(model_dir: Path, entity_name: str, data: dict) -> None:
    """Validate and write/overwrite entity YAML in DB (no materialization)."""
    errors = _validate_entity_data(data, model_dir)
    if errors:
        raise EntityValidationError(errors)
    _write_entity_yaml(model_dir, entity_name, data)


def create_entity_async(model_dir: Path, data: dict, job_service) -> object:
    """Validate, write YAML to DB, then materialize in background. Returns the job."""
    name = data["name"]

    if metadata_db.get_current_content(model_dir, "entity", _entity_db_key(model_dir, name)) is not None:
        raise FileExistsError(f"Entity '{name}' already exists.")

    errors = _validate_entity_data(data, model_dir)
    if errors:
        raise EntityValidationError(errors)

    _write_entity_yaml(model_dir, name, data)
    return _submit_materialize_job(model_dir, name, job_service)


def upsert_entity_async(model_dir: Path, entity_name: str, data: dict, job_service) -> tuple:
    """Validate, write/overwrite YAML in DB, then materialize in background. Returns (job, created)."""
    name = data.get("name", entity_name)

    created = metadata_db.get_current_content(model_dir, "entity", _entity_db_key(model_dir, entity_name)) is None

    errors = _validate_entity_data(data, model_dir)
    if errors:
        raise EntityValidationError(errors)

    _write_entity_yaml(model_dir, entity_name, data)
    return _submit_materialize_job(model_dir, name, job_service), created


# ---------------------------------------------------------------------------
# Internal helpers for write path
# ---------------------------------------------------------------------------

def _validate_entity_data(data: dict, model_dir: Path) -> list[dict]:
    """Write data to a temp YAML, run schema + load validation, return error dicts."""
    import os
    import tempfile

    from semantic.ontology import validate_ontology_yaml

    tmp_fd, tmp_path_str = tempfile.mkstemp(suffix=".yaml")
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(tmp_fd, "w") as f:
            yaml.dump(
                _build_yaml_dict(data),
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
        schema_errors = validate_ontology_yaml(tmp_path)
        errors = [{"path": e.path, "message": e.message} for e in schema_errors]

        if not errors:
            try:
                from semantic.ontology import parse_entity_yaml
                entity_name = data.get("name", "")
                loaded_entity = parse_entity_yaml(tmp_path)
                if loaded_entity.name != entity_name:
                    errors.append({
                        "path": "name",
                        "message": f"Entity '{entity_name}' could not be loaded after validation.",
                    })
            except Exception as exc:
                errors.append({"path": "", "message": str(exc)})
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass

    return errors


def _build_yaml_dict(data: dict) -> dict:
    """Convert request body dict to the ontology YAML structure (without writing)."""
    out: dict = {"entity": data["name"]}
    if data.get("description"):
        out["description"] = data["description"]

    for block in ("identity", "attributes"):
        items = data.get(block) or {}
        if items:
            out[block] = {
                attr_name: _attr_to_yaml(attr)
                for attr_name, attr in items.items()
            }

    if data.get("history"):
        out["history"] = data["history"]
    if data.get("events"):
        out["events"] = data["events"]
    if data.get("relations"):
        out["relations"] = {
            rel_name: _rel_to_yaml(rel)
            for rel_name, rel in data["relations"].items()
        }
    return out


def _submit_materialize_job(model_dir: Path, entity_name: str, job_service) -> object:
    """Create a job and submit background materialize for all entities in this model."""
    from core.registry import append_job_audit
    job = job_service.create(
        model=model_dir.name,
        operation="materialize",
        entity=entity_name,
    )
    append_job_audit(model_dir, job)

    def _run():
        import logging
        try:
            _materialize_entity(model_dir, entity_name)
            job_service.complete(job.job_id, {"entity": entity_name, "status": "materialized"})
        except BaseException as exc:
            logging.getLogger(__name__).exception("Unhandled error in materialize job %s", job.job_id)
            job_service.fail(job.job_id, str(exc))
        finally:
            append_job_audit(model_dir, job_service.get(model_dir.name, job.job_id) or job)

    job_service.submit(_run)
    return job


def rematerialize_entity_async(model_dir: Path, entity_name: str, job_service) -> object:
    """Rematerialize a single entity's DB table without changing its YAML. Returns a background job."""
    entities = {e.name: e for e in YAMLAdapter.load_entities(model_dir / "ontology")}
    if entity_name not in entities:
        raise KeyError(f"Entity '{entity_name}' not found.")
    return _submit_materialize_job(model_dir, entity_name, job_service)


def reconcile_all_async(model_dir: Path, job_service) -> object:
    """Materialize all entities in the model's DB. Returns a background job."""
    from core.db import get_backend
    from core.registry import append_job_audit
    from semantic.materialize import materialize_with_backend

    ontology_dir = model_dir / "ontology"
    all_entities = YAMLAdapter.load_entities(ontology_dir)
    entity_names = [e.name for e in all_entities]

    job = job_service.create(
        model=model_dir.name,
        operation="reconcile_all",
        entity="*",
    )
    append_job_audit(model_dir, job)

    def _run():
        import logging
        try:
            backend = get_backend(model_dir)
            backend.connect()
            try:
                materialize_with_backend(all_entities, backend)
            finally:
                backend.close()
            job_service.complete(job.job_id, {"entities": entity_names, "status": "materialized"})
        except BaseException as exc:
            logging.getLogger(__name__).exception("Unhandled error in reconcile_all job %s", job.job_id)
            job_service.fail(job.job_id, str(exc))
        finally:
            append_job_audit(model_dir, job_service.get(model_dir.name, job.job_id) or job)

    job_service.submit(_run)
    return job


def _write_entity_yaml(model_dir: Path, entity_name: str, data: dict) -> None:
    """Serialise request body dict to the ontology YAML format and store in DB."""
    import io
    content = io.StringIO()
    yaml.dump(_build_yaml_dict(data), content, default_flow_style=False, sort_keys=False, allow_unicode=True)
    metadata_db.record_version(model_dir, "entity", _entity_db_key(model_dir, entity_name), content.getvalue())


def _attr_to_yaml(attr: dict) -> dict:
    d: dict = {"type": attr["type"]}
    if attr.get("semantic"):
        d["semantic"] = attr["semantic"]
    if attr.get("references"):
        d["references"] = attr["references"]
    return d


def _rel_to_yaml(rel: dict) -> dict:
    d: dict = {"target": rel["target"], "cardinality": rel["cardinality"]}
    if rel.get("via") is not None:
        d["via"] = rel["via"]
    if rel.get("description"):
        d["description"] = rel["description"]
    return d


def _materialize_entity(model_dir: Path, entity_name: str) -> None:
    """Load all entities and create/alter DB tables. Creates the DB file if absent."""
    from core.db import get_backend
    from semantic.materialize import materialize_with_backend

    ontology_dir = model_dir / "ontology"
    all_entities = YAMLAdapter.load_entities(ontology_dir)
    if not any(e.name == entity_name for e in all_entities):
        raise KeyError(f"Entity '{entity_name}' not found after writing YAML.")

    backend = get_backend(model_dir)
    backend.connect()
    try:
        materialize_with_backend(all_entities, backend)
    finally:
        backend.close()


def validate_all(model_dir: Path) -> list[dict]:
    """Validate all ontology artifacts in the DB; returns list of {file, valid, errors}."""
    import tempfile

    from semantic.ontology import validate_ontology_yaml

    results = []
    for name in metadata_db.list_artifact_names(model_dir, "entity"):
        content = metadata_db.get_current_content(model_dir, "entity", name)
        tmp_path = Path(tempfile.mktemp(suffix=".yaml"))
        try:
            tmp_path.write_text(content)
            errors = validate_ontology_yaml(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)
        results.append({
            "file": f"{name}.yaml",
            "valid": len(errors) == 0,
            "errors": [{"path": e.path, "message": e.message, "rule": getattr(e, "rule", "")} for e in errors],
        })
    return results


def list_entities_full(model_dir: Path) -> list[dict]:
    """List all entities with full attribute/relation detail for JSON output."""
    entities = _load_entities(model_dir)
    result = []
    for name, e in entities.items():
        result.append({
            "name": name,
            "table_name": e.table_name,
            "description": getattr(e, "description", None) or "",
            "identity": [
                {
                    "name": a.name,
                    "type": a.col_type.value if hasattr(a.col_type, "value") else str(a.col_type),
                    "semantic": a.semantic.value if hasattr(a.semantic, "value") else str(a.semantic),
                    "references": getattr(a, "references", None),
                }
                for a in (getattr(e, "identity", None) or [])
            ],
            "attributes": [
                {
                    "name": a.name,
                    "type": a.col_type.value if hasattr(a.col_type, "value") else str(a.col_type),
                    "semantic": a.semantic.value if hasattr(a.semantic, "value") else str(a.semantic),
                }
                for a in (getattr(e, "attributes", None) or [])
            ],
            "relations": [
                {
                    "name": getattr(r, "name", None),
                    "target": r.target,
                    "cardinality": r.cardinality.value if hasattr(r.cardinality, "value") else str(r.cardinality),
                }
                for r in (getattr(e, "relations", None) or [])
            ],
            "has_history": getattr(e, "history", None) is not None,
            "events": [ev.name for ev in (getattr(e, "events", None) or [])],
        })
    return result


def get_entity_relations(model_dir: Path, entity_name: str) -> dict:
    """Return outgoing and incoming relations for an entity (for inspect --relations)."""
    entities = _load_entities(model_dir)
    if entity_name not in entities:
        raise KeyError(f"Entity '{entity_name}' not found.")
    e = entities[entity_name]

    outgoing = [
        {
            "name": getattr(r, "name", None),
            "target": r.target,
            "cardinality": r.cardinality.value if hasattr(r.cardinality, "value") else str(r.cardinality),
        }
        for r in (getattr(e, "relations", None) or [])
    ]

    incoming = []
    for other_name, other in entities.items():
        if other_name == entity_name:
            continue
        for rel in (getattr(other, "relations", None) or []):
            if rel.target == entity_name:
                incoming.append({"from": other_name, "relation": getattr(rel, "name", None)})

    return {
        "name": entity_name,
        "table_name": e.table_name,
        "description": getattr(e, "description", None),
        "outgoing": outgoing,
        "incoming": incoming,
    }


def save_feature_expr(model_dir: Path, feature_id: str, expr: str) -> dict:
    """Update the expr field of a feature in the DB."""
    content = metadata_db.get_current_content(model_dir, "feature", feature_id)
    if content is None:
        raise KeyError(f"Feature '{feature_id}' not found.")
    data = yaml.safe_load(content)
    data["expr"] = expr
    new_content = yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
    metadata_db.record_version(model_dir, "feature", feature_id, new_content)
    return {
        "feature_id": feature_id,
        "name": data.get("name", feature_id),
        "description": data.get("description"),
        "data_type": data.get("data_type", "FLOAT"),
        "expr": expr,
    }


def get___feature_values_for_entity(model_dir: Path, entity_name: str) -> dict:
    """Return {entity_id: {feature_id: value}} for all computed feature values of an entity."""
    features = _features_for_entity(model_dir, entity_name)
    if not features:
        return {}
    feature_ids = [f["feature_id"] for f in features]
    result: dict = {}
    try:
        from core.db import get_backend
        backend = get_backend(model_dir, namespace="sem")
        backend.connect()
        try:
            if not backend.table_exists("__feature_value"):
                return {}
            placeholders = ",".join(["?"] * len(feature_ids))
            rows = backend.execute(
                f"SELECT entity_id, feature_id, value FROM __feature_value"
                f" WHERE feature_id IN ({placeholders})",
                feature_ids,
            )
        finally:
            backend.close()
        for row in rows:
            entity_id, feature_id, value = row[0], row[1], row[2]
            if entity_id not in result:
                result[entity_id] = {}
            result[entity_id][feature_id] = value
    except Exception:
        pass  # __feature_value table may not exist
    return result


def save_entity_yaml(model_dir: Path, entity_name: str, content: str) -> dict:
    """Validate raw YAML and store the entity in the metadata DB (no materialization)."""
    import os
    import tempfile

    from semantic.ontology import validate_all_entities, validate_ontology_yaml

    tmp_fd, tmp_path_str = tempfile.mkstemp(suffix=".yaml")
    tmp_path = Path(tmp_path_str)
    errors = []
    try:
        with os.fdopen(tmp_fd, "w") as f:
            f.write(content)
        errors = validate_ontology_yaml(tmp_path)
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass

    if errors:
        raise EntityValidationError([{"path": e.path, "message": e.message} for e in errors])

    # Cross-entity validation: load all entities (substituting the new content for
    # this one) and run validate_all_entities to catch e.g. via_target column mismatches.
    all_entities = list(_load_entities(model_dir).values())
    import yaml as _yaml
    new_entity_data = _yaml.safe_load(content)
    from semantic.ontology import parse_entity_dict as _parse_entity_dict
    try:
        new_entity = _parse_entity_dict(new_entity_data)
        # Replace existing entry (if any) with the candidate
        all_entities = [e for e in all_entities if e.name != new_entity.name] + [new_entity]
        cross_errors = validate_all_entities(all_entities)
        # Report only errors touching this entity to avoid surfacing pre-existing issues
        cross_errors = [e for e in cross_errors if entity_name in e.path]
        if cross_errors:
            raise EntityValidationError([{"path": e.path, "message": e.message} for e in cross_errors])
    except EntityValidationError:
        raise
    except Exception:
        pass  # parsing failure already caught above

    metadata_db.record_version(model_dir, "entity", _entity_db_key(model_dir, entity_name), content)
    return get_entity(model_dir, entity_name)


def _entity_db_stats_out(model_dir: Path, table_name: str) -> dict:
    """Return DB stats with keys matching the EntityDetail.database schema."""
    raw = _entity_db_stats(model_dir, table_name)
    return {"present": raw["present"], "row_count": raw["row_count"], "last_updated_at": raw.get("last_updated_at")}


def _entity_db_stats(model_dir: Path, table_name: str) -> dict:
    from core.db import get_backend
    stats: dict = {"present": False, "row_count": 0, "last_updated_at": None}
    try:
        backend = get_backend(model_dir)
        backend.connect()
        try:
            if not backend.table_exists(table_name):
                return stats
            stats["present"] = True
            rows = backend.execute(f"SELECT COUNT(*) FROM {table_name}")
            stats["row_count"] = rows[0][0] if rows else 0
        finally:
            backend.close()
    except Exception:
        pass
    return stats
