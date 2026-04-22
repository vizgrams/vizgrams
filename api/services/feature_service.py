# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Feature service: list and inspect feature definitions."""

from pathlib import Path

import yaml

from core import metadata_db
from semantic.yaml_adapter import YAMLAdapter


def list_features(model_dir: Path, entity_name: str) -> list[dict]:
    features_dir = model_dir / "features"
    result = []
    for fd in YAMLAdapter.load_features(features_dir):
        if fd.entity_type != entity_name:
            continue
        feat_name = fd.feature_id.split(".")[-1]
        item: dict = {
            "name": feat_name,
            "entity": entity_name,
            "feature_type": getattr(fd, "feature_type", "raw_sql"),
        }
        if hasattr(fd, "description") and fd.description:
            item["description"] = fd.description
        result.append(item)
    return result


def get_feature(model_dir: Path, entity_name: str, feature_name: str) -> dict:
    features_dir = model_dir / "features"

    for fd in YAMLAdapter.load_features(features_dir):
        if fd.entity_type != entity_name:
            continue
        if fd.feature_id.split(".")[-1] != feature_name:
            continue

        feature_type = getattr(fd, "feature_type", "raw_sql")
        item: dict = {
            "name": feature_name,
            "entity": entity_name,
            "feature_type": feature_type,
        }

        if hasattr(fd, "description") and fd.description:
            item["description"] = fd.description

        if feature_type == "expression":
            item["expression"] = _expr_to_str(fd)
            item["compiled_sql"] = _compile_feature(fd, model_dir)
        elif hasattr(fd, "raw_sql") and fd.raw_sql:
            item["raw_sql"] = fd.raw_sql

        return item

    raise KeyError(f"Feature '{feature_name}' not found for entity '{entity_name}'.")


def list_all_features(model_dir: Path) -> list[dict]:
    """List all features across every entity."""
    result = []
    for name in metadata_db.list_artifact_names(model_dir, "feature"):
        content = metadata_db.get_current_content(model_dir, "feature", name)
        if not content:
            continue
        raw = yaml.safe_load(content)
        if not raw.get("feature_id"):
            continue
        item: dict = {
            "feature_id": raw["feature_id"],
            "name": raw.get("name", raw["feature_id"].split(".")[-1]),
            "entity": raw.get("entity_type", ""),
            "feature_type": raw.get("feature_type", "raw_sql"),
            "expr": raw.get("expr", raw.get("raw_sql", "")),
            "raw_yaml": content,
        }
        if raw.get("description"):
            item["description"] = raw["description"]
        if raw.get("data_type"):
            item["data_type"] = raw["data_type"]
        result.append(item)
    return result


class FeatureValidationError(Exception):
    """Raised when feature YAML fails schema validation."""
    def __init__(self, errors: list[dict]):
        self.errors = errors
        super().__init__(f"{len(errors)} validation error(s)")


def create_or_replace_feature(
    model_dir: Path, entity_name: str, feature_name: str, content: str
) -> dict:
    """Validate YAML content and write feature to DB.

    Two invariants enforced here regardless of what the UI sends:
    1. entity_key is corrected to the entity's actual primary-key column from
       the ontology, so a hardcoded 'id' placeholder never reaches disk.
    2. The canonical feature_id is used as the DB key.
    """
    import os
    import re
    import tempfile

    from semantic.feature import validate_feature_yaml
    from semantic.types import SemanticHint

    # Derive the canonical feature_id and correct entity_key from the ontology.
    canonical_feature_id = f"{entity_name.lower()}.{feature_name}"
    entities = _load_entities(model_dir)
    ent = entities.get(entity_name)
    if ent:
        pk_attr = next(
            (a for a in ent.identity if a.semantic == SemanticHint.PRIMARY_KEY), None
        )
        if pk_attr:
            content = re.sub(
                r"^entity_key:.*$", f"entity_key: {pk_attr.name}", content, flags=re.MULTILINE
            )

    tmp_fd, tmp_path_str = tempfile.mkstemp(suffix=".yaml")
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(tmp_fd, "w") as f:
            f.write(content)
        errors = validate_feature_yaml(tmp_path)
        if errors:
            raise FeatureValidationError(
                [{"path": e.path, "message": e.message} for e in errors]
            )
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass

    metadata_db.record_version(model_dir, "feature", canonical_feature_id, content)
    return get_feature(model_dir, entity_name, feature_name)


def save_feature_by_id(model_dir: Path, feature_id: str, content: str) -> dict:
    """Overwrite feature content in the DB by feature_id."""
    if metadata_db.get_current_content(model_dir, "feature", feature_id) is None:
        raise KeyError(f"Feature '{feature_id}' not found.")
    metadata_db.record_version(model_dir, "feature", feature_id, content)
    updated = yaml.safe_load(content)
    return {
        "feature_id": updated.get("feature_id", feature_id),
        "name": updated.get("name", feature_id.split(".")[-1]),
        "entity": updated.get("entity_type", ""),
        "feature_type": updated.get("feature_type", "raw_sql"),
        "expr": updated.get("expr", updated.get("raw_sql", "")),
        "description": updated.get("description"),
        "data_type": updated.get("data_type"),
        "raw_yaml": content,
    }


def _load_entities(model_dir: Path) -> dict:
    ontology_dir = model_dir / "ontology"
    return {e.name: e for e in YAMLAdapter.load_entities(ontology_dir)}


def _to_snake(name: str) -> str:
    import re
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


def reconcile_features(model_dir: Path, dry_run: bool = False) -> dict:
    """Run feature reconciliation; returns {count, dry_run}."""
    from core.db import get_backend
    from semantic.feature import reconcile_with_backend

    features_dir = model_dir / "features"
    ontology_dir = model_dir / "ontology"

    feature_defs = YAMLAdapter.load_features(features_dir)
    if not feature_defs:
        return {"count": 0, "dry_run": dry_run}

    entities = {e.name: e for e in YAMLAdapter.load_entities(ontology_dir)}
    backend = get_backend(model_dir, namespace="sem")
    backend.connect()
    try:
        reconcile_with_backend(feature_defs, entities, backend, dry_run=dry_run)
    finally:
        backend.close()
    return {"count": len(feature_defs), "dry_run": dry_run}


def reconcile_all_features(
    model_dir: Path,
    entity_name: str | None,
    job_service,
) -> object:
    """Start a background job to reconcile all features, optionally filtered to one entity."""
    features_dir = model_dir / "features"
    all_feature_defs = YAMLAdapter.load_features(features_dir)
    if not all_feature_defs:
        raise KeyError("No features found in this model.")

    if entity_name is not None:
        materialize_ids: set[str] | None = {
            fd.feature_id for fd in all_feature_defs if fd.entity_type == entity_name
        }
        if not materialize_ids:
            raise KeyError(f"No features found for entity '{entity_name}'.")
    else:
        materialize_ids = None

    scope = entity_name or "all entities"
    n = len(materialize_ids) if materialize_ids is not None else len(all_feature_defs)

    job = job_service.create(
        model=model_dir.name,
        operation="reconcile",
        entity=entity_name,
    )
    from core.registry import append_job_audit
    append_job_audit(model_dir, job)

    def _run():
        import logging
        import time

        from core.db import get_backend
        from semantic.feature import reconcile_with_backend

        try:
            ontology_dir = model_dir / "ontology"
            entities = {e.name: e for e in YAMLAdapter.load_entities(ontology_dir)}
            job_service.update_progress(
                job.job_id, f"reconciling {n} feature(s) for {scope}"
            )
            t0 = time.time()
            backend = get_backend(model_dir, namespace="sem")
            backend.connect()
            try:
                reconcile_with_backend(
                    all_feature_defs, entities, backend,
                    materialize_ids=materialize_ids,
                )
            finally:
                backend.close()
            elapsed = round(time.time() - t0, 1)
            job_service.complete(
                job.job_id,
                {"features_reconciled": n, "duration_s": elapsed},
            )
        except BaseException as exc:
            logging.getLogger(__name__).exception(
                "Unhandled error in reconcile-all job %s", job.job_id
            )
            job_service.fail(job.job_id, str(exc))
        finally:
            from core.registry import append_job_audit
            append_job_audit(
                model_dir, job_service.get(model_dir.name, job.job_id) or job
            )

    job_service.submit(_run)
    return job


def reconcile_entity_feature(
    model_dir: Path,
    entity_name: str,
    feature_name: str,
    job_service,
) -> object:
    """Start a background job to reconcile features for a given entity (and optionally a single feature).

    feature_name='*' reconciles all features for the entity.
    Otherwise reconciles only the named feature.
    """
    # Load all feature defs upfront to validate the requested feature exists.
    features_dir = model_dir / "features"
    all_feature_defs = YAMLAdapter.load_features(features_dir)

    entity_feature_ids = {
        fd.feature_id
        for fd in all_feature_defs
        if fd.entity_type == entity_name
    }
    if not entity_feature_ids:
        raise KeyError(f"No features found for entity '{entity_name}'.")

    if feature_name == "*":
        materialize_ids = entity_feature_ids
    else:
        matching = {
            fd.feature_id
            for fd in all_feature_defs
            if fd.entity_type == entity_name and fd.feature_id.split(".")[-1] == feature_name
        }
        if not matching:
            raise KeyError(f"Feature '{feature_name}' not found for entity '{entity_name}'.")
        materialize_ids = matching

    job = job_service.create(
        model=model_dir.name,
        operation="reconcile",
        entity=entity_name,
    )
    from core.registry import append_job_audit
    append_job_audit(model_dir, job)

    def _run():
        import logging
        import time

        from core.db import get_backend
        from semantic.feature import reconcile_with_backend

        try:
            ontology_dir = model_dir / "ontology"
            entities = {e.name: e for e in YAMLAdapter.load_entities(ontology_dir)}
            n = len(materialize_ids)
            job_service.update_progress(
                job.job_id,
                f"reconciling {n} feature(s) for {entity_name}",
            )
            t0 = time.time()
            backend = get_backend(model_dir, namespace="sem")
            backend.connect()
            try:
                reconcile_with_backend(
                    all_feature_defs, entities, backend,
                    materialize_ids=materialize_ids,
                )
            finally:
                backend.close()
            elapsed = round(time.time() - t0, 1)
            job_service.complete(
                job.job_id,
                {"features_reconciled": n, "duration_s": elapsed},
            )
        except BaseException as exc:
            logging.getLogger(__name__).exception(
                "Unhandled error in reconcile job %s", job.job_id
            )
            job_service.fail(job.job_id, str(exc))
        finally:
            from core.registry import append_job_audit
            append_job_audit(
                model_dir, job_service.get(model_dir.name, job.job_id) or job
            )

    job_service.submit(_run)
    return job


def validate_all(model_dir: Path) -> list[dict]:
    """Validate all feature artifacts in the DB; returns list of {file, valid, errors}."""
    import tempfile
    from semantic.feature import validate_feature_yaml

    results = []
    for name in metadata_db.list_artifact_names(model_dir, "feature"):
        content = metadata_db.get_current_content(model_dir, "feature", name)
        tmp_path = Path(tempfile.mktemp(suffix=".yaml"))
        try:
            tmp_path.write_text(content)
            errors = validate_feature_yaml(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)
        results.append({
            "file": f"{name}.yaml",
            "valid": len(errors) == 0,
            "errors": [{"path": e.path, "message": e.message, "rule": getattr(e, "rule", "")} for e in errors],
        })
    return results


def _expr_to_str(fd) -> str | None:
    if hasattr(fd, "expression") and fd.expression is not None:
        try:
            return str(fd.expression)
        except Exception:
            pass
    return None


def _compile_feature(fd, model_dir: Path) -> str | None:
    try:
        from core.model_config import load_database_config
        from engine.expression_compiler import compile_feature_to_sql
        from semantic.yaml_adapter import YAMLAdapter

        dialect = load_database_config(model_dir).get("backend", "clickhouse")
        ontology_dir = model_dir / "ontology"
        entities = {e.name: e for e in YAMLAdapter.load_entities(ontology_dir)}
        features = _load_features_by_entity(model_dir)
        return compile_feature_to_sql(fd, entities, features=features, dialect=dialect)
    except Exception:
        return None


def _load_features_by_entity(model_dir: Path) -> dict:
    features_dir = model_dir / "features"
    result: dict = {}
    for fd in YAMLAdapter.load_features(features_dir):
        attr_name = fd.feature_id.split(".")[-1]
        result.setdefault(fd.entity_type, {})[attr_name] = fd
    return result
