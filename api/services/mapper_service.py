# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Mapper service: mapper inspection, validation and execution."""

import logging
from pathlib import Path

from core import metadata_db
from semantic.mapper import MapperConfig
from semantic.yaml_adapter import YAMLAdapter

_log = logging.getLogger(__name__)


def _find_mapper_for_entity(model_dir: Path, entity_name: str) -> MapperConfig | None:
    """Find the mapper that writes to the given entity (first match)."""
    mappers_dir = model_dir / "mappers"
    for mc in YAMLAdapter.load_mappers(mappers_dir):
        for target in (mc.targets if hasattr(mc, "targets") else []):
            if getattr(target, "entity_name", None) == entity_name:
                return mc
    return None


def list_mappers(model_dir: Path) -> list[dict]:
    """List all mappers with basic info."""
    mappers_dir = model_dir / "mappers"
    return [_mapper_to_dict(mc, model_dir) for mc in YAMLAdapter.load_mappers(mappers_dir)]


def get_mapper_by_name(model_dir: Path, mapper_name: str) -> dict:
    """Get full mapper detail by mapper name (not entity name)."""
    mappers_dir = model_dir / "mappers"
    for mc in YAMLAdapter.load_mappers(mappers_dir):
        if mc.name == mapper_name:
            return _mapper_full_detail(mc)
    raise KeyError(f"Mapper '{mapper_name}' not found.")


def get_mapper(model_dir: Path, entity_name: str) -> dict:
    mc = _find_mapper_for_entity(model_dir, entity_name)
    if mc is None:
        raise KeyError(f"No mapper found for entity '{entity_name}'.")
    return _mapper_to_dict(mc, model_dir)


def validate_mapper(model_dir: Path, entity_name: str) -> dict:
    import tempfile

    from semantic.mapper import validate_mapper_yaml

    mc = _find_mapper_for_entity(model_dir, entity_name)
    if mc is None:
        raise KeyError(f"No mapper found for entity '{entity_name}'.")

    content = metadata_db.get_current_content(model_dir, "mapper", mc.name)
    errors = []
    if content:
        tmp_path = Path(tempfile.mktemp(suffix=".yaml"))
        try:
            tmp_path.write_text(content)
            errors = validate_mapper_yaml(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)
    return {
        "valid": len(errors) == 0,
        "errors": [{"path": e.path, "message": e.message} for e in errors],
    }


def run_mappers_sync(
    model_dir: Path,
    mapper_names: list[str] | None = None,
    dry_run: bool = False,
    strict: bool = False,
) -> list[dict]:
    """Run mappers synchronously; returns list of {name, result, error}."""
    from core.db import get_backend
    from engine.mapper import FanOutError, MapperError, run_mapper, topological_sort

    mappers_dir = model_dir / "mappers"
    ontology_dir = model_dir / "ontology"
    ontology_entities = YAMLAdapter.load_entities(ontology_dir)

    all_mappers = YAMLAdapter.load_mappers(mappers_dir)
    if mapper_names:
        mappers = [mc for mc in all_mappers if mc.name in mapper_names]
    else:
        mappers = topological_sort(all_mappers)

    backend = get_backend(model_dir, namespace="sem")
    source_backend = get_backend(model_dir, namespace="raw")
    backend.connect()
    source_backend.connect()
    results = []
    try:
        for mc in mappers:
            try:
                result = run_mapper(mc, ontology_entities, backend, strict=strict, dry_run=dry_run,
                                    source_backend=source_backend)
                results.append({"name": mc.name, "result": result, "error": None})
            except (FanOutError, MapperError) as exc:
                results.append({"name": mc.name, "result": None, "error": str(exc)})
    finally:
        backend.close()
        source_backend.close()

    if not dry_run:
        from core.registry import append_audit, load_registry
        if model_dir.name in load_registry(model_dir.parent):
            scope = ", ".join(mapper_names) if mapper_names else "all mappers"
            append_audit(model_dir, "map_run", scope, actor="cli")

    return results


class MapperValidationError(Exception):
    """Raised when mapper YAML fails schema validation."""
    def __init__(self, errors: list[dict]):
        self.errors = errors
        super().__init__(f"{len(errors)} validation error(s)")


def create_or_replace_mapper(model_dir: Path, name: str, content: str) -> dict:
    """Validate YAML content and write to the metadata DB."""
    import os
    import tempfile

    import yaml as _yaml

    from semantic.mapper import parse_mapper_dict, validate_mapper_yaml

    tmp_fd, tmp_path_str = tempfile.mkstemp(suffix=".yaml")
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(tmp_fd, "w") as f:
            f.write(content)
        errors = validate_mapper_yaml(tmp_path)
        if errors:
            raise MapperValidationError(
                [{"path": e.path, "message": e.message} for e in errors]
            )
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass

    metadata_db.record_version(model_dir, "mapper", name, content)
    mc = parse_mapper_dict(_yaml.safe_load(content))
    return _mapper_to_dict(mc, model_dir)


def validate_all(model_dir: Path) -> list[dict]:
    """Validate all mapper artifacts in the DB; returns list of {file, valid, errors}."""
    import tempfile

    from semantic.mapper import validate_mapper_yaml

    results = []
    for name in metadata_db.list_artifact_names(model_dir, "mapper"):
        content = metadata_db.get_current_content(model_dir, "mapper", name)
        tmp_path = Path(tempfile.mktemp(suffix=".yaml"))
        try:
            tmp_path.write_text(content)
            errors = validate_mapper_yaml(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)
        results.append({
            "file": f"{name}.yaml",
            "valid": len(errors) == 0,
            "errors": [{"path": e.path, "message": e.message, "rule": getattr(e, "rule", "")} for e in errors],
        })
    return results


def execute_mapper(
    model_dir: Path,
    entity_name: str,
    job_service,
) -> object:
    """Start a background mapper execution job and return the job."""
    mc = _find_mapper_for_entity(model_dir, entity_name)
    if mc is None:
        raise KeyError(f"No mapper found for entity '{entity_name}'.")

    job = job_service.create(
        model=model_dir.name,
        operation="map",
        entity=entity_name,
    )
    from core.registry import append_audit, append_job_audit, load_registry
    append_job_audit(model_dir, job)

    def _run():
        import time

        from core.db import get_backend
        from engine.mapper import run_mapper
        from semantic.yaml_adapter import YAMLAdapter

        try:
            entities = YAMLAdapter.load_entities(model_dir / "ontology")
            backend = get_backend(model_dir, namespace="sem")
            source_backend = get_backend(model_dir, namespace="raw")
            backend.connect()
            source_backend.connect()
            t0 = time.time()
            try:
                result = run_mapper(mc, entities, backend, source_backend=source_backend)
            finally:
                backend.close()
                source_backend.close()
            elapsed = round(time.time() - t0, 1)
            rows = result.total_grain_rows
            job_service.complete(
                job.job_id,
                {"rows_written": rows, "duration_s": elapsed},
            )
            if model_dir.name in load_registry(model_dir.parent):
                append_audit(
                    model_dir,
                    "map_run",
                    f"{mc.name} — {rows} rows; {elapsed}s",
                    actor="api",
                )
        except BaseException as exc:
            _log.exception("Unhandled error in mapper job %s", job.job_id)
            job_service.fail(job.job_id, str(exc))
        finally:
            append_job_audit(model_dir, job_service.get(model_dir.name, job.job_id) or job)

    job_service.submit(_run)
    return job


def execute_all_mappers(model_dir: Path, job_service) -> object:
    """Run all mappers sequentially in a single background job."""
    mappers_dir = model_dir / "mappers"
    all_mappers = YAMLAdapter.load_mappers(mappers_dir)
    if not all_mappers:
        raise KeyError("No mappers found in this model.")

    job = job_service.create(
        model=model_dir.name,
        operation="map",
    )
    from core.registry import append_job_audit
    append_job_audit(model_dir, job)

    def _run():
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from core.db import get_backend
        from core.registry import append_audit, append_job_audit, load_registry
        from engine.mapper import build_execution_waves, run_mapper
        from semantic.yaml_adapter import YAMLAdapter

        def _run_one(mc, entities):
            backend = get_backend(model_dir, namespace="sem")
            source_backend = get_backend(model_dir, namespace="raw")
            backend.connect()
            source_backend.connect()
            try:
                result = run_mapper(mc, entities, backend, source_backend=source_backend)
                return result.total_grain_rows
            finally:
                backend.close()
                source_backend.close()

        try:
            entities = YAMLAdapter.load_entities(model_dir / "ontology")
            waves = build_execution_waves(all_mappers)
            n_waves = len(waves)
            total_rows = 0
            t0 = time.time()

            for wave_idx, wave in enumerate(waves, 1):
                if len(wave) == 1:
                    mc = wave[0]
                    job_service.update_progress(job.job_id, f"mapper {mc.name} — starting")
                    t_map = time.time()
                    rows = _run_one(mc, entities)
                    total_rows += rows
                    elapsed_map = round(time.time() - t_map, 1)
                    job_service.update_progress(
                        job.job_id,
                        f"mapper {mc.name} — done  {rows} rows  ({elapsed_map}s)",
                    )
                else:
                    names = ", ".join(mc.name for mc in wave)
                    job_service.update_progress(
                        job.job_id,
                        f"wave {wave_idx}/{n_waves}: {names} — starting ({len(wave)} parallel)",
                    )
                    wave_rows = 0
                    import os as _os
                    workers = min(len(wave), int(_os.environ.get("MAPPER_WAVE_WORKERS", "8")))
                    with ThreadPoolExecutor(max_workers=workers) as wave_pool:
                        futures = {wave_pool.submit(_run_one, mc, entities): mc for mc in wave}
                        for fut in as_completed(futures):
                            mc = futures[fut]
                            rows = fut.result()
                            wave_rows += rows
                            job_service.update_progress(
                                job.job_id, f"mapper {mc.name} — done  {rows} rows"
                            )
                    total_rows += wave_rows
                    job_service.update_progress(
                        job.job_id, f"wave {wave_idx}/{n_waves}: done  {wave_rows} rows"
                    )

            elapsed = round(time.time() - t0, 1)
            job_service.complete(job.job_id, {"rows_written": total_rows, "duration_s": elapsed})
            if model_dir.name in load_registry(model_dir.parent):
                n = len(all_mappers)
                append_audit(model_dir, "map_run", f"all ({n} mappers) — {total_rows} rows; {elapsed}s", actor="api")
        except BaseException as exc:
            _log.exception("Unhandled error in execute-all-mappers job %s", job.job_id)
            job_service.fail(job.job_id, str(exc))
        finally:
            append_job_audit(model_dir, job_service.get(model_dir.name, job.job_id) or job)

    job_service.submit(_run)
    return job


# ---------------------------------------------------------------------------

def _mapper_full_detail(mc: MapperConfig) -> dict:
    """Rich dict for the inspect command — includes filter, ON conditions, enums."""
    return {
        "name": mc.name,
        "description": mc.description,
        "grain": mc.grain,
        "depends_on": list(mc.depends_on),
        "sources": [
            {
                "alias": s.alias,
                "table": s.table,
                "columns": list(s.columns),
                "filter": s.filter,
            }
            for s in mc.sources
        ],
        "joins": [
            {
                "from_alias": j.from_alias,
                "to_alias": j.to_alias,
                "join_type": j.join_type.value if hasattr(j.join_type, "value") else str(j.join_type),
                "on": [{"left": c.left, "right": c.right} for c in j.on],
            }
            for j in mc.joins
        ],
        "targets": [
            {
                "entity_name": t.entity_name,
                "columns": [{"name": c.name, "expression": c.expression} for c in t.columns],
            }
            for t in mc.targets
        ],
        "enums": [
            {"name": e.name, "mapping": e.mapping}
            for e in mc.enums
        ],
    }


def _mapper_to_dict(mc: MapperConfig, model_dir: Path) -> dict:
    entity = None
    for target in (mc.targets if hasattr(mc, "targets") else []):
        entity = getattr(target, "entity_name", None)
        if entity:
            break
    raw_yaml = metadata_db.get_current_content(model_dir, "mapper", mc.name)
    return {
        "name": mc.name,
        "file": f"mappers/{mc.name}.yaml",
        "depends_on": list(mc.depends_on) if hasattr(mc, "depends_on") else [],
        "target_table": _target_table(mc),
        "entity": entity,
        "sources": [_source_to_dict(s) for s in (mc.sources if hasattr(mc, "sources") else [])],
        "joins": [_join_to_dict(j) for j in (mc.joins if hasattr(mc, "joins") else [])],
        "target_columns": _target_columns(mc),
        "raw_yaml": raw_yaml,
    }


def _target_table(mc: MapperConfig) -> str | None:
    for target in (mc.targets if hasattr(mc, "targets") else []):
        entity = getattr(target, "entity_name", None)
        if entity:
            return entity.lower()
    return None


def _source_to_dict(s) -> dict:
    return {
        "alias": getattr(s, "alias", None),
        "table": getattr(s, "table", None),
        "columns": list(getattr(s, "columns", [])),
    }


def _join_to_dict(j) -> dict:
    return {
        "from": getattr(j, "from_alias", None),
        "to": getattr(j, "to_alias", None),
        "type": getattr(j, "join_type", "left"),
    }


def _target_columns(mc: MapperConfig) -> list[dict]:
    cols = []
    for target in (mc.targets if hasattr(mc, "targets") else []):
        for col in (target.columns if hasattr(target, "columns") else []):
            cols.append({
                "name": getattr(col, "name", None),
                "expression": getattr(col, "expression", None),
            })
    return cols
