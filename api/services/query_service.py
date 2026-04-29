# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Query service: list, inspect, validate and execute queries."""

import time
from pathlib import Path

from core import metadata_db
from core.db import get_backend
from core.model_config import load_database_config
from semantic.types import expand_event_entities
from semantic.yaml_adapter import YAMLAdapter


def list_queries(model_dir: Path) -> list[dict]:
    queries_dir = model_dir / "queries"
    queries = YAMLAdapter.load_queries(queries_dir)
    return [
        {
            "name": q.name,
            "root": _root(q),
            "measure_count": _measure_count(q),
            "group_by_count": len(q.slices) if hasattr(q, "slices") and q.slices else 0,
        }
        for q in queries
    ]


def get_query(model_dir: Path, query_name: str) -> dict:
    queries_dir = model_dir / "queries"
    q = YAMLAdapter.load_query(query_name, queries_dir)
    if q is None:
        raise KeyError(f"Query '{query_name}' not found.")

    compiled_sql = _compile_query(q, model_dir)
    raw_yaml = metadata_db.get_current_content(model_dir, "query", query_name)
    import re as _re
    description = None
    if raw_yaml:
        m = _re.search(r'^description:\s*(.+)$', raw_yaml, _re.MULTILINE)
        description = m.group(1).strip().strip('"\'') if m else None
    parameters = []
    for p in (getattr(q, "parameters", None) or []):
        parameters.append({
            "name": p.name,
            "type": p.type,
            "label": p.label,
            "default": p.default,
            "optional": p.optional,
        })

    return {
        "name": q.name,
        "root": _root(q),
        "description": description,
        "group_by": [s.field for s in (q.slices or [])] if hasattr(q, "slices") else [],
        "attributes": [
            {"field": s.field, "alias": s.alias or "", "format_pattern": s.format_pattern or ""}
            for s in (q.slices or [])
        ] if hasattr(q, "slices") else [],
        "detail_attributes": [
            {
                "field": a.expr_str or ".".join(a.parts),
                "alias": a.label or "",
            }
            for a in (q.attributes or [])
        ] if hasattr(q, "attributes") else [],
        "measures": _measures_dict(q),
        "where": list(q.filters) if hasattr(q, "filters") and q.filters else [],
        "params": parameters,
        "order_by": _order_by(q),
        "compiled_sql": compiled_sql,
        "raw_yaml": raw_yaml,
    }


def validate_query(model_dir: Path, query_name: str) -> dict:
    import shutil
    import tempfile

    from semantic.query import validate_query_yaml

    queries_dir = model_dir / "queries"
    q = YAMLAdapter.load_query(query_name, queries_dir)
    if q is None:
        raise KeyError(f"Query '{query_name}' not found.")

    content = metadata_db.get_current_content(model_dir, "query", query_name)
    errors = []
    if content:
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            query_file = tmp_dir / f"{query_name}.yaml"
            query_file.write_text(content)
            ontology_dir = model_dir / "ontology"
            entities = expand_event_entities({e.name: e for e in YAMLAdapter.load_entities(ontology_dir)})
            features_by_entity = _load_features_by_entity(model_dir)
            errors = validate_query_yaml(query_file, entities=entities, features_by_entity=features_by_entity)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    compiled_sql = _compile_query(q, model_dir) if not errors else None
    return {
        "valid": len(errors) == 0,
        "errors": [{"path": e.path, "message": e.message} for e in errors],
        "compiled_sql": compiled_sql,
    }


def validate_inline_query(model_dir: Path, name: str, content: str) -> dict:
    """Validate YAML content without saving to disk."""
    import shutil
    import tempfile

    from semantic.query import validate_query_yaml

    ontology_dir = model_dir / "ontology"
    entities = expand_event_entities({e.name: e for e in YAMLAdapter.load_entities(ontology_dir)})
    features_by_entity = _load_features_by_entity(model_dir)

    tmp_dir = Path(tempfile.mkdtemp())
    try:
        tmp_path = tmp_dir / f"{name}.yaml"
        tmp_path.write_text(content)
        errors = validate_query_yaml(tmp_path, entities=entities, features_by_entity=features_by_entity)

        compiled_sql = None
        if not errors:
            try:
                import yaml as _yaml

                from semantic.query import parse_query_dict
                q = parse_query_dict(_yaml.safe_load(tmp_path.read_text()))
                if q:
                    compiled_sql = _compile_query(q, model_dir)
            except Exception:
                pass
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return {
        "valid": len(errors) == 0,
        "errors": [{"path": e.path, "message": e.message} for e in errors],
        "compiled_sql": compiled_sql,
    }


def execute_query(
    model_dir: Path,
    query_name: str,
    limit: int = 1000,
    offset: int = 0,
    fmt: str = "json",
    params: dict | None = None,
) -> dict:
    queries_dir = model_dir / "queries"
    q = YAMLAdapter.load_query(query_name, queries_dir)
    if q is None:
        raise KeyError(f"Query '{query_name}' not found.")

    dialect = load_database_config(model_dir).get("backend", "clickhouse")

    try:
        sql = _compile_query_or_raise(q, model_dir, params=params, dialect=dialect)
    except Exception as exc:
        raise ValueError(f"Could not compile query '{query_name}': {exc}") from exc

    backend = get_backend(model_dir)
    backend.connect()
    try:
        from semantic.types import expand_event_entities
        entities = {e.name: e for e in YAMLAdapter.load_entities(model_dir / "ontology")}
        entities = expand_event_entities(entities)
        root_name = getattr(q, "entity", None) or getattr(q, "root", None)
        root_entity = entities.get(root_name) if root_name else None
        if root_entity and not backend.table_exists(root_entity.table_name):
            raise FileNotFoundError(
                f"Entity table '{root_entity.table_name}' not found. "
                "Run materialisation for this model first."
            )
        t0 = time.time()
        all_rows_raw = backend.execute(sql)
        columns = [c.split(".", 1)[-1] if "." in c else c for c in backend.last_columns]
        all_rows = [list(row) for row in all_rows_raw]
        elapsed_ms = round((time.time() - t0) * 1000)
    finally:
        backend.close()

    # Apply threshold evaluation: convert numeric values to status strings
    threshold_cols: dict[int, list] = {}
    if hasattr(q, "metrics") and q.metrics:
        from semantic.query import evaluate_threshold
        col_index = {col: i for i, col in enumerate(columns)}
        for metric_name, metric in q.metrics.items():
            if metric.thresholds and metric_name in col_index:
                threshold_cols[col_index[metric_name]] = metric.thresholds
    if threshold_cols:
        for row in all_rows:
            for col_idx, thresholds in threshold_cols.items():
                row[col_idx] = evaluate_threshold(row[col_idx], thresholds)

    total = len(all_rows)
    page = all_rows[offset: offset + limit]
    truncated = (offset + limit) < total

    # Build format metadata: column name → FormatSpec dict
    formats: dict[str, dict] = {}
    if hasattr(q, "metrics") and q.metrics:
        for metric_name, metric in q.metrics.items():
            if metric.format is not None:
                formats[metric_name] = {
                    "type": metric.format.type,
                    "pattern": metric.format.pattern,
                    "unit": metric.format.unit,
                }

    return {
        "query": query_name,
        "sql": sql,
        "columns": columns,
        "rows": page,
        "row_count": len(page),
        "total_row_count": total,
        "duration_ms": elapsed_ms,
        "truncated": truncated,
        "formats": formats,
    }


def execute_inline_yaml(
    model_dir: Path,
    name: str,
    content: str,
    limit: int = 1000,
    offset: int = 0,
) -> dict:
    """Compile and execute a query defined as YAML content (no file required)."""
    import yaml as _yaml

    from semantic.query import parse_query_dict

    try:
        q = parse_query_dict(_yaml.safe_load(content))
    except Exception as exc:
        raise ValueError(f"Could not parse query YAML for '{name}'") from exc
    if q is None:
        raise ValueError(f"Could not parse query YAML for '{name}'")

    return _execute_query_obj(q, model_dir, limit=limit, offset=offset)


def execute_inline_query(
    model_dir: Path,
    query_data: dict,
    limit: int = 1000,
    offset: int = 0,
) -> dict:
    """Compile and execute a query defined inline as a dict (no file required)."""
    from semantic.query import parse_query_dict

    try:
        q = parse_query_dict(query_data)
    except Exception as exc:
        raise ValueError(f"Could not parse query: {exc}") from exc

    dialect = load_database_config(model_dir).get("backend", "clickhouse")
    try:
        # Use a large page_size so the SQL fetches all rows; Python handles ordering + pagination.
        sql = _compile_query_or_raise(q, model_dir, detail_page_size=1_000_000, dialect=dialect)
    except Exception as exc:
        raise ValueError(f"Could not compile query: {exc}") from exc

    backend = get_backend(model_dir)
    backend.connect()
    try:
        t0 = time.time()
        all_rows_raw = backend.execute(sql)
        columns = [c.split(".", 1)[-1] if "." in c else c for c in backend.last_columns]
        all_rows = [list(row) for row in all_rows_raw]
        elapsed_ms = round((time.time() - t0) * 1000)
    finally:
        backend.close()

    # Apply threshold evaluation
    threshold_cols: dict[int, list] = {}
    if hasattr(q, "metrics") and q.metrics:
        from semantic.query import evaluate_threshold
        col_index = {col: i for i, col in enumerate(columns)}
        for metric_name, metric in q.metrics.items():
            if metric.thresholds and metric_name in col_index:
                threshold_cols[col_index[metric_name]] = metric.thresholds
    if threshold_cols:
        for row in all_rows:
            for col_idx, thresholds in threshold_cols.items():
                row[col_idx] = evaluate_threshold(row[col_idx], thresholds)

    # Sort across all rows before paginating
    if q.order_by:
        col_index = {c: i for i, c in enumerate(columns)}
        for col, direction in reversed(q.order_by):
            idx = col_index.get(col)
            if idx is not None:
                all_rows.sort(
                    key=lambda r, i=idx: (r[i] is None, r[i]),
                    reverse=(direction == "DESC"),
                )

    total = len(all_rows)
    page = all_rows[offset: offset + limit]
    truncated = (offset + limit) < total

    formats: dict[str, dict] = {}
    if hasattr(q, "metrics") and q.metrics:
        for metric_name, metric in q.metrics.items():
            if metric.format is not None:
                formats[metric_name] = {
                    "type": metric.format.type,
                    "pattern": metric.format.pattern,
                    "unit": metric.format.unit,
                }

    return {
        "query": "_inline",
        "sql": sql,
        "columns": columns,
        "rows": page,
        "row_count": len(page),
        "total_row_count": total,
        "duration_ms": elapsed_ms,
        "truncated": truncated,
        "formats": formats,
    }


class QueryValidationError(Exception):
    """Raised when query YAML fails schema validation."""
    def __init__(self, errors: list[dict]):
        self.errors = errors
        super().__init__(f"{len(errors)} validation error(s)")


def create_or_replace_query(model_dir: Path, query_name: str, content: str) -> dict:
    """Validate YAML content and write to the metadata DB."""
    import shutil
    import tempfile

    from semantic.query import validate_query_yaml

    ontology_dir = model_dir / "ontology"
    entities = expand_event_entities({e.name: e for e in YAMLAdapter.load_entities(ontology_dir)})
    features_by_entity = _load_features_by_entity(model_dir)

    # Use a temp directory so we control the filename (validator checks name == stem)
    tmp_dir = Path(tempfile.mkdtemp())
    try:
        tmp_path = tmp_dir / f"{query_name}.yaml"
        tmp_path.write_text(content)
        errors = validate_query_yaml(
            tmp_path,
            entities=entities,
            features_by_entity=features_by_entity,
        )
        if errors:
            raise QueryValidationError(
                [{"path": e.path, "message": e.message} for e in errors]
            )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    metadata_db.record_version(model_dir, "query", query_name, content)
    return get_query(model_dir, query_name)


def validate_all(model_dir: Path) -> list[dict]:
    """Validate all query artifacts in the DB; returns list of {file, valid, errors}."""
    import shutil
    import tempfile

    from semantic.query import validate_query_yaml

    ontology_dir = model_dir / "ontology"
    entities = expand_event_entities({e.name: e for e in YAMLAdapter.load_entities(ontology_dir)})
    features_by_entity = _load_features_by_entity(model_dir)
    results = []
    for name in metadata_db.list_artifact_names(model_dir, "query"):
        content = metadata_db.get_current_content(model_dir, "query", name)
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            query_file = tmp_dir / f"{name}.yaml"
            query_file.write_text(content)
            errors = validate_query_yaml(query_file, entities=entities, features_by_entity=features_by_entity)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        results.append({
            "file": f"{name}.yaml",
            "valid": len(errors) == 0,
            "errors": [{"path": e.path, "message": e.message, "rule": getattr(e, "rule", "")} for e in errors],
        })
    return results


# ---------------------------------------------------------------------------

def _root(q) -> str | None:
    if hasattr(q, "entity"):
        return q.entity
    if hasattr(q, "root_entity"):
        return q.root_entity
    return None


def _measure_count(q) -> int:
    metrics = getattr(q, "metrics", None) or []
    return len(metrics)


def _measures_dict(q) -> dict:
    from semantic.query import RatioMetric
    result = {}
    metrics = getattr(q, "metrics", None) or {}
    items = (
        metrics.items() if isinstance(metrics, dict)
        else [(getattr(m, "name", None) or getattr(m, "alias", None), m) for m in metrics]
    )
    for name, m in items:
        if not name:
            continue
        if isinstance(m, RatioMetric):
            n, d = m.numerator, m.denominator
            expr = f"{n.rollup}({n.field}) / {d.rollup}({d.field})"
        else:
            rollup = getattr(m, "rollup", "count") or "count"
            field = getattr(m, "field", "") or ""
            expr = f"{rollup}({field})" if field else f"{rollup}(*)"
        entry: dict = {"expr": expr}
        fmt = getattr(m, "format", None)
        if fmt:
            entry["format"] = {
                "type": fmt.type,
                "pattern": fmt.pattern,
                "unit": fmt.unit,
            }
        result[name] = entry
    return result


def _order_by(q) -> list:
    sorts = getattr(q, "order_by", None) or []
    result = []
    for s in sorts:
        if isinstance(s, tuple):
            result.append({"field": str(s[0]), "direction": s[1].lower() if len(s) > 1 else "asc"})
        else:
            result.append({"field": str(getattr(s, "field", s)), "direction": getattr(s, "direction", "asc")})
    return result


def _execute_query_obj(q, model_dir: Path, label: str = "_inline", limit: int = 1000, offset: int = 0) -> dict:
    """Compile and execute a loaded QueryDef object."""
    from semantic.query import evaluate_threshold

    dialect = load_database_config(model_dir).get("backend", "clickhouse")
    try:
        sql = _compile_query_or_raise(q, model_dir, detail_page_size=1_000_000, dialect=dialect)
    except Exception as exc:
        raise ValueError(f"Could not compile query: {exc}") from exc

    backend = get_backend(model_dir)
    backend.connect()
    try:
        t0 = time.time()
        all_rows_raw = backend.execute(sql)
        columns = [c.split(".", 1)[-1] if "." in c else c for c in backend.last_columns]
        all_rows = [list(row) for row in all_rows_raw]
        elapsed_ms = round((time.time() - t0) * 1000)
    finally:
        backend.close()

    threshold_cols: dict[int, list] = {}
    if hasattr(q, "metrics") and q.metrics:
        col_index = {col: i for i, col in enumerate(columns)}
        for metric_name, metric in q.metrics.items():
            if metric.thresholds and metric_name in col_index:
                threshold_cols[col_index[metric_name]] = metric.thresholds
    if threshold_cols:
        for row in all_rows:
            for col_idx, thresholds in threshold_cols.items():
                row[col_idx] = evaluate_threshold(row[col_idx], thresholds)

    if getattr(q, "order_by", None):
        col_index = {c: i for i, c in enumerate(columns)}
        for col, direction in reversed(q.order_by):
            idx = col_index.get(col)
            if idx is not None:
                all_rows.sort(key=lambda r, i=idx: (r[i] is None, r[i]), reverse=(direction == "DESC"))

    total = len(all_rows)
    page = all_rows[offset: offset + limit]
    truncated = (offset + limit) < total

    formats: dict[str, dict] = {}
    if hasattr(q, "metrics") and q.metrics:
        for metric_name, metric in q.metrics.items():
            if metric.format is not None:
                formats[metric_name] = {
                    "type": metric.format.type,
                    "pattern": metric.format.pattern,
                    "unit": metric.format.unit,
                }

    return {
        "query": label,
        "sql": sql,
        "columns": columns,
        "rows": page,
        "row_count": len(page),
        "total_row_count": total,
        "duration_ms": elapsed_ms,
        "truncated": truncated,
        "formats": formats,
    }


def _compile_query_or_raise(
    q,
    model_dir: Path,
    detail_page_size: int = 1000,
    params: dict | None = None,
    dialect: str = "sqlite",
) -> str:
    import dataclasses

    from engine.filter_compiler import apply_params
    from engine.query_runner import build_aggregate_query, build_detail_query

    ontology_dir = model_dir / "ontology"
    entities = expand_event_entities({e.name: e for e in YAMLAdapter.load_entities(ontology_dir)})
    features_by_entity = _load_features_by_entity(model_dir)

    import re as _re
    resolved_filters = apply_params(
        getattr(q, "filters", []),
        getattr(q, "parameters", None) or [],
        params or {},
    )
    # Drop any filters still containing unresolved {param} placeholders
    # (happens when the caller omits params: from the YAML and apply_params has no param_map)
    _unresolved = _re.compile(r'\{[^}]+\}')
    resolved_filters = [f for f in resolved_filters if not (isinstance(f, str) and _unresolved.search(f))]
    q = dataclasses.replace(q, filters=resolved_filters)

    is_detail = q.detail or (
        not getattr(q, "slices", None)
        and not getattr(q, "metrics", None)
    )
    if is_detail:
        return build_detail_query(
            q, entities, page=1, page_size=detail_page_size,
            features_by_entity=features_by_entity, dialect=dialect,
        )
    return build_aggregate_query(q, entities, features_by_entity=features_by_entity, dialect=dialect)


def _compile_query(q, model_dir: Path) -> str | None:
    try:
        return _compile_query_or_raise(q, model_dir)
    except Exception:
        return None


def _load_features_by_entity(model_dir: Path) -> dict:
    features_dir = model_dir / "features"
    result: dict = {}
    for fd in YAMLAdapter.load_features(features_dir):
        attr_name = fd.feature_id.split(".")[-1]
        result.setdefault(fd.entity_type, {})[attr_name] = fd
    return result


