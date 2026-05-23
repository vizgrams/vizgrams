# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""View service: list, inspect, validate and execute views."""

import logging
from pathlib import Path

from core import metadata_db
from semantic.yaml_adapter import YAMLAdapter

_log = logging.getLogger(__name__)


def list_views(model_dir: Path) -> list[dict]:
    views_dir = model_dir / "views"
    views = YAMLAdapter.load_views(views_dir)
    from api.services.certification_service import list_cert_payloads
    certs = list_cert_payloads(model_dir, "view")
    return [
        {"name": v.name, "type": v.type, "query": v.query, **certs.get(v.name, _cert_default())}
        for v in views
    ]


def get_view(model_dir: Path, view_name: str) -> dict:
    views_dir = model_dir / "views"
    v = YAMLAdapter.load_view(view_name, views_dir)
    if v is None:
        raise KeyError(f"View '{view_name}' not found.")
    raw_yaml = metadata_db.get_current_content(model_dir, "view", view_name)
    from api.services.certification_service import get_cert_payload
    return {
        **_view_detail(v, raw_yaml),
        "params": _query_params(model_dir, v.query),
        **get_cert_payload(model_dir, "view", view_name),
    }


def _cert_default() -> dict:
    """Shape used when an artifact has no certification row."""
    return {
        "is_certified": False,
        "certified_by": None,
        "certified_by_display": None,
        "certified_at": None,
    }


def execute_view(
    model_dir: Path, view_name: str, limit: int = 1000, offset: int = 0, params: dict | None = None
) -> dict:
    views_dir = model_dir / "views"
    v = YAMLAdapter.load_view(view_name, views_dir)
    if v is None:
        raise KeyError(f"View '{view_name}' not found.")

    from api.services.query_service import execute_query
    result = execute_query(model_dir, v.query, limit=limit, offset=offset, params=params)

    # Validate that declared view columns exist in the query output
    viz = v.visualization or {}
    declared_cols = viz.get("columns", []) if isinstance(viz, dict) else []
    if declared_cols:
        result_col_set = set(result["columns"])
        missing = [c for c in declared_cols if c not in result_col_set]
        if missing:
            raise ValueError(
                f"View '{view_name}' references columns not produced by query '{v.query}': "
                + ", ".join(repr(c) for c in missing)
            )

    # Merge column_formats from view visualization on top of query-derived formats
    col_formats = viz.get("column_formats", {}) if isinstance(viz, dict) else {}
    formats = {**result.get("formats", {}), **col_formats}

    return {
        **_view_detail(v, raw_yaml=None),
        "params": _query_params(model_dir, v.query),
        "columns": result["columns"],
        "rows": result["rows"],
        "row_count": result["row_count"],
        "total_row_count": result["total_row_count"],
        "duration_ms": result["duration_ms"],
        "truncated": result["truncated"],
        "formats": formats,
    }


def validate_view(model_dir: Path, view_name: str) -> dict:
    import shutil
    import tempfile

    from semantic.view import validate_view as _validate_view

    views_dir = model_dir / "views"
    content = metadata_db.get_current_content(model_dir, "view", view_name)
    if content is None:
        raise KeyError(f"View '{view_name}' not found.")

    # Attempt column resolution for deeper validation; skip gracefully if DB unavailable
    v = YAMLAdapter.load_view(view_name, views_dir)
    known_query_columns: dict[str, list[str]] | None = None
    if v is not None:
        try:
            from api.services.query_service import execute_query
            result = execute_query(model_dir, v.query, limit=1)
            known_query_columns = {v.query: result["columns"]}
        except Exception as exc:
            _log.debug("Could not resolve columns for view '%s': %s", view_name, exc)

    tmp_dir = Path(tempfile.mkdtemp())
    try:
        view_file = tmp_dir / f"{view_name}.yaml"
        view_file.write_text(content)
        errors = _validate_view(view_file, known_query_columns=known_query_columns)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return {
        "valid": len(errors) == 0,
        "errors": [{"path": e.path, "message": e.message} for e in errors],
        "compiled_sql": None,
    }


def execute_inline_view(
    model_dir: Path,
    view_yaml: str,
    query_yaml: str | None = None,
    params: dict | None = None,
    limit: int = 1000,
    offset: int = 0,
) -> dict:
    """Execute a view defined as YAML content (no saved artifact required).

    Used by the chat (Epic 20 VG-237) for transient inline views.

    Two modes:
      - ``query_yaml=None``: the view's ``query:`` field must name an
        already-saved query. We execute that saved query and apply the
        view's visualization on top.
      - ``query_yaml`` given: a transient query that doesn't exist in
        ``api.db``. The view's ``query:`` field should match the inline
        query's name; we execute the inline query and apply the view.
    """
    import yaml as _yaml

    from semantic.view import parse_view_dict

    try:
        view_data = _yaml.safe_load(view_yaml)
        view = parse_view_dict(view_data)
    except Exception as exc:
        raise ValueError(f"Could not parse inline view YAML: {exc}") from exc

    # Execute the underlying query — either inline or by saved name.
    if query_yaml is not None:
        from api.services.query_service import execute_inline_yaml
        query_result = execute_inline_yaml(
            model_dir, view.query, query_yaml, limit=limit, offset=offset,
        )
    else:
        from api.services.query_service import execute_query
        query_result = execute_query(
            model_dir, view.query, limit=limit, offset=offset, params=params,
        )

    # Optional column validation — same checks ``execute_view`` does for
    # saved views. Skipped if the view doesn't declare columns explicitly.
    viz = view.visualization or {}
    declared_cols = viz.get("columns", []) if isinstance(viz, dict) else []
    if declared_cols:
        result_col_set = set(query_result["columns"])
        missing = [c for c in declared_cols if c not in result_col_set]
        if missing:
            raise ValueError(
                "Inline view references columns not produced by query: "
                + ", ".join(repr(c) for c in missing)
            )

    col_formats = viz.get("column_formats", {}) if isinstance(viz, dict) else {}
    formats = {**query_result.get("formats", {}), **col_formats}

    return {
        **_view_detail(view, raw_yaml=view_yaml),
        "params": [],  # inline views don't carry a param schema separately
        "columns": query_result["columns"],
        "rows": query_result["rows"],
        "row_count": query_result["row_count"],
        "total_row_count": query_result["total_row_count"],
        "duration_ms": query_result["duration_ms"],
        "truncated": query_result["truncated"],
        "formats": formats,
    }


def validate_inline_view(
    model_dir: Path,
    view_name: str,
    content: str,
    known_query_columns: dict[str, list[str]] | None = None,
) -> dict:
    """Validate ViewDef YAML content without saving to disk.

    Mirrors ``query_service.validate_inline_query``. ``known_query_columns``
    maps query name → its result columns; pass it when the underlying
    query hasn't been saved yet (e.g. the inline query authored by
    text2query) so axes-vs-columns validation still runs.
    """
    import shutil
    import tempfile

    from semantic.view import validate_view as _validate_view

    tmp_dir = Path(tempfile.mkdtemp())
    try:
        tmp_path = tmp_dir / f"{view_name}.yaml"
        tmp_path.write_text(content)
        errors = _validate_view(tmp_path, known_query_columns=known_query_columns)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return {
        "valid": len(errors) == 0,
        "errors": [{"path": e.path, "message": e.message} for e in errors],
    }


class ViewValidationError(Exception):
    def __init__(self, errors: list[dict]):
        self.errors = errors
        super().__init__("View validation failed")


def create_or_replace_view(model_dir: Path, view_name: str, content: str) -> dict:
    import shutil
    import tempfile

    from semantic.view import validate_view as _validate_view

    tmp_dir = Path(tempfile.mkdtemp())
    try:
        tmp_path = tmp_dir / f"{view_name}.yaml"
        tmp_path.write_text(content)

        v = YAMLAdapter.load_view(view_name, tmp_dir)
        known_query_columns: dict[str, list[str]] | None = None
        if v is not None:
            try:
                from api.services.query_service import execute_query
                result = execute_query(model_dir, v.query, limit=1)
                known_query_columns = {v.query: result["columns"]}
            except Exception as exc:
                _log.debug("Could not resolve columns for '%s': %s", view_name, exc)

        errors = _validate_view(tmp_path, known_query_columns=known_query_columns)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    if errors:
        raise ViewValidationError([{"path": e.path, "message": e.message} for e in errors])

    metadata_db.record_version(model_dir, "view", view_name, content)
    return get_view(model_dir, view_name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _view_detail(v, raw_yaml: str | None) -> dict:
    return {
        "name": v.name,
        "type": v.type,
        "query": v.query,
        "measure": v.measure,
        "visualization": v.visualization,
        "inputs": {k: {"type": inp.type, "default": inp.default} for k, inp in v.inputs.items()},
        "raw_yaml": raw_yaml,
    }


def _query_params(model_dir: Path, query_name: str) -> list[dict]:
    """Return the params list for a query, or [] if none."""
    queries_dir = model_dir / "queries"
    query = YAMLAdapter.load_query(query_name, queries_dir)
    if query is None:
        return []
    return [
        {"name": p.name, "type": p.type, "label": p.label, "default": p.default, "optional": p.optional}
        for p in getattr(query, "parameters", [])
    ]


def _validate_app_views(model_dir: Path, view_names: list[str]) -> list:
    """Validate each view's declared columns exist in its query output."""
    import shutil
    import tempfile

    from core.validation import ValidationError
    from semantic.view import validate_view as _validate_view

    errors = []
    views_dir = model_dir / "views"
    for view_name in view_names:
        content = metadata_db.get_current_content(model_dir, "view", view_name)
        if content is None:
            continue  # missing view already caught by structural validation
        v = YAMLAdapter.load_view(view_name, views_dir)
        if v is None:
            continue
        known_query_columns = None
        try:
            from api.services.query_service import execute_query
            result = execute_query(model_dir, v.query, limit=1)
            known_query_columns = {v.query: result["columns"]}
        except Exception as exc:
            _log.debug("Could not resolve columns for view '%s': %s", view_name, exc)
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            view_file = tmp_dir / f"{view_name}.yaml"
            view_file.write_text(content)
            view_errors = _validate_view(view_file, known_query_columns=known_query_columns)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        for e in view_errors:
            errors.append(ValidationError(
                path=f"view:{view_name}/{e.path}",
                message=f"[view:{view_name}] {e.message}",
                rule=e.rule,
            ))
    return errors
