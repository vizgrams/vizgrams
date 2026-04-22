# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Application service: list, inspect, validate and save applications."""

import logging
from pathlib import Path

from core import metadata_db
from semantic.yaml_adapter import YAMLAdapter

_log = logging.getLogger(__name__)


def list_applications(model_dir: Path) -> list[dict]:
    apps_dir = model_dir / "applications"
    apps = YAMLAdapter.load_applications(apps_dir)
    return [{"name": a.name, "view_count": len(a.views)} for a in apps]


def get_application(model_dir: Path, app_name: str) -> dict:
    apps_dir = model_dir / "applications"
    a = YAMLAdapter.load_application(app_name, apps_dir)
    if a is None:
        raise KeyError(f"Application '{app_name}' not found.")
    raw_yaml = metadata_db.get_current_content(model_dir, "application", app_name)

    # Collect unique params across all views' queries (first declaration wins for each name)
    views_dir = model_dir / "views"
    queries_dir = model_dir / "queries"
    seen_param_names: set[str] = set()
    params: list[dict] = []
    for view_name in a.views:
        view = YAMLAdapter.load_view(view_name, views_dir)
        if view is None:
            continue
        query = YAMLAdapter.load_query(view.query, queries_dir)
        if query is None:
            continue
        for p in getattr(query, "parameters", []):
            if p.name not in seen_param_names:
                seen_param_names.add(p.name)
                params.append({
                    "name": p.name, "type": p.type, "label": p.label,
                    "default": p.default, "optional": p.optional,
                })

    detail = _app_detail(a, raw_yaml)
    detail["params"] = params
    return detail


def validate_application(model_dir: Path, app_name: str) -> dict:
    import shutil
    import tempfile

    from semantic.application import validate_application as _validate_application

    content = metadata_db.get_current_content(model_dir, "application", app_name)
    if content is None:
        raise KeyError(f"Application '{app_name}' not found.")

    known_view_names = set(metadata_db.list_artifact_names(model_dir, "view"))

    tmp_dir = Path(tempfile.mkdtemp())
    try:
        app_file = tmp_dir / f"{app_name}.yaml"
        app_file.write_text(content)
        errors = _validate_application(app_file, known_view_names=known_view_names)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # Also validate each referenced view's columns against its query
    a = YAMLAdapter.load_application(app_name, model_dir / "applications")
    if a:
        from api.services.view_service import _validate_app_views
        errors.extend(_validate_app_views(model_dir, a.views))

    return {
        "valid": len(errors) == 0,
        "errors": [{"path": e.path, "message": e.message} for e in errors],
        "compiled_sql": None,
    }


class ApplicationValidationError(Exception):
    def __init__(self, errors: list[dict]):
        self.errors = errors
        super().__init__("Application validation failed")


def create_or_replace_application(model_dir: Path, app_name: str, content: str) -> dict:
    import shutil
    import tempfile

    import yaml

    from semantic.application import parse_application_dict
    from semantic.application import validate_application as _validate_application

    known_view_names = set(metadata_db.list_artifact_names(model_dir, "view"))

    tmp_dir = Path(tempfile.mkdtemp())
    try:
        tmp_path = tmp_dir / f"{app_name}.yaml"
        tmp_path.write_text(content)
        errors = _validate_application(tmp_path, known_view_names=known_view_names)
        if not errors:
            a = parse_application_dict(yaml.safe_load(content))
            from api.services.view_service import _validate_app_views
            errors.extend(_validate_app_views(model_dir, a.views))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    if errors:
        raise ApplicationValidationError([{"path": e.path, "message": e.message} for e in errors])

    metadata_db.record_version(model_dir, "application", app_name, content)
    return get_application(model_dir, app_name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _app_detail(a, raw_yaml: str | None) -> dict:
    return {
        "name": a.name,
        "views": a.views,
        "layout": [{"row": row.views} for row in a.layout],
        "raw_yaml": raw_yaml,
    }
