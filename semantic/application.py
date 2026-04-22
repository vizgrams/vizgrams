# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Application definition dataclasses, YAML parsing, and validation."""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import yaml

_log = logging.getLogger(__name__)

from core.validation import ValidationError, validate_schema


@dataclass
class LayoutRow:
    views: list[str]


@dataclass
class ApplicationDef:
    name: str
    views: list[str]
    layout: list[LayoutRow] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_application_dict(data: dict) -> ApplicationDef:
    layout: list[LayoutRow] = []
    for row in (data.get("layout") or []):
        layout.append(LayoutRow(views=list(row.get("row", []))))
    return ApplicationDef(
        name=data.get("name", ""),
        views=list(data.get("views") or []),
        layout=layout,
    )


def parse_application_yaml(path: str | Path) -> ApplicationDef:
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)
    return parse_application_dict(data)


def load_all_applications(apps_dir: str | Path) -> list[ApplicationDef]:
    apps_dir = Path(apps_dir)
    result = []
    for path in sorted(apps_dir.glob("*.yaml")):
        try:
            result.append(parse_application_yaml(path))
        except Exception as exc:
            _log.warning("Skipping malformed application file %s: %s", path.name, exc)
    return result


def load_application_by_name(name: str, apps_dir: str | Path) -> ApplicationDef | None:
    path = Path(apps_dir) / f"{name}.yaml"
    if not path.is_file():
        return None
    return parse_application_yaml(path)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_application(
    path: str | Path,
    known_view_names: set[str] | None = None,
) -> list[ValidationError]:
    """Validate an application YAML file.

    Phase 1: JSON Schema structural check.
    Phase 2: name matches filename; all referenced views exist.
    """
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)

    errors = validate_schema(data, "application")
    if errors:
        return errors

    app = parse_application_dict(data)

    if app.name != path.stem:
        errors.append(ValidationError(
            path="name",
            message=f"application name '{app.name}' must match filename '{path.stem}'",
            rule="name_matches_filename",
        ))

    if known_view_names is not None:
        all_referenced: set[str] = set(app.views)
        for row in app.layout:
            all_referenced.update(row.views)
        for view_name in sorted(all_referenced):
            if view_name not in known_view_names:
                errors.append(ValidationError(
                    path="views",
                    message=f"view '{view_name}' not found in model",
                    rule="unknown_view",
                ))

    return errors
