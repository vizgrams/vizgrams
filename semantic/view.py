# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""View definition dataclasses, YAML parsing, and validation."""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_log = logging.getLogger(__name__)

from core.validation import ValidationError, validate_schema


@dataclass
class InputDef:
    type: str   # STRING | INTEGER | BOOLEAN
    default: Any = None


@dataclass
class ViewDef:
    name: str
    type: str                                    # chart | metric | table | map
    query: str
    visualization: dict                          # raw dict; type-specific checks in validate_view
    measure: str | None = None                   # required for type: metric
    inputs: dict[str, InputDef] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_view_dict(data: dict) -> ViewDef:
    """Parse a view definition dict (already loaded from YAML) into a ViewDef."""
    inputs: dict[str, InputDef] = {}
    for k, v in (data.get("inputs") or {}).items():
        inputs[k] = InputDef(type=v.get("type", "STRING"), default=v.get("default"))
    return ViewDef(
        name=data.get("name", ""),
        type=data.get("type", ""),
        query=data.get("query", ""),
        visualization=dict(data.get("visualization") or {}),
        measure=data.get("measure"),
        inputs=inputs,
    )


def parse_view_yaml(path: str | Path) -> ViewDef:
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)
    return parse_view_dict(data)


def load_all_views(views_dir: str | Path) -> list[ViewDef]:
    views_dir = Path(views_dir)
    result = []
    for path in sorted(views_dir.glob("*.yaml")):
        try:
            result.append(parse_view_yaml(path))
        except Exception as exc:
            _log.warning("Skipping malformed view file %s: %s", path.name, exc)
    return result


def load_view_by_name(name: str, views_dir: str | Path) -> ViewDef | None:
    path = Path(views_dir) / f"{name}.yaml"
    if not path.is_file():
        return None
    return parse_view_yaml(path)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_view(
    path: str | Path,
    known_query_columns: dict[str, list[str]] | None = None,
) -> list[ValidationError]:
    """Validate a view YAML file.

    Phase 1: JSON Schema structural check.
    Phase 2: semantic checks (name matches filename, column references).

    known_query_columns: maps query_name → list of output column names.
    Column-level checks are skipped if the query is not present in this map.
    """
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)

    errors = validate_schema(data, "view")
    if errors:
        return errors

    view = parse_view_dict(data)

    if view.name != path.stem:
        errors.append(ValidationError(
            path="name",
            message=f"view name '{view.name}' must match filename '{path.stem}'",
            rule="name_matches_filename",
        ))

    cols = (known_query_columns or {}).get(view.query)
    if cols is not None:
        col_set = set(cols)
        viz = view.visualization

        if view.type == "metric":
            if not view.measure:
                errors.append(ValidationError(
                    path="measure",
                    message="type 'metric' requires a 'measure' field",
                    rule="metric_requires_measure",
                ))
            elif view.measure not in col_set:
                errors.append(ValidationError(
                    path="measure",
                    message=f"measure column '{view.measure}' not found in query '{view.query}' output",
                    rule="unknown_column",
                ))

        elif view.type == "table":
            for col in (viz.get("columns") or []):
                if col not in col_set:
                    errors.append(ValidationError(
                        path="visualization.columns",
                        message=f"column '{col}' not found in query '{view.query}' output",
                        rule="unknown_column",
                    ))

        elif view.type == "chart":
            chart_type = viz.get("chart_type", "")
            if chart_type in ("line", "bar"):
                x = viz.get("x")
                if x and x not in col_set:
                    errors.append(ValidationError(
                        path="visualization.x",
                        message=f"column '{x}' not found in query '{view.query}' output",
                        rule="unknown_column",
                    ))
                for y_col in (viz.get("y") or []):
                    if y_col not in col_set:
                        errors.append(ValidationError(
                            path="visualization.y",
                            message=f"column '{y_col}' not found in query '{view.query}' output",
                            rule="unknown_column",
                        ))
            elif chart_type == "calendar_heatmap":
                for field_name, col in [("date", viz.get("date")), ("value", viz.get("value"))]:
                    if col and col not in col_set:
                        errors.append(ValidationError(
                            path=f"visualization.{field_name}",
                            message=f"column '{col}' not found in query '{view.query}' output",
                            rule="unknown_column",
                        ))
                group_col = viz.get("group_by")
                if group_col and group_col not in col_set:
                    errors.append(ValidationError(
                        path="visualization.group_by",
                        message=f"column '{group_col}' not found in query '{view.query}' output",
                        rule="unknown_column",
                    ))

        elif view.type == "map":
            for field_name in ("lat", "lon"):
                col = viz.get(field_name)
                if not col:
                    errors.append(ValidationError(
                        path=f"visualization.{field_name}",
                        message=f"type 'map' requires a '{field_name}' field",
                        rule="map_requires_lat_lon",
                    ))
                elif col not in col_set:
                    errors.append(ValidationError(
                        path=f"visualization.{field_name}",
                        message=f"column '{col}' not found in query '{view.query}' output",
                        rule="unknown_column",
                    ))
            for opt_field in ("label", "size"):
                col = viz.get(opt_field)
                if col and col not in col_set:
                    errors.append(ValidationError(
                        path=f"visualization.{opt_field}",
                        message=f"column '{col}' not found in query '{view.query}' output",
                        rule="unknown_column",
                    ))

    return errors
