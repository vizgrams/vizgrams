# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Window validation rule tests for semantic/query.py validate_query_yaml."""

import yaml

from semantic.query import validate_query_yaml
from semantic.types import (
    AttributeDef,
    ColumnType,
    EntityDef,
    SemanticHint,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _pk(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.INTEGER, semantic=SemanticHint.PRIMARY_KEY)


def _ts(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.STRING, semantic=SemanticHint.TIMESTAMP)


def _attr(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.STRING)


def _measure(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.FLOAT)


def _make_entities() -> dict[str, EntityDef]:
    """ProductVersion with released_at (TIMESTAMP), build_duration (FLOAT), display_name (STRING)."""
    pv = EntityDef(
        name="ProductVersion",
        identity=[_pk("pv_id")],
        attributes=[
            _ts("released_at"),
            _measure("build_duration"),
            _attr("display_name"),
        ],
    )
    return {"ProductVersion": pv}


def _write_pivot(tmp_path, data: dict, name: str = "test_pivot") -> object:
    """Write a query YAML file and return its path."""
    data.setdefault("name", name)
    data.setdefault("entity", "ProductVersion")
    p = tmp_path / f"{name}.yaml"
    p.write_text(yaml.dump(data))
    return p


def _error_rules(errors) -> set[str]:
    return {e.rule for e in errors}


# ---------------------------------------------------------------------------
# TestWeightedRequiresAvg
# ---------------------------------------------------------------------------

class TestWeightedRequiresAvg:
    def test_weighted_requires_avg(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "m": {
                    "expr": "sum(build_duration)",
                    "window": {"method": "weighted", "unit": "month", "frame": 3},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "weighted_requires_avg" in _error_rules(errors)

    def test_weighted_with_avg_passes(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "m": {
                    "expr": "avg(build_duration)",
                    "window": {"method": "weighted", "unit": "month", "frame": 3},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "weighted_requires_avg" not in _error_rules(errors)


# ---------------------------------------------------------------------------
# TestWindowRequiresFrame
# ---------------------------------------------------------------------------

class TestWindowRequiresFrame:
    def test_simple_requires_frame(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "m": {
                    "expr": "avg(build_duration)",
                    "window": {"method": "simple", "unit": "month"},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "window_requires_frame" in _error_rules(errors)

    def test_cumulative_no_frame_ok(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "m": {
                    "expr": "sum(build_duration)",
                    "window": {"method": "cumulative", "unit": "month"},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "window_requires_frame" not in _error_rules(errors)


# ---------------------------------------------------------------------------
# TestLagLeadRequiresOffset
# ---------------------------------------------------------------------------

class TestLagLeadRequiresOffset:
    def test_lag_requires_offset(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "m": {
                    "expr": "avg(build_duration)",
                    "window": {"method": "lag", "unit": "month"},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "lag_lead_requires_offset" in _error_rules(errors)

    def test_lead_requires_offset(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "m": {
                    "expr": "avg(build_duration)",
                    "window": {"method": "lead", "unit": "month"},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "lag_lead_requires_offset" in _error_rules(errors)


# ---------------------------------------------------------------------------
# TestWindowUnitMustMatchSliceGrain
# ---------------------------------------------------------------------------

class TestWindowUnitMustMatchSliceGrain:
    def test_unit_grain_must_match_slice(self, tmp_path):
        # unit: month but no month slice (only week)
        path = _write_pivot(tmp_path, {
            "attributes": [{"week_key": 'format_time(released_at, "YYYY-WW")'}],
            "measures": [{
                "m": {
                    "expr": "avg(build_duration)",
                    "window": {"method": "simple", "unit": "month", "frame": 3},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "window_unit_must_match_slice_grain" in _error_rules(errors)

    def test_unit_grain_matches_slice_passes(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "m": {
                    "expr": "avg(build_duration)",
                    "window": {"method": "simple", "unit": "month", "frame": 3},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "window_unit_must_match_slice_grain" not in _error_rules(errors)


# ---------------------------------------------------------------------------
# TestRowsRequiresMultipleSlices
# ---------------------------------------------------------------------------

class TestRowsRequiresMultipleSlices:
    def test_rows_requires_multiple_slices(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "m": {
                    "expr": "avg(build_duration)",
                    "window": {"method": "simple", "unit": "rows", "frame": 3},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "window_rows_requires_multiple_slices" in _error_rules(errors)

    def test_rows_with_two_slices_passes(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [
                {"month_key": 'format_time(released_at, "YYYY-MM")'},
                {"disp": "display_name"},
            ],
            "measures": [{
                "m": {
                    "expr": "avg(build_duration)",
                    "window": {"method": "simple", "unit": "rows", "frame": 3},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "window_rows_requires_multiple_slices" not in _error_rules(errors)


# ---------------------------------------------------------------------------
# TestRatioValidation
# ---------------------------------------------------------------------------

class TestRatioValidation:
    def test_valid_ratio_passes(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{"pct": {"expr": "sum(build_duration) / count(pv_id)"}}],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert not errors

    def test_invalid_numerator_field(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{"pct": {"expr": "sum(nonexistent) / count(pv_id)"}}],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "invalid_ratio_component_field" in _error_rules(errors)
        assert any("numerator" in e.path for e in errors)

    def test_invalid_denominator_field(self, tmp_path):
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{"pct": {"expr": "sum(build_duration) / count(nonexistent)"}}],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "invalid_ratio_component_field" in _error_rules(errors)
        assert any("denominator" in e.path for e in errors)

    def test_ratio_not_subject_to_metric_field_rule(self, tmp_path):
        # A ratio metric with valid component fields must NOT raise invalid_metric_field
        path = _write_pivot(tmp_path, {
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{"pct": {"expr": "sum(build_duration) / count(pv_id)"}}],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert "invalid_metric_field" not in _error_rules(errors)
