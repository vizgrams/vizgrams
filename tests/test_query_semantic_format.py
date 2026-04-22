# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for new attributes: + measures: list format query semantics."""

from pathlib import Path

import pytest
import yaml

from semantic.query import (
    QueryMetric,
    RatioMetric,
    WindowDef,
    _parse_attribute_item,
    _parse_measure_expr,
    _parse_measure_list_item,
    _parse_order,
    parse_query_dict,
    parse_query_yaml,
    validate_query_yaml,
)
from semantic.types import AttributeDef, ColumnType, EntityDef, SemanticHint

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _pk(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.INTEGER, semantic=SemanticHint.PRIMARY_KEY)


def _ts(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.STRING, semantic=SemanticHint.TIMESTAMP)


def _attr(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.STRING)


def _float(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.FLOAT)


def _make_entities() -> dict[str, EntityDef]:
    pv = EntityDef(
        name="ProductVersion",
        identity=[_pk("product_version_key")],
        attributes=[
            _ts("released_at"),
            _ts("created_at"),
            _float("build_duration"),
            _float("days_since_prev_version"),
            _float("is_lt_28d"),
        ],
    )
    return {"ProductVersion": pv}


def _write_query(tmp_path: Path, data: dict, name: str = "test_query") -> Path:
    data.setdefault("name", name)
    p = tmp_path / f"{name}.yaml"
    p.write_text(yaml.dump(data))
    return p


# ---------------------------------------------------------------------------
# _parse_order
# ---------------------------------------------------------------------------

class TestParseOrder:
    def test_position_and_asc(self):
        pos, direction = _parse_order("1, asc")
        assert pos == 1
        assert direction == "asc"

    def test_position_and_desc(self):
        pos, direction = _parse_order("2, desc")
        assert pos == 2
        assert direction == "desc"

    def test_position_only(self):
        pos, direction = _parse_order("3")
        assert pos == 3
        assert direction == "asc"

    def test_direction_only(self):
        pos, direction = _parse_order("desc")
        assert pos is None
        assert direction == "desc"

    def test_default(self):
        pos, direction = _parse_order("asc")
        assert pos is None
        assert direction == "asc"


# ---------------------------------------------------------------------------
# _parse_attribute_item
# ---------------------------------------------------------------------------

class TestParseAttributeItem:
    def test_plain_field(self):
        s = _parse_attribute_item({"week_key": "committed_at"})
        assert s.field == "committed_at"
        assert s.alias == "week_key"
        assert s.format_pattern is None
        assert s.order_position is None
        assert s.order_direction == "asc"

    def test_format_time_expression(self):
        s = _parse_attribute_item({"week_key": 'format_time(committed_at, "YYYY-WW")'})
        assert s.field == "committed_at"
        assert s.alias == "week_key"
        assert s.format_pattern == "YYYY-WW"
        assert s.inferred_grain == "week"

    def test_format_time_month(self):
        s = _parse_attribute_item({"month_key": 'format_time(released_at, "YYYY-MM")'})
        assert s.field == "released_at"
        assert s.alias == "month_key"
        assert s.format_pattern == "YYYY-MM"
        assert s.inferred_grain == "month"

    def test_with_order(self):
        s = _parse_attribute_item({"week_key": 'format_time(committed_at, "YYYY-WW")', "order": "1, asc"})
        assert s.order_position == 1
        assert s.order_direction == "asc"

    def test_with_order_desc(self):
        s = _parse_attribute_item({"identity": "is_authored_by.subject.name", "order": "2, desc"})
        assert s.field == "is_authored_by.subject.name"
        assert s.alias == "identity"
        assert s.order_position == 2
        assert s.order_direction == "desc"

    def test_traversal_field(self):
        s = _parse_attribute_item({"product": "Repository.Product.display_name"})
        assert s.field == "Repository.Product.display_name"
        assert s.alias == "product"

    def test_invalid_not_dict(self):
        with pytest.raises(ValueError, match="Attribute must be a dict"):
            _parse_attribute_item("plain_string")

    def test_invalid_missing_alias(self):
        with pytest.raises(ValueError, match="Attribute item must have exactly one alias key"):
            _parse_attribute_item({"order": "1, asc"})


# ---------------------------------------------------------------------------
# _parse_measure_expr
# ---------------------------------------------------------------------------

class TestParseMeasureExpr:
    def test_simple_avg(self):
        m = _parse_measure_expr("avg(build_duration)")
        assert isinstance(m, QueryMetric)
        assert m.field == "build_duration"
        assert m.rollup == "avg"
        assert m.window is None

    def test_simple_count(self):
        m = _parse_measure_expr("count(created_at)")
        assert isinstance(m, QueryMetric)
        assert m.field == "created_at"
        assert m.rollup == "count"

    def test_simple_sum(self):
        m = _parse_measure_expr("sum(is_lt_28d)")
        assert isinstance(m, QueryMetric)
        assert m.field == "is_lt_28d"
        assert m.rollup == "sum"

    def test_ratio(self):
        m = _parse_measure_expr("sum(is_lt_28d) / count(product_version_key)")
        assert isinstance(m, RatioMetric)
        assert m.numerator.field == "is_lt_28d"
        assert m.numerator.rollup == "sum"
        assert m.denominator.field == "product_version_key"
        assert m.denominator.rollup == "count"

    def test_with_window(self):
        window = WindowDef(method="weighted", unit="month", frame=3)
        m = _parse_measure_expr("avg(days_since_prev_version)", window)
        assert isinstance(m, QueryMetric)
        assert m.field == "days_since_prev_version"
        assert m.rollup == "avg"
        assert m.window == window

    def test_unsupported_non_agg_raises(self):
        with pytest.raises(ValueError, match="Unsupported measure expression"):
            _parse_measure_expr("build_duration + 1")


# ---------------------------------------------------------------------------
# _parse_measure_list_item
# ---------------------------------------------------------------------------

class TestParseMeasureListItem:
    def test_simple_metric(self):
        alias, m = _parse_measure_list_item({"commit_count": {"expr": "count(commit_sha)"}})
        assert alias == "commit_count"
        assert isinstance(m, QueryMetric)
        assert m.field == "commit_sha"
        assert m.rollup == "count"
        assert m.order_position is None
        assert m.format is None

    def test_with_format(self):
        alias, m = _parse_measure_list_item({
            "commit_count": {
                "expr": "count(commit_sha)",
                "format": {"type": "number", "pattern": "0"},
            }
        })
        assert alias == "commit_count"
        assert m.format is not None
        assert m.format.type == "number"
        assert m.format.pattern == "0"
        assert m.format.unit is None

    def test_with_format_duration(self):
        alias, m = _parse_measure_list_item({
            "avg_hrs": {
                "expr": "avg(fc_to_pr_open_hours)",
                "format": {"type": "duration", "unit": "hours", "pattern": "0.1"},
            }
        })
        assert m.format.type == "duration"
        assert m.format.unit == "hours"
        assert m.format.pattern == "0.1"

    def test_with_order(self):
        alias, m = _parse_measure_list_item({"commit_count": {"expr": "count(commit_sha)", "order": "3, asc"}})
        assert alias == "commit_count"
        assert m.order_position == 3
        assert m.order_direction == "asc"

    def test_with_window(self):
        alias, m = _parse_measure_list_item({
            "rolling": {
                "expr": "avg(days_since_prev_version)",
                "window": {"method": "weighted", "unit": "month", "frame": 3},
            },
        })
        assert alias == "rolling"
        assert isinstance(m, QueryMetric)
        assert m.window is not None
        assert m.window.method == "weighted"
        assert m.window.frame == 3

    def test_with_thresholds(self):
        alias, m = _parse_measure_list_item({
            "pct": {
                "expr": "sum(is_lt_28d) / count(product_version_key)",
                "thresholds": [
                    {"op": ">=", "value": 0.8, "status": "green"},
                    {"op": "<", "value": 0.8, "status": "red"},
                ],
            },
        })
        assert alias == "pct"
        assert isinstance(m, RatioMetric)
        assert len(m.thresholds) == 2

    def test_invalid_missing_expr(self):
        with pytest.raises(ValueError, match="must be a dict with an 'expr' key"):
            _parse_measure_list_item({"commit_count": "count(commit_sha)"})

    def test_invalid_multiple_keys(self):
        with pytest.raises(ValueError, match="exactly one alias key"):
            _parse_measure_list_item({"a": {"expr": "count(x)"}, "b": {"expr": "count(y)"}})


# ---------------------------------------------------------------------------
# parse_query_yaml — new format
# ---------------------------------------------------------------------------

class TestParseQueryYaml:
    def test_root_key_accepted(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"bd": "build_duration"}],
        })
        qd = parse_query_yaml(path)
        assert qd.entity == "ProductVersion"

    def test_attributes_as_slices(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [
                {"month_key": 'format_time(released_at, "YYYY-MM")', "order": "1, asc"},
            ],
            "measures": [{"cnt": {"expr": "count(build_duration)"}}],
        })
        qd = parse_query_yaml(path)
        assert len(qd.slices) == 1
        assert qd.slices[0].field == "released_at"
        assert qd.slices[0].alias == "month_key"
        assert qd.slices[0].format_pattern == "YYYY-MM"
        assert qd.slices[0].order_position == 1
        assert qd.slices[0].order_direction == "asc"

    def test_where_key_accepted(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "where": ["build_duration is not NULL"],
            "attributes": [{"bd": "build_duration"}],
        })
        qd = parse_query_yaml(path)
        assert qd.filters == ["build_duration is not NULL"]

    def test_measures_list_simple_agg(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{"avg_build": {"expr": "avg(build_duration)"}}],
        })
        qd = parse_query_yaml(path)
        m = qd.metrics["avg_build"]
        assert isinstance(m, QueryMetric)
        assert m.field == "build_duration"
        assert m.rollup == "avg"

    def test_measures_list_ratio(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{"pct_lt_28d": {"expr": "sum(is_lt_28d) / count(product_version_key)"}}],
        })
        qd = parse_query_yaml(path)
        m = qd.metrics["pct_lt_28d"]
        assert isinstance(m, RatioMetric)
        assert m.numerator.field == "is_lt_28d"
        assert m.denominator.field == "product_version_key"

    def test_measures_list_with_window(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "rolling": {
                    "expr": "avg(days_since_prev_version)",
                    "window": {"method": "weighted", "unit": "month", "frame": 3},
                },
            }],
        })
        qd = parse_query_yaml(path)
        m = qd.metrics["rolling"]
        assert isinstance(m, QueryMetric)
        assert m.field == "days_since_prev_version"
        assert m.rollup == "avg"
        assert m.window is not None
        assert m.window.method == "weighted"
        assert m.window.unit == "month"
        assert m.window.frame == 3

    def test_measures_list_with_thresholds(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "pct": {
                    "expr": "sum(is_lt_28d) / count(product_version_key)",
                    "thresholds": [
                        {"op": ">=", "value": 0.8, "status": "green"},
                        {"op": "<", "value": 0.8, "status": "red"},
                    ],
                },
            }],
        })
        qd = parse_query_yaml(path)
        m = qd.metrics["pct"]
        assert isinstance(m, RatioMetric)
        assert len(m.thresholds) == 2

    def test_measures_list_with_format(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "avg_build": {
                    "expr": "avg(build_duration)",
                    "format": {"type": "duration", "unit": "days", "pattern": "0.1"},
                }
            }],
        })
        qd = parse_query_yaml(path)
        m = qd.metrics["avg_build"]
        assert isinstance(m, QueryMetric)
        assert m.format is not None
        assert m.format.type == "duration"
        assert m.format.unit == "days"
        assert m.format.pattern == "0.1"

    def test_order_in_attributes(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [
                {"month_key": 'format_time(released_at, "YYYY-MM")', "order": "1, asc"},
                {"product": "Product.display_name"},
            ],
            "measures": [{"cnt": {"expr": "count(build_duration)"}}],
        })
        qd = parse_query_yaml(path)
        assert qd.slices[0].order_position == 1
        assert qd.slices[0].order_direction == "asc"
        assert qd.slices[1].order_position is None

    def test_order_in_measures(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{"cnt": {"expr": "count(build_duration)", "order": "2, desc"}}],
        })
        qd = parse_query_yaml(path)
        m = qd.metrics["cnt"]
        assert m.order_position == 2
        assert m.order_direction == "desc"

    def test_pv_build_duration_yaml_loads(self):
        """The migrated pv_build_duration query loads without errors and produces correct types."""
        import yaml as _yaml

        from core import metadata_db
        model_dir = Path(__file__).resolve().parent.parent / "models" / "example"
        metadata_db.seed_from_directory(model_dir)
        content = metadata_db.get_current_content(model_dir, "query", "pv_build_duration")
        assert content is not None, "pv_build_duration not found in metadata DB"
        qd = parse_query_dict(_yaml.safe_load(content))

        assert qd.name == "pv_build_duration"
        assert qd.entity == "ProductVersion"
        assert len(qd.slices) == 2
        assert qd.slices[0].field == "released_at"
        assert qd.slices[0].format_pattern == "YYYY-MM"
        assert qd.slices[0].alias == "month_key"
        assert qd.slices[1].field == "Product.display_name"
        assert qd.slices[1].alias == "product"

        assert isinstance(qd.metrics["avg_build_duration"], QueryMetric)
        assert qd.metrics["avg_build_duration"].field == "build_duration"
        assert qd.metrics["avg_build_duration"].rollup == "avg"

        rolling = qd.metrics["rolling_avg_days_since_prev_version"]
        assert isinstance(rolling, QueryMetric)
        assert rolling.window is not None
        assert rolling.window.method == "weighted"

        pct = qd.metrics["pct_versions_lt_28d"]
        assert isinstance(pct, RatioMetric)
        assert pct.numerator.field == "is_lt_28d"
        assert pct.denominator.field == "product_version_key"

        assert qd.filters == ["build_duration is not NULL"]

    def test_old_plain_string_attributes_still_work(self, tmp_path):
        """Plain string attributes (detail query format) still parse as QueryAttribute."""
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": ["build_duration", "released_at"],
        })
        qd = parse_query_yaml(path)
        assert len(qd.attributes) == 2
        assert qd.attributes[0].parts == ["build_duration"]
        assert qd.attributes[1].parts == ["released_at"]
        assert qd.slices == []


# ---------------------------------------------------------------------------
# validate_query_yaml — new format
# ---------------------------------------------------------------------------

class TestValidateQueryYaml:
    def test_root_key_passes_validation(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": ["build_duration"],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert errors == []

    def test_new_dict_attributes_pass_validation(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [
                {"month_key": 'format_time(released_at, "YYYY-MM")', "order": "1, asc"},
            ],
            "measures": [{"cnt": {"expr": "avg(build_duration)"}}],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert errors == []

    def test_where_key_passes_validation(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "where": ["build_duration is not NULL"],
            "attributes": ["build_duration"],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert errors == []

    def test_measures_list_simple_agg_passes_validation(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{"avg_build": {"expr": "avg(build_duration)"}}],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert errors == []

    def test_measures_list_ratio_passes_validation(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [
                {"pct_lt_28d": {"expr": "sum(is_lt_28d) / count(product_version_key)"}}
            ],
        })
        errors = validate_query_yaml(path, _make_entities())
        assert errors == []

    def test_measures_list_unknown_field_fails_validation(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{"bad": {"expr": "avg(nonexistent_field)"}}],
        })
        errors = validate_query_yaml(path, _make_entities())
        rules = {e.rule for e in errors}
        assert "invalid_metric_field" in rules

    def test_timestamp_without_grain_fails(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"ts": "released_at"}],
            "measures": [{"cnt": {"expr": "count(build_duration)"}}],
        })
        errors = validate_query_yaml(path, _make_entities())
        rules = {e.rule for e in errors}
        assert "timestamp_requires_grain" in rules

    def test_window_unit_must_match_slice_grain(self, tmp_path):
        path = _write_query(tmp_path, {
            "root": "ProductVersion",
            "attributes": [{"month_key": 'format_time(released_at, "YYYY-MM")'}],
            "measures": [{
                "rolling": {
                    "expr": "avg(days_since_prev_version)",
                    "window": {"method": "weighted", "unit": "week", "frame": 3},
                },
            }],
        })
        errors = validate_query_yaml(path, _make_entities())
        rules = {e.rule for e in errors}
        assert "window_unit_must_match_slice_grain" in rules
