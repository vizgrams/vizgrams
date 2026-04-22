# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/services/query_service.py."""

import types
from pathlib import Path

import pytest

from api.services.query_service import (
    _measures_dict,
    _order_by,
    get_query,
    validate_inline_query,
)

MODEL_DIR = Path(__file__).resolve().parent.parent / "models" / "example"

# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------

def _make_metric(rollup="count", field="", format_obj=None):
    return types.SimpleNamespace(
        rollup=rollup,
        field=field,
        format=format_obj,
        thresholds=None,
    )


def _make_format(type="number", pattern="0", unit=None):
    return types.SimpleNamespace(type=type, pattern=pattern, unit=unit)


# ---------------------------------------------------------------------------
# Unit tests: _measures_dict
# ---------------------------------------------------------------------------

class TestMeasuresDict:
    def test_empty_metrics_returns_empty_dict(self):
        q = types.SimpleNamespace(metrics={})
        assert _measures_dict(q) == {}

    def test_no_metrics_attr_returns_empty_dict(self):
        q = types.SimpleNamespace()
        assert _measures_dict(q) == {}

    def test_none_metrics_returns_empty_dict(self):
        q = types.SimpleNamespace(metrics=None)
        assert _measures_dict(q) == {}

    def test_single_metric_has_expr_key(self):
        q = types.SimpleNamespace(metrics={"pr_count": _make_metric(rollup="count", field="pull_request_key")})
        result = _measures_dict(q)
        assert "pr_count" in result
        assert "expr" in result["pr_count"]

    def test_expr_format_count_with_field(self):
        q = types.SimpleNamespace(metrics={"pr_count": _make_metric(rollup="count", field="pull_request_key")})
        result = _measures_dict(q)
        assert result["pr_count"]["expr"] == "count(pull_request_key)"

    def test_expr_format_sum_with_field(self):
        q = types.SimpleNamespace(metrics={"total": _make_metric(rollup="sum", field="value")})
        result = _measures_dict(q)
        assert result["total"]["expr"] == "sum(value)"

    def test_expr_format_count_no_field(self):
        q = types.SimpleNamespace(metrics={"cnt": _make_metric(rollup="count", field="")})
        result = _measures_dict(q)
        assert result["cnt"]["expr"] == "count(*)"

    def test_format_info_included_when_present(self):
        fmt = _make_format(type="number", pattern="0,0", unit=None)
        q = types.SimpleNamespace(metrics={"pr_count": _make_metric(format_obj=fmt)})
        result = _measures_dict(q)
        assert "format" in result["pr_count"]
        assert result["pr_count"]["format"]["type"] == "number"
        assert result["pr_count"]["format"]["pattern"] == "0,0"

    def test_format_info_absent_when_no_format(self):
        q = types.SimpleNamespace(metrics={"cnt": _make_metric()})
        result = _measures_dict(q)
        assert "format" not in result["cnt"]

    def test_multiple_metrics_all_included(self):
        q = types.SimpleNamespace(metrics={
            "a": _make_metric(rollup="count", field="x"),
            "b": _make_metric(rollup="sum", field="y"),
        })
        result = _measures_dict(q)
        assert set(result.keys()) == {"a", "b"}

    def test_cross_entity_field_preserved_in_expr(self):
        """Fields like PullRequestReviewComment.pull_request_key should appear verbatim."""
        q = types.SimpleNamespace(metrics={
            "review_comment_count": _make_metric(rollup="count", field="PullRequestReviewComment.pull_request_key"),
        })
        result = _measures_dict(q)
        assert result["review_comment_count"]["expr"] == "count(PullRequestReviewComment.pull_request_key)"

    def test_iterates_over_items_not_keys(self):
        """Regression: _measures_dict used to iterate over dict keys instead of .items()."""
        fmt = _make_format(type="duration", pattern=None, unit="minutes")
        metrics = {
            "m1": _make_metric(rollup="avg", field="f1", format_obj=fmt),
            "m2": _make_metric(rollup="count", field="f2"),
        }
        q = types.SimpleNamespace(metrics=metrics)
        result = _measures_dict(q)
        # Both keys present and each has expr — not string/char entries from iterating keys
        assert result["m1"]["expr"] == "avg(f1)"
        assert result["m2"]["expr"] == "count(f2)"
        assert "format" in result["m1"]
        assert "format" not in result["m2"]


# ---------------------------------------------------------------------------
# Unit tests: _order_by
# ---------------------------------------------------------------------------

class TestOrderBy:
    def test_empty_list_returns_empty(self):
        q = types.SimpleNamespace(order_by=[])
        assert _order_by(q) == []

    def test_no_order_by_attr_returns_empty(self):
        q = types.SimpleNamespace()
        assert _order_by(q) == []

    def test_none_order_by_returns_empty(self):
        q = types.SimpleNamespace(order_by=None)
        assert _order_by(q) == []

    def test_single_asc_tuple(self):
        q = types.SimpleNamespace(order_by=[("week_key", "ASC")])
        result = _order_by(q)
        assert result == [{"field": "week_key", "direction": "asc"}]

    def test_single_desc_tuple(self):
        q = types.SimpleNamespace(order_by=[("identity", "DESC")])
        result = _order_by(q)
        assert result == [{"field": "identity", "direction": "desc"}]

    def test_multiple_tuples_preserved_order(self):
        q = types.SimpleNamespace(order_by=[("week_key", "ASC"), ("identity", "DESC")])
        result = _order_by(q)
        assert result == [
            {"field": "week_key", "direction": "asc"},
            {"field": "identity", "direction": "desc"},
        ]

    def test_output_has_field_and_direction_keys(self):
        q = types.SimpleNamespace(order_by=[("col", "ASC")])
        result = _order_by(q)
        assert "field" in result[0]
        assert "direction" in result[0]

    def test_direction_lowercased(self):
        q = types.SimpleNamespace(order_by=[("x", "ASC"), ("y", "DESC")])
        result = _order_by(q)
        assert result[0]["direction"] == "asc"
        assert result[1]["direction"] == "desc"

    def test_regression_does_not_use_sorts_attr(self):
        """Regression: _order_by used to look for q.sorts which doesn't exist."""
        q = types.SimpleNamespace(order_by=[("col", "ASC")])
        # q has no .sorts; should not raise
        result = _order_by(q)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Integration tests: get_query response shape (using inline fixtures)
# ---------------------------------------------------------------------------

_MULTI_MEASURE_QUERY = """\
name: multi_measure_query
root: Product

attributes:
  - week_key: format_time(status, 'YYYY-wWW')
  - domain: Domain.display_name
  - region: status
  - owner: display_name

measures:
  - count_a:
      expr: count(product_key)
      format:
        type: number
        pattern: "0"
  - count_b:
      expr: count(product_key)
      format:
        type: number
        pattern: "0"
  - count_c:
      expr: count(product_key)
      format:
        type: number
        pattern: "0"
  - count_d:
      expr: count(product_key)
      format:
        type: number
        pattern: "0"
"""

_ORDERED_QUERY = """\
name: ordered_query
root: Product

attributes:
  - domain: Domain.display_name

measures:
  - cnt:
      expr: count(product_key)
      format:
        type: number
        pattern: "0"

order:
  - domain: asc
"""

_WHERE_QUERY = """\
name: where_query
root: Product

attributes:
  - status: status

measures:
  - cnt:
      expr: count(product_key)
      format:
        type: number
        pattern: "0"

where:
  - status == "active"
"""


@pytest.fixture
def inline_model_dir(tmp_path):
    """A minimal model dir with inline query files and example ontology seeded to DB."""
    from core.metadata_db import record_version, seed_from_directory

    # Seed queries into DB
    (tmp_path / "data").mkdir()
    record_version(tmp_path, "query", "multi_measure_query", _MULTI_MEASURE_QUERY)
    record_version(tmp_path, "query", "ordered_query", _ORDERED_QUERY)
    record_version(tmp_path, "query", "where_query", _WHERE_QUERY)
    # Seed entities from example model into this tmp model
    src_ontology = MODEL_DIR / "ontology"
    for path in sorted(src_ontology.glob("*.yaml")):
        content = path.read_text()
        record_version(tmp_path, "entity", path.stem, content)
    return tmp_path


class TestGetQueryShape:
    def test_multi_measure_query_measure_count(self, inline_model_dir):
        result = get_query(inline_model_dir, "multi_measure_query")
        assert len(result["measures"]) == 4

    def test_multi_measure_query_attributes_include_week_key(self, inline_model_dir):
        result = get_query(inline_model_dir, "multi_measure_query")
        attribute_aliases = [a["alias"] for a in result["attributes"]]
        assert "week_key" in attribute_aliases

    def test_query_order_by_key_exists(self, inline_model_dir):
        result = get_query(inline_model_dir, "multi_measure_query")
        assert "order_by" in result
        assert isinstance(result["order_by"], list)

    def test_ordered_query_order_by_non_empty(self, inline_model_dir):
        result = get_query(inline_model_dir, "ordered_query")
        assert len(result["order_by"]) >= 1

    def test_ordered_query_order_by_entry_has_field_and_direction(self, inline_model_dir):
        entry = get_query(inline_model_dir, "ordered_query")["order_by"][0]
        assert "field" in entry
        assert "direction" in entry

    def test_where_query_measures_non_empty(self, inline_model_dir):
        result = get_query(inline_model_dir, "where_query")
        assert len(result["measures"]) > 0

    def test_where_query_where_non_empty(self, inline_model_dir):
        result = get_query(inline_model_dir, "where_query")
        assert len(result["where"]) > 0

    def test_get_query_returns_name(self, inline_model_dir):
        result = get_query(inline_model_dir, "ordered_query")
        assert result["name"] == "ordered_query"

    def test_get_query_returns_root(self, inline_model_dir):
        result = get_query(inline_model_dir, "ordered_query")
        assert result["root"] == "Product"

    def test_get_query_returns_raw_yaml(self, inline_model_dir):
        result = get_query(inline_model_dir, "ordered_query")
        assert result["raw_yaml"] is not None
        assert len(result["raw_yaml"]) > 0

    def test_get_query_unknown_raises_key_error(self, inline_model_dir):
        with pytest.raises(KeyError):
            get_query(inline_model_dir, "nonexistent_query_xyz")


# ---------------------------------------------------------------------------
# Integration tests: roundtrip via validate_inline_query
# ---------------------------------------------------------------------------

def _all_query_names():
    queries_dir = MODEL_DIR / "queries"
    return [p.stem for p in sorted(queries_dir.glob("*.yaml"))]


@pytest.fixture
def _seed_example_model():
    """Seed example model YAML files into the test DB."""
    from core.metadata_db import seed_from_directory
    seed_from_directory(MODEL_DIR)


@pytest.mark.parametrize("query_name", _all_query_names())
def test_roundtrip_validate_inline_query(query_name, _seed_example_model):
    """get_query → validate_inline_query should always produce valid == True."""
    result = get_query(MODEL_DIR, query_name)
    raw_yaml = result["raw_yaml"]
    assert raw_yaml is not None, f"No raw_yaml returned for {query_name}"

    validation = validate_inline_query(MODEL_DIR, query_name, raw_yaml)
    assert validation["valid"] is True, (
        f"Query '{query_name}' failed validation: {validation['errors']}"
    )
    assert validation["errors"] == [], (
        f"Query '{query_name}' had errors: {validation['errors']}"
    )
