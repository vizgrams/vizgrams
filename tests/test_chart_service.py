# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for ``api.services.chart_service`` — the split/join seam that
lets a chart be authored as one YAML file while still storing as a
query + view pair. Round-trip correctness is the load-bearing property:
if split → join isn't a fixed point (modulo key order), the single-file
editor would silently drop or duplicate fields on every save.
"""

import textwrap

import yaml

from api.services.chart_service import join_chart_yaml, split_chart_yaml


def _norm(y: str) -> dict:
    """YAML dumps by key insertion order; comparisons should be
    order-independent."""
    return yaml.safe_load(y)


class TestSplitChartYaml:
    def test_query_and_view_fields_route_correctly(self):
        chart = textwrap.dedent("""
            name: top5
            type: table
            root: Activity
            attributes: [calendar_date, tss]
            where: ["activity_type == 'run'"]
            order: [{tss: desc}]
            pagination: {page_size: 5}
            visualization:
              columns: [calendar_date, tss]
        """)
        q, v = split_chart_yaml(chart, "top5")
        q_d, v_d = _norm(q), _norm(v)

        assert v_d == {
            "name": "top5",
            "type": "table",
            "visualization": {"columns": ["calendar_date", "tss"]},
            "query": "top5",
        }
        assert q_d == {
            "name": "top5",
            "root": "Activity",
            "attributes": ["calendar_date", "tss"],
            "where": ["activity_type == 'run'"],
            "order": [{"tss": "desc"}],
            "pagination": {"page_size": 5},
        }

    def test_view_query_reference_preserved(self):
        """When the chart YAML uses ``query: <name>`` to point at an
        external query, the split emits an empty query body and a view
        that keeps the reference — supports the 'promote to shared
        query' case without introducing a separate path."""
        chart = "name: dashboard_chart\ntype: line\nquery: shared_query\nvisualization: {x: date}\n"
        q, v = split_chart_yaml(chart, "dashboard_chart")
        v_d = _norm(v)
        assert v_d["query"] == "shared_query"
        # Query body is just {name: ...} — the referenced query owns everything
        assert _norm(q) == {"name": "dashboard_chart"}

    def test_non_mapping_root_raises(self):
        """A stray list at the top level would silently coerce to an empty
        query if not caught, dropping all the user's work on save."""
        import pytest
        with pytest.raises(ValueError, match="mapping at the top level"):
            split_chart_yaml("- foo\n- bar\n", "x")

    def test_empty_yaml_yields_bare_pair(self):
        """Editor starting from an empty template should still produce a
        valid split — both files carry the name and nothing else."""
        q, v = split_chart_yaml("", "empty")
        assert _norm(q) == {"name": "empty"}
        assert _norm(v) == {"name": "empty", "query": "empty"}


class TestJoinChartYaml:
    def test_composes_query_plus_view(self):
        query_y = "name: c\nroot: A\nattributes: [x]\n"
        view_y = "name: c\ntype: table\nquery: c\nvisualization: {columns: [x]}\n"
        result = _norm(join_chart_yaml(query_y, view_y))
        assert result == {
            "name": "c", "root": "A", "attributes": ["x"],
            "type": "table", "visualization": {"columns": ["x"]},
        }

    def test_drops_synthetic_query_reference(self):
        """The ``query: <name>`` field is an implementation detail of the
        split — it's a self-reference that just clutters the editor."""
        result = _norm(join_chart_yaml("name: c\n", "name: c\nquery: c\ntype: bar\n"))
        assert "query" not in result

    def test_missing_query_or_view_still_composes(self):
        """A partially-saved chart (e.g. query written, view save failed
        mid-transaction) shouldn't produce an unreadable response."""
        assert _norm(join_chart_yaml(None, "name: c\ntype: bar\nquery: c\n")) \
            == {"name": "c", "type": "bar"}
        assert _norm(join_chart_yaml("name: c\nroot: A\n", None)) \
            == {"name": "c", "root": "A"}


class TestRoundTrip:
    def test_split_then_join_is_a_fixed_point(self):
        """The critical safety property: authoring a chart, saving it,
        then loading it back should give the same YAML (modulo key
        insertion order)."""
        chart_in = textwrap.dedent("""
            name: my_chart
            type: line
            root: Activity
            attributes: [calendar_date, tss]
            where: [always_true]
            visualization:
              x: calendar_date
              y: [tss]
              column_formats:
                tss: {type: number, pattern: '0'}
        """)
        q, v = split_chart_yaml(chart_in, "my_chart")
        chart_out = _norm(join_chart_yaml(q, v))
        assert chart_out == _norm(chart_in)
