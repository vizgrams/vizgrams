# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""SQL generation tests for engine/query_runner.py windowed aggregate query support."""


from engine.query_runner import (
    _entity_qualified_alias,
    _slice_col_alias,
    _window_frame_sql,
    build_aggregate_query,
)
from semantic.query import (
    QueryAttribute,
    QueryDef,
    QueryMetric,
    RatioComponent,
    RatioMetric,
    SliceDef,
    WindowDef,
)
from semantic.types import (
    AttributeDef,
    Cardinality,
    ColumnType,
    EntityDef,
    RelationDef,
    SemanticHint,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _pk(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.INTEGER, semantic=SemanticHint.PRIMARY_KEY)


def _fk(name: str, references: str) -> AttributeDef:
    return AttributeDef(
        name=name, col_type=ColumnType.INTEGER,
        semantic=SemanticHint.RELATION, references=references,
    )


def _ts(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.STRING, semantic=SemanticHint.TIMESTAMP)


def _measure(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.FLOAT)


def _m2o(name: str, target: str, via: str) -> RelationDef:
    return RelationDef(name=name, target=target, via=via, cardinality=Cardinality.MANY_TO_ONE)


def _make_entities() -> dict[str, EntityDef]:
    """ProductVersion (released_at TIMESTAMP, build_duration FLOAT) → Product (display_name)."""
    product = EntityDef(
        name="Product",
        identity=[_pk("product_id")],
        attributes=[AttributeDef(name="display_name", col_type=ColumnType.STRING)],
    )
    product_version = EntityDef(
        name="ProductVersion",
        identity=[_pk("pv_id"), _fk("product_id", "Product")],
        attributes=[_ts("released_at"), _measure("build_duration")],
        relations=[_m2o("product", "Product", "product_id")],
    )
    return {"ProductVersion": product_version, "Product": product}


def _attr(field: str) -> QueryAttribute:
    return QueryAttribute(parts=field.split("."))


def _pivot(
    slices: list[SliceDef],
    metrics: dict[str, QueryMetric],
    sort: list | None = None,
    attributes: list[QueryAttribute] | None = None,
) -> QueryDef:
    return QueryDef(
        name="test_pivot",
        entity="ProductVersion",
        slices=slices,
        metrics=metrics,
        attributes=attributes or [],
    )


def _slice(field: str, grain: str | None = None) -> SliceDef:
    """Create a SliceDef. grain maps to format_pattern for backward compat in tests."""
    _grain_to_pattern = {
        "day": "YYYY-MM-DD",
        "week": "YYYY-WW",
        "month": "YYYY-MM",
        "quarter": "YYYY-Q",
    }
    format_pattern = _grain_to_pattern.get(grain) if grain else None
    return SliceDef(field=field, format_pattern=format_pattern)


def _metric(field: str, rollup: str, window: WindowDef | None = None) -> QueryMetric:
    return QueryMetric(field=field, rollup=rollup, window=window)


def _window(method: str, unit: str, frame: int | None = None, offset: int | None = None) -> WindowDef:
    return WindowDef(method=method, unit=unit, frame=frame, offset=offset)


# ---------------------------------------------------------------------------
# TestWindowFrameSql
# ---------------------------------------------------------------------------

class TestWindowFrameSql:
    def test_weighted_frame_3(self):
        w = _window("weighted", "month", frame=3)
        assert _window_frame_sql(w) == "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"

    def test_simple_frame_3(self):
        w = _window("simple", "month", frame=3)
        assert _window_frame_sql(w) == "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"

    def test_simple_frame_1(self):
        w = _window("simple", "month", frame=1)
        assert _window_frame_sql(w) == "ROWS BETWEEN 0 PRECEDING AND CURRENT ROW"

    def test_cumulative_unbounded(self):
        w = _window("cumulative", "month")
        assert _window_frame_sql(w) == "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"

    def test_lag_returns_empty(self):
        w = _window("lag", "month", offset=1)
        assert _window_frame_sql(w) == ""

    def test_lead_returns_empty(self):
        w = _window("lead", "month", offset=2)
        assert _window_frame_sql(w) == ""


# ---------------------------------------------------------------------------
# TestBuildWindowedAggregateQuery
# ---------------------------------------------------------------------------

class TestBuildWindowedAggregateQuery:
    def _sql(self, slices, metrics, sort=None, attributes=None):
        entities = _make_entities()
        pv = _pivot(slices, metrics, sort, attributes=attributes)
        return build_aggregate_query(pv, entities)

    def test_non_windowed_uses_flat_path(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"total": _metric("build_duration", "sum")},
        )
        assert "WITH base AS" not in sql

    def test_windowed_uses_cte(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"avg_dur": _metric("build_duration", "avg", _window("simple", "month", frame=3))},
        )
        assert "WITH base AS" in sql

    def test_weighted_helper_cols_in_cte(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"avg_dur": _metric("build_duration", "avg", _window("weighted", "month", frame=3))},
        )
        assert "__avg_dur_sum" in sql
        assert "__avg_dur_count" in sql

    def test_weighted_outer_divides(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"avg_dur": _metric("build_duration", "avg", _window("weighted", "month", frame=3))},
        )
        assert "SUM(__avg_dur_sum)" in sql
        assert "SUM(__avg_dur_count)" in sql
        assert " / " in sql

    def test_simple_method_applies_window_fn(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"avg_dur": _metric("build_duration", "avg", _window("simple", "month", frame=3))},
        )
        assert "AVG(__avg_dur)" in sql
        assert "OVER (" in sql

    def test_cumulative_unbounded(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"cum_dur": _metric("build_duration", "sum", _window("cumulative", "month"))},
        )
        assert "UNBOUNDED PRECEDING" in sql

    def test_lag_function(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"prev_dur": _metric("build_duration", "avg", _window("lag", "month", offset=1))},
        )
        assert "LAG(__prev_dur, 1)" in sql

    def test_lead_function(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"next_dur": _metric("build_duration", "avg", _window("lead", "month", offset=2))},
        )
        assert "LEAD(__next_dur, 2)" in sql

    def test_partition_by_non_order_slice(self):
        sql = self._sql(
            slices=[
                _slice("released_at", "month"),
                _slice("Product.display_name"),
            ],
            metrics={
                "avg_dur": _metric(
                    "build_duration", "avg",
                    _window("simple", "month", frame=3),
                )
            },
        )
        assert "PARTITION BY" in sql
        assert '"Product.display_name"' in sql

    def test_order_by_time_grain_slice(self):
        sql = self._sql(
            slices=[
                _slice("released_at", "month"),
                _slice("Product.display_name"),
            ],
            metrics={
                "avg_dur": _metric(
                    "build_duration", "avg",
                    _window("simple", "month", frame=3),
                )
            },
        )
        assert 'ORDER BY "ProductVersion.released_at"' in sql

    def test_order_by_last_slice_for_rows_unit(self):
        sql = self._sql(
            slices=[
                _slice("Product.display_name"),
                _slice("released_at", "month"),
            ],
            metrics={
                "avg_dur": _metric(
                    "build_duration", "avg",
                    _window("simple", "rows", frame=3),
                )
            },
        )
        assert 'ORDER BY "ProductVersion.released_at"' in sql
        assert 'PARTITION BY "Product.display_name"' in sql

    def test_attributes_passed_through_in_outer_select(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"avg_dur": _metric("build_duration", "avg", _window("simple", "month", frame=3))},
            attributes=[_attr("build_duration")],
        )
        assert '"ProductVersion.build_duration"' in sql

    def test_mixed_windowed_and_flat_metric(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={
                "total": _metric("build_duration", "sum"),
                "rolling_avg": _metric("build_duration", "avg", _window("simple", "month", frame=3)),
            },
        )
        assert "WITH base AS" in sql
        assert "SUM(" in sql
        assert "OVER (" in sql
        assert "total" in sql


# ---------------------------------------------------------------------------
# TestQueryAttributes
# ---------------------------------------------------------------------------

class TestQueryAttributes:
    def _sql(self, slices, metrics, attributes=None):
        entities = _make_entities()
        pv = _pivot(slices, metrics, attributes=attributes or [])
        return build_aggregate_query(pv, entities)

    def test_bare_attribute_in_flat_select(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"total": _metric("build_duration", "sum")},
            attributes=[_attr("build_duration")],
        )
        assert '"ProductVersion.build_duration"' in sql

    def test_bare_attribute_in_flat_group_by(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"total": _metric("build_duration", "sum")},
            attributes=[_attr("build_duration")],
        )
        assert "GROUP BY" in sql
        group_by_section = sql[sql.index("GROUP BY"):]
        assert "build_duration" in group_by_section

    def test_root_entity_qualified_attribute(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"total": _metric("build_duration", "sum")},
            attributes=[_attr("ProductVersion.build_duration")],
        )
        assert '"ProductVersion.build_duration"' in sql

    def test_traversal_attribute_adds_join(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"total": _metric("build_duration", "sum")},
            attributes=[_attr("Product.display_name")],
        )
        assert "LEFT JOIN product" in sql
        assert '"Product.display_name"' in sql

    def test_traversal_attribute_deduplicates_join_with_slice(self):
        sql = self._sql(
            slices=[_slice("released_at", "month"), _slice("Product.display_name")],
            metrics={"total": _metric("build_duration", "sum")},
            attributes=[_attr("Product.display_name")],
        )
        assert sql.count("LEFT JOIN product") == 1

    def test_no_attributes_unchanged(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"total": _metric("build_duration", "sum")},
        )
        assert "WITH base AS" not in sql

    def test_attribute_in_windowed_base_cte(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"avg_dur": _metric("build_duration", "avg", _window("simple", "month", frame=3))},
            attributes=[_attr("build_duration")],
        )
        base_section = sql[:sql.index("SELECT\n  \"")]
        assert "build_duration" in base_section

    def test_attribute_in_windowed_outer_select(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"avg_dur": _metric("build_duration", "avg", _window("simple", "month", frame=3))},
            attributes=[_attr("build_duration")],
        )
        assert '"ProductVersion.build_duration"' in sql

    def test_attribute_does_not_affect_window_partition(self):
        sql = self._sql(
            slices=[
                _slice("released_at", "month"),
                _slice("Product.display_name"),
            ],
            metrics={"avg_dur": _metric("build_duration", "avg", _window("simple", "month", frame=3))},
            attributes=[_attr("build_duration")],
        )
        assert 'PARTITION BY "Product.display_name"' in sql
        over_start = sql.index("OVER (")
        over_end = sql.index(")", over_start)
        over_clause = sql[over_start:over_end]
        assert "build_duration" not in over_clause


# ---------------------------------------------------------------------------
# TestRatioMetrics
# ---------------------------------------------------------------------------

def _ratio(num_field: str, num_rollup: str, den_field: str, den_rollup: str) -> RatioMetric:
    return RatioMetric(
        numerator=RatioComponent(field=num_field, rollup=num_rollup),
        denominator=RatioComponent(field=den_field, rollup=den_rollup),
    )


class TestRatioMetrics:
    def _sql(self, slices, metrics, sort=None):
        entities = _make_entities()
        pv = _pivot(slices, metrics, sort)
        return build_aggregate_query(pv, entities)

    def test_flat_ratio_uses_cast_division(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"pct": _ratio("build_duration", "sum", "pv_id", "count")},
        )
        assert "CAST(" in sql
        assert "AS REAL)" in sql
        assert " / " in sql

    def test_flat_ratio_no_cte(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"pct": _ratio("build_duration", "sum", "pv_id", "count")},
        )
        assert "WITH base AS" not in sql

    def test_flat_ratio_aliases_metric_name(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"pct": _ratio("build_duration", "sum", "pv_id", "count")},
        )
        assert "AS pct" in sql

    def test_windowed_ratio_helpers_in_base_cte(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={
                "pct": _ratio("build_duration", "sum", "pv_id", "count"),
                "rolling_avg": _metric("build_duration", "avg", _window("simple", "month", frame=3)),
            },
        )
        assert "__pct_num" in sql
        assert "__pct_den" in sql

    def test_windowed_ratio_helpers_use_cast(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={
                "pct": _ratio("build_duration", "sum", "pv_id", "count"),
                "rolling_avg": _metric("build_duration", "avg", _window("simple", "month", frame=3)),
            },
        )
        assert "CAST(" in sql
        assert "AS REAL) AS __pct_num" in sql

    def test_windowed_ratio_outer_divides(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={
                "pct": _ratio("build_duration", "sum", "pv_id", "count"),
                "rolling_avg": _metric("build_duration", "avg", _window("simple", "month", frame=3)),
            },
        )
        assert "__pct_num / __pct_den AS pct" in sql

    def test_ratio_alone_does_not_trigger_cte(self):
        sql = self._sql(
            slices=[_slice("released_at", "month")],
            metrics={"pct": _ratio("build_duration", "sum", "pv_id", "count")},
        )
        assert "WITH base AS" not in sql


# ---------------------------------------------------------------------------
# TestDynamicRelationWithM2O — regression for dynamic + multi-hop M2O slices
# ---------------------------------------------------------------------------

def _make_commit_entities() -> dict[str, EntityDef]:
    """Commit → Identity (is_authored_by) → dynamic(subject_type) + Commit → Repository → Product."""
    person = EntityDef(
        name="Person",
        identity=[_pk("person_key")],
        attributes=[AttributeDef(name="name", col_type=ColumnType.STRING)],
    )
    machine = EntityDef(
        name="Machine",
        identity=[_pk("machine_key")],
        attributes=[AttributeDef(name="name", col_type=ColumnType.STRING)],
    )
    identity = EntityDef(
        name="Identity",
        identity=[_pk("identity_key")],
        attributes=[
            AttributeDef(name="subject_type", col_type=ColumnType.STRING),
            AttributeDef(name="subject_key", col_type=ColumnType.STRING),
        ],
        relations=[
            RelationDef(
                name="subject",
                target="",
                via="subject_key",
                cardinality=Cardinality.MANY_TO_ONE,
                dynamic_field="subject_type",
            ),
        ],
    )
    product = EntityDef(
        name="Product",
        identity=[_pk("product_key")],
        attributes=[AttributeDef(name="display_name", col_type=ColumnType.STRING)],
    )
    repository = EntityDef(
        name="Repository",
        identity=[_pk("repository_key"), _fk("product_key", "Product")],
        attributes=[],
        relations=[_m2o("product", "Product", "product_key")],
    )
    commit = EntityDef(
        name="Commit",
        identity=[_pk("commit_sha"), _fk("author_identity_key", "Identity")],
        attributes=[
            _ts("committed_at"),
            _fk("repository_key", "Repository"),
        ],
        relations=[
            RelationDef(
                name="is_authored_by",
                target="Identity",
                via="author_identity_key",
                via_target="identity_key",
                cardinality=Cardinality.MANY_TO_ONE,
            ),
            _m2o("belongs_to", "Repository", "repository_key"),
        ],
    )
    return {
        "Commit": commit,
        "Identity": identity,
        "Person": person,
        "Machine": machine,
        "Repository": repository,
        "Product": product,
    }


def _commit_query(slices: list[SliceDef], metrics: dict) -> QueryDef:
    return QueryDef(
        name="test_commit_query",
        entity="Commit",
        slices=slices,
        metrics=metrics,
        attributes=[],
    )


class TestDynamicRelationWithM2O:
    """Regression tests: dynamic multi-hop slice + MANY_TO_ONE multi-hop slice must not interfere."""

    def _sql(self, slices: list[SliceDef]) -> str:
        entities = _make_commit_entities()
        q = _commit_query(slices, {"commit_count": _metric("commit_sha", "count")})
        return build_aggregate_query(q, entities)

    def test_dynamic_slice_col_alias_is_relation_name_not_entity_name(self):
        """_slice_col_alias for is_authored_by.subject.name must return 'subject.name',
        NOT 'Identity.name' — the relation qualifier, not the intermediate entity name."""
        entities = _make_commit_entities()
        root = entities["Commit"]
        sd = SliceDef(field="is_authored_by.subject.name")
        assert _slice_col_alias(sd, root, entities) == "subject.name"

    def test_dynamic_slice_alone_produces_correct_sql_alias(self):
        sql = self._sql([_slice("is_authored_by.subject.name")])
        assert 'AS "subject.name"' in sql

    def test_dynamic_slice_not_aliased_as_entity_name(self):
        """Regression: must not produce 'Identity.name' as the alias for a dynamic relation field."""
        sql = self._sql([_slice("is_authored_by.subject.name")])
        assert 'AS "Identity.name"' not in sql

    def test_dynamic_slice_generates_person_join(self):
        sql = self._sql([_slice("is_authored_by.subject.name")])
        assert "person" in sql

    def test_dynamic_slice_generates_machine_join(self):
        sql = self._sql([_slice("is_authored_by.subject.name")])
        assert "machine" in sql

    def test_m2o_multihop_slice_col_alias(self):
        """Repository.Product.display_name → 'Product.display_name'."""
        entities = _make_commit_entities()
        root = entities["Commit"]
        sd = SliceDef(field="Repository.Product.display_name")
        assert _slice_col_alias(sd, root, entities) == "Product.display_name"

    def test_m2o_multihop_generates_correct_sql_alias(self):
        sql = self._sql([_slice("Repository.Product.display_name")])
        assert 'AS "Product.display_name"' in sql

    def test_both_slices_present_dynamic_alias_correct(self):
        """Core regression: adding Repository.Product.display_name must not drop subject.name."""
        sql = self._sql([
            _slice("is_authored_by.subject.name"),
            _slice("committed_at", "month"),
            _slice("Repository.Product.display_name"),
        ])
        assert 'AS "subject.name"' in sql

    def test_both_slices_present_m2o_alias_correct(self):
        sql = self._sql([
            _slice("is_authored_by.subject.name"),
            _slice("committed_at", "month"),
            _slice("Repository.Product.display_name"),
        ])
        assert 'AS "Product.display_name"' in sql

    def test_both_slices_present_dynamic_not_aliased_as_identity(self):
        """Regression guard: dynamic column must NEVER be aliased as Identity.name."""
        sql = self._sql([
            _slice("is_authored_by.subject.name"),
            _slice("committed_at", "month"),
            _slice("Repository.Product.display_name"),
        ])
        assert 'AS "Identity.name"' not in sql

    def test_reversed_order_dynamic_alias_still_correct(self):
        """Order of slices must not affect aliases — dynamic slice last."""
        sql = self._sql([
            _slice("Repository.Product.display_name"),
            _slice("committed_at", "month"),
            _slice("is_authored_by.subject.name"),
        ])
        assert 'AS "subject.name"' in sql
        assert 'AS "Identity.name"' not in sql

    def test_both_slices_present_all_joins_included(self):
        """All required JOINs must be present: Identity, Person/Machine, Repository, Product."""
        sql = self._sql([
            _slice("is_authored_by.subject.name"),
            _slice("committed_at", "month"),
            _slice("Repository.Product.display_name"),
        ])
        assert "identity" in sql
        assert "person" in sql
        assert "machine" in sql
        assert "repository" in sql
        assert "product" in sql

    def test_entity_qualified_alias_for_dynamic_via_intermediate_relation(self):
        """_entity_qualified_alias: two-hop with dynamic at end → relation name as qualifier."""
        entities = _make_commit_entities()
        root = entities["Commit"]
        alias = _entity_qualified_alias(["is_authored_by", "subject", "name"], root, entities)
        assert alias == "subject.name"

    def test_entity_qualified_alias_direct_dynamic_relation(self):
        """_entity_qualified_alias: direct dynamic relation slice."""
        entities = _make_commit_entities()
        root = entities["Identity"]
        alias = _entity_qualified_alias(["subject", "name"], root, entities)
        assert alias == "subject.name"
