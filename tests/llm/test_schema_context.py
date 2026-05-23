# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for schema_context.build_schema_context."""

from semantic.feature import FeatureDef
from semantic.llm.schema_context import build_schema_context
from semantic.types import (
    AttributeDef,
    Cardinality,
    ColumnType,
    EntityDef,
    RelationDef,
    SemanticHint,
)


def _attr(name: str, t: ColumnType = ColumnType.STRING, semantic: SemanticHint | None = None) -> AttributeDef:
    return AttributeDef(name=name, col_type=t, semantic=semantic)


def _entity(name: str, **kwargs) -> EntityDef:
    return EntityDef(name=name, **kwargs)


def test_renders_one_block_per_entity():
    entities = [
        _entity(
            "Widget",
            description="a sample widget",
            identity=[_attr("widget_key", semantic=SemanticHint.PRIMARY_KEY)],
            attributes=[_attr("colour"), _attr("size", ColumnType.INTEGER)],
        ),
        _entity("Gadget", identity=[_attr("gadget_key")]),
    ]
    out = build_schema_context("toy", entities)
    assert "MODEL: toy" in out
    assert "ENTITY Widget — a sample widget" in out
    assert "ENTITY Gadget" in out
    assert "colour:STRING" in out
    assert "size:INTEGER" in out


def test_renders_relations_with_cardinality():
    entities = [
        _entity(
            "Order",
            identity=[_attr("order_key")],
            relations=[
                RelationDef(name="customer", target="Customer", cardinality=Cardinality.MANY_TO_ONE),
                RelationDef(name="items", target="LineItem", cardinality=Cardinality.ONE_TO_MANY),
            ],
        ),
    ]
    out = build_schema_context("commerce", entities)
    assert "customer (N→1 Customer)" in out
    assert "items (1→N LineItem)" in out


def test_truncates_long_attribute_lists():
    attrs = [_attr(f"col{i}") for i in range(30)]
    entities = [_entity("Wide", identity=[_attr("k")], attributes=attrs)]
    out = build_schema_context("m", entities, max_attrs_per_entity=5)
    assert "col0:STRING" in out
    assert "col4:STRING" in out
    assert "col5:STRING" not in out
    assert "… (25 more)" in out


def test_includes_features_when_provided():
    entities = [_entity("PR", identity=[_attr("pr_key")])]
    feat = FeatureDef(
        feature_id="pr.days_open",
        entity_type="PR",
        data_type="INTEGER",
        name="days_open",
        entity_key="pr_key",
        materialization_mode="dynamic",
        raw_sql="datetime_diff(merged_at, created_at, unit='days')",
    )
    out = build_schema_context("dev", entities, {"PR": [feat]})
    assert "features: days_open" in out


def test_omits_features_line_when_none():
    entities = [_entity("PR", identity=[_attr("pr_key")])]
    out = build_schema_context("dev", entities)
    assert "features:" not in out


def test_truncates_long_feature_lists():
    entities = [_entity("PR", identity=[_attr("pr_key")])]
    feats = [
        FeatureDef(
            feature_id=f"pr.feat_{i}",
            name=f"feat_{i}",
            entity_type="PR",
            entity_key="pr_key",
            data_type="INTEGER",
            materialization_mode="dynamic",
            raw_sql="1",
        )
        for i in range(15)
    ]
    out = build_schema_context("dev", entities, {"PR": feats}, max_features_per_entity=3)
    assert "feat_0" in out
    assert "feat_2" in out
    assert "feat_3" not in out
    assert "… (12 more)" in out
