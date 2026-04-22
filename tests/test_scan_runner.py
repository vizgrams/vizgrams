# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Unit and integration tests for engine/query_runner.py multi-hop join support (detail queries)."""

import pytest

from engine.query_runner import _build_count_query, _find_m2o_path, build_detail_query
from semantic.query import PaginationDef, QueryAttribute, QueryDef
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
    return AttributeDef(name=name, col_type=ColumnType.INTEGER, semantic=SemanticHint.RELATION, references=references)


def _attr(name: str) -> AttributeDef:
    return AttributeDef(name=name, col_type=ColumnType.STRING)


def _m2o(name: str, target: str, via: str) -> RelationDef:
    return RelationDef(name=name, target=target, via=via, cardinality=Cardinality.MANY_TO_ONE)


def _make_entities() -> dict[str, EntityDef]:
    """Build a 3-entity chain: ProductVersion → Product → Domain, plus an isolated entity."""
    domain = EntityDef(
        name="Domain",
        identity=[_pk("domain_id")],
        attributes=[_attr("display_name")],
    )
    product = EntityDef(
        name="Product",
        identity=[_pk("product_id"), _fk("domain_id", "Domain")],
        attributes=[_attr("display_name")],
        relations=[_m2o("domain", "Domain", "domain_id")],
    )
    product_version = EntityDef(
        name="ProductVersion",
        identity=[_pk("pv_id"), _fk("product_id", "Product")],
        attributes=[_attr("version"), _attr("build_duration")],
        relations=[_m2o("product", "Product", "product_id")],
    )
    isolated = EntityDef(
        name="Isolated",
        identity=[_pk("isolated_id")],
        attributes=[_attr("label")],
    )
    return {
        "ProductVersion": product_version,
        "Product": product,
        "Domain": domain,
        "Isolated": isolated,
    }


def _make_4hop_entities() -> dict[str, EntityDef]:
    """A → B → C → D chain for three-hop tests."""
    d = EntityDef(name="D", identity=[_pk("d_id")], attributes=[_attr("d_val")])
    c = EntityDef(
        name="C",
        identity=[_pk("c_id"), _fk("d_id", "D")],
        attributes=[_attr("c_val")],
        relations=[_m2o("d", "D", "d_id")],
    )
    b = EntityDef(
        name="B",
        identity=[_pk("b_id"), _fk("c_id", "C")],
        attributes=[_attr("b_val")],
        relations=[_m2o("c", "C", "c_id")],
    )
    a = EntityDef(
        name="A",
        identity=[_pk("a_id"), _fk("b_id", "B")],
        attributes=[_attr("a_val")],
        relations=[_m2o("b", "B", "b_id")],
    )
    return {"A": a, "B": b, "C": c, "D": d}


def _query(entity: str, attrs: list[str], filters: list[str] | None = None) -> QueryDef:
    return QueryDef(
        name="test_query",
        entity=entity,
        attributes=[QueryAttribute(parts=a.split(".")) for a in attrs],
        filters=filters or [],
        pagination=PaginationDef(),
    )


# ---------------------------------------------------------------------------
# TestFindM2OPath
# ---------------------------------------------------------------------------

class TestFindM2OPath:
    def test_direct_path(self):
        entities = _make_entities()
        result = _find_m2o_path("ProductVersion", "Product", entities)
        assert result == ["Product"]

    def test_two_hop_path(self):
        entities = _make_entities()
        result = _find_m2o_path("ProductVersion", "Domain", entities)
        assert result == ["Product", "Domain"]

    def test_no_path_returns_none(self):
        entities = _make_entities()
        # Domain has no outgoing MANY_TO_ONE to ProductVersion
        result = _find_m2o_path("Domain", "ProductVersion", entities)
        assert result is None

    def test_unknown_target_returns_none(self):
        entities = _make_entities()
        result = _find_m2o_path("ProductVersion", "Nonexistent", entities)
        assert result is None

    def test_same_entity_returns_empty(self):
        entities = _make_entities()
        result = _find_m2o_path("ProductVersion", "ProductVersion", entities)
        assert result == []


# ---------------------------------------------------------------------------
# TestBuildDetailQueryJoins
# ---------------------------------------------------------------------------

class TestBuildDetailQueryJoins:
    def _sql(self, entity: str, attrs: list[str], **kwargs) -> str:
        entities = _make_entities()
        query = _query(entity, attrs, **kwargs)
        return build_detail_query(query, entities, page=1, page_size=50)

    def test_direct_single_hop(self):
        sql = self._sql("ProductVersion", ["Product.display_name"])
        assert sql.count("LEFT JOIN") == 1
        assert "product" in sql

    def test_two_hop_join_emitted(self):
        sql = self._sql("ProductVersion", ["Domain.display_name"])
        assert sql.count("LEFT JOIN") == 2

    def test_two_hop_join_order(self):
        sql = self._sql("ProductVersion", ["Domain.display_name"])
        product_pos = sql.index("product")
        domain_pos = sql.index("domain")
        assert product_pos < domain_pos, "Product JOIN must appear before Domain JOIN"

    def test_two_hop_domain_alias_in_select(self):
        sql = self._sql("ProductVersion", ["Domain.display_name"])
        assert '"Domain.display_name"' in sql

    def test_deduplication_of_intermediate_join(self):
        """Product.display_name + Domain.display_name should produce exactly 2 JOINs."""
        entities = _make_entities()
        query = _query("ProductVersion", ["Product.display_name", "Domain.display_name"])
        sql = build_detail_query(query, entities, page=1, page_size=50)
        assert sql.count("LEFT JOIN") == 2

    def test_truly_unreachable_entity_raises(self):
        entities = _make_entities()
        query = _query("ProductVersion", ["Isolated.label"])
        with pytest.raises(ValueError, match="No MANY_TO_ONE relation path"):
            build_detail_query(query, entities, page=1, page_size=50)

    def test_count_query_two_hop(self):
        entities = _make_entities()
        query = _query("ProductVersion", ["Domain.display_name"])
        sql = _build_count_query(query, entities)
        assert sql.count("LEFT JOIN") == 2
        assert "COUNT(*)" in sql

    def test_history_condition_on_terminal_hop(self):
        """If Domain had SCD2 history, the final JOIN should include valid_to IS NULL."""
        from semantic.types import HistoryDef, HistoryType
        entities = _make_entities()
        entities["Domain"].history = HistoryDef(history_type=HistoryType.SCD2)
        query = _query("ProductVersion", ["Domain.display_name"])
        sql = build_detail_query(query, entities, page=1, page_size=50)
        # The Domain join should have the valid_to guard
        assert "valid_to IS NULL" in sql

    def test_three_hop_path(self):
        entities = _make_4hop_entities()
        query = _query("A", ["D.d_val"])
        sql = build_detail_query(query, entities, page=1, page_size=50)
        assert sql.count("LEFT JOIN") == 3
        # Order: B, C, D
        b_pos = sql.upper().index("JOIN B ")
        c_pos = sql.upper().index("JOIN C ")
        d_pos = sql.upper().index("JOIN D ")
        assert b_pos < c_pos < d_pos
