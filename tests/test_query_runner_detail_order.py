# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for ``build_detail_query`` — top-level ``order:`` handling.

Prior to the fix, ``order_by_clause`` in ``build_detail_query`` was
initialised to ``""`` and never populated from ``query.order_by``, so
YAML like::

  order:
    - calendar_date: asc

silently compiled to SQL with **no** ORDER BY clause at all. Rows came
back in whatever scan order DuckDB decided on, which for a features-join
query meant nothing close to sorted. The aggregate path (line 1218+ in
query_runner.py) had always handled it correctly; only the detail path
missed it. These tests pin the fix.
"""

from engine.query_runner import build_detail_query
from semantic.query import parse_query_dict
from semantic.types import (
    AttributeDef,
    ColumnType,
    EntityDef,
    SemanticHint,
)


def _widget_entity() -> EntityDef:
    """Minimal entity with a couple of non-key attributes so we have
    something to sort on. Kept unrelated to any concrete model — this
    test is about ORDER BY plumbing, not domain semantics."""
    return EntityDef(
        name="Widget",
        identity=[
            AttributeDef(
                name="widget_key",
                col_type=ColumnType.STRING,
                semantic=SemanticHint.PRIMARY_KEY,
            ),
        ],
        attributes=[
            AttributeDef(
                name="created_at",
                col_type=ColumnType.STRING,
                semantic=SemanticHint.TIMESTAMP,
            ),
            AttributeDef(
                name="score",
                col_type=ColumnType.FLOAT,
                semantic=SemanticHint.MEASURE,
            ),
        ],
        relations=[],
    )


def _detail_query(order_block: list | None = None):
    """Build a detail QueryDef with the given top-level order block."""
    data = {
        "name": "widget_detail",
        "root": "Widget",
        "attributes": ["widget_key", "created_at", "score"],
    }
    if order_block is not None:
        data["order"] = order_block
    return parse_query_dict(data)


def test_detail_query_no_order_block_produces_no_order_by():
    """Baseline: omit ``order:`` → the compiled SQL must have no ORDER BY.
    Guards against the fix over-correcting into "always emit ORDER BY"."""
    q = _detail_query(order_block=None)
    entities = {"Widget": _widget_entity()}
    sql = build_detail_query(q, entities, page=1, page_size=100)
    assert "ORDER BY" not in sql


def test_detail_query_single_column_asc_emits_order_by():
    """The bug: ``order: [{created_at: asc}]`` silently dropped. Now it
    must produce a real ORDER BY clause referencing the aliased column."""
    q = _detail_query(order_block=[{"created_at": "asc"}])
    entities = {"Widget": _widget_entity()}
    sql = build_detail_query(q, entities, page=1, page_size=100)
    # The SELECT alias for a bare attribute is "Entity.attr" — that's what
    # ORDER BY must reference, not the bare column name.
    assert 'ORDER BY "Widget.created_at" ASC' in sql


def test_detail_query_desc_direction_preserved():
    q = _detail_query(order_block=[{"score": "desc"}])
    entities = {"Widget": _widget_entity()}
    sql = build_detail_query(q, entities, page=1, page_size=100)
    assert 'ORDER BY "Widget.score" DESC' in sql


def test_detail_query_multiple_order_columns_join_with_comma():
    """Multi-column order: preserved in the order the user wrote them."""
    q = _detail_query(order_block=[{"score": "desc"}, {"created_at": "asc"}])
    entities = {"Widget": _widget_entity()}
    sql = build_detail_query(q, entities, page=1, page_size=100)
    assert (
        'ORDER BY "Widget.score" DESC, "Widget.created_at" ASC' in sql
    )


def test_detail_query_order_by_precedes_limit():
    """ORDER BY must appear before LIMIT — swapping them is a SQL syntax
    error. Cheap check that the line assembly puts the clause in the
    right slot."""
    q = _detail_query(order_block=[{"created_at": "asc"}])
    entities = {"Widget": _widget_entity()}
    sql = build_detail_query(q, entities, page=1, page_size=100)
    order_pos = sql.index("ORDER BY")
    limit_pos = sql.index("LIMIT")
    assert order_pos < limit_pos


def test_detail_query_order_by_fully_qualified_alias_resolves():
    """The user can also write ``order: [{Widget.created_at: asc}]`` —
    the fully-qualified form. Both should resolve to the same alias."""
    q = _detail_query(order_block=[{"Widget.created_at": "asc"}])
    entities = {"Widget": _widget_entity()}
    sql = build_detail_query(q, entities, page=1, page_size=100)
    assert 'ORDER BY "Widget.created_at" ASC' in sql
