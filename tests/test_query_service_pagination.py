# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for ``_compile_query_or_raise`` — YAML ``pagination.page_size``
must trump the caller's default when smaller.

Motivating case: a "top 5 leaderboard" query declares
``pagination.page_size: 5`` in the YAML, expecting the compiled SQL to
carry ``LIMIT 5``. Previously the API service always passed
``detail_page_size=1_000_000`` down to ``build_detail_query`` (the
"fetch all, paginate in Python" pattern) and the YAML value was
silently overridden. Top-N leaderboards ended up returning 163 rows
sorted, not 5.
"""

from api.services.query_service import _compile_query_or_raise
from tests.conftest import seed_artifact

_ENTITY_YAML = """\
entity: Widget
identity:
  widget_key:
    type: STRING
    semantic: PRIMARY_KEY
attributes:
  score:
    type: FLOAT
    semantic: MEASURE
"""


def test_yaml_page_size_smaller_than_default_wins(tmp_path, monkeypatch):
    """The top-N case: YAML says 5, caller default is 1M — SQL gets LIMIT 5."""
    monkeypatch.setattr(
        "core.model_config.load_database_config",
        lambda md: {"backend": "sqlite", "path": "data/data.db"},
    )
    seed_artifact(tmp_path, "entity", "Widget", _ENTITY_YAML)
    (tmp_path / "config.yaml").write_text("")

    from semantic.query import parse_query_dict
    q = parse_query_dict({
        "name": "top5_widgets",
        "root": "Widget",
        "attributes": ["widget_key", "score"],
        "order": [{"score": "desc"}],
        "pagination": {"page_size": 5},
    })

    sql = _compile_query_or_raise(q, tmp_path, detail_page_size=1_000_000)
    assert "LIMIT 5" in sql, sql


def test_yaml_page_size_larger_than_default_falls_back(tmp_path, monkeypatch):
    """If the YAML wants MORE than the caller allows (e.g. YAML says 500
    but caller passes 200), the caller's cap wins — we don't let the YAML
    override an operator-imposed page limit upward."""
    monkeypatch.setattr(
        "core.model_config.load_database_config",
        lambda md: {"backend": "sqlite", "path": "data/data.db"},
    )
    seed_artifact(tmp_path, "entity", "Widget", _ENTITY_YAML)
    (tmp_path / "config.yaml").write_text("")

    from semantic.query import parse_query_dict
    q = parse_query_dict({
        "name": "many_widgets",
        "root": "Widget",
        "attributes": ["widget_key", "score"],
        "pagination": {"page_size": 500},
    })

    sql = _compile_query_or_raise(q, tmp_path, detail_page_size=200)
    assert "LIMIT 200" in sql, sql
    assert "LIMIT 500" not in sql


def test_no_yaml_pagination_uses_caller_default(tmp_path, monkeypatch):
    """No pagination block → caller's default is preserved. The parser
    fills in ``PaginationDef.page_size = 100`` even without an explicit
    block; the ``< 100`` sentinel in the fix intentionally lets that
    default through so the "fetch all, paginate in Python" contract
    holds for queries that never opted in."""
    monkeypatch.setattr(
        "core.model_config.load_database_config",
        lambda md: {"backend": "sqlite", "path": "data/data.db"},
    )
    seed_artifact(tmp_path, "entity", "Widget", _ENTITY_YAML)
    (tmp_path / "config.yaml").write_text("")

    from semantic.query import parse_query_dict
    q = parse_query_dict({
        "name": "widgets",
        "root": "Widget",
        "attributes": ["widget_key", "score"],
    })

    sql = _compile_query_or_raise(q, tmp_path, detail_page_size=1_000_000)
    assert "LIMIT 1000000" in sql, sql


def test_yaml_page_size_100_falls_back_to_caller(tmp_path, monkeypatch):
    """A user explicitly writing ``pagination.page_size: 100`` gets the
    caller's cap, not their YAML value. The ``< 100`` sentinel means the
    parser default and an explicit-100 are indistinguishable — a
    deliberate trade-off documented in the code. Users who want exactly
    100 rows should pass it via the ``?limit=`` query param instead."""
    monkeypatch.setattr(
        "core.model_config.load_database_config",
        lambda md: {"backend": "sqlite", "path": "data/data.db"},
    )
    seed_artifact(tmp_path, "entity", "Widget", _ENTITY_YAML)
    (tmp_path / "config.yaml").write_text("")

    from semantic.query import parse_query_dict
    q = parse_query_dict({
        "name": "widgets",
        "root": "Widget",
        "attributes": ["widget_key", "score"],
        "pagination": {"page_size": 100},
    })

    sql = _compile_query_or_raise(q, tmp_path, detail_page_size=1_000_000)
    assert "LIMIT 1000000" in sql, sql
