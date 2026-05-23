# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for semantic/llm/embeddings/index."""

from __future__ import annotations

from semantic.llm.embeddings.index import build_embedding_text, index_now

# ---------------------------------------------------------------------------
# build_embedding_text — per-kind text builders
# ---------------------------------------------------------------------------


def test_query_text_includes_root_measures_filters():
    content = (
        "name: pr_count\n"
        "description: weekly count of merged pull requests\n"
        "root: PullRequest\n"
        "measures:\n"
        "  - count_merged: count(pull_request_key)\n"
        "where:\n"
        "  - state == 'merged'\n"
    )
    text = build_embedding_text("query", "pr_count", content)
    assert "query pr_count" in text
    assert "weekly count" in text
    assert "PullRequest" in text
    assert "count_merged" in text
    assert "merged" in text


def test_query_text_renders_measures_as_alias_equals_expr():
    """LLMs need both the alias AND the underlying field — render both."""
    content = (
        "name: dora_clt_trend\n"
        "root: PullRequest\n"
        "measures:\n"
        "  - avg_clt_prd:\n"
        "      expr: avg(change_lead_time_prd)\n"
        "  - avg_merge_time:\n"
        "      expr: avg(merge_time)\n"
    )
    text = build_embedding_text("query", "dora_clt_trend", content)
    # Both alias and inner field must appear so the LLM can extract field paths
    assert "avg_clt_prd=avg(change_lead_time_prd)" in text
    assert "avg_merge_time=avg(merge_time)" in text


def test_query_text_falls_back_to_bare_name_when_no_expr():
    """Some queries define measures via the legacy flat shape (no `expr:` key)."""
    content = (
        "name: legacy\n"
        "root: X\n"
        "measures:\n"
        "  - just_a_name: count(some_col)\n"
    )
    text = build_embedding_text("query", "legacy", content)
    # No nested expr → fall back to just the alias rather than rendering oddly
    assert "just_a_name" in text


def test_view_text_includes_chart_type_and_query():
    content = (
        "name: pr_trend\n"
        "type: chart\n"
        "query: pr_count\n"
        "visualization:\n"
        "  chart_type: line\n"
        "  x: week\n"
        "  y: [count]\n"
    )
    text = build_embedding_text("view", "pr_trend", content)
    assert "view pr_trend" in text
    assert "chart line" in text
    assert "on query pr_count" in text


def test_feature_text_includes_entity_and_expression():
    content = (
        "feature_id: pull_request.days_open\n"
        "entity_type: PullRequest\n"
        "description: days between creation and merge\n"
        "expr: datetime_diff(merged_at, created_at, unit='days')\n"
    )
    text = build_embedding_text("feature", "days_open", content)
    assert "feature pull_request.days_open" in text
    assert "PullRequest" in text
    assert "days between" in text
    assert "datetime_diff" in text


def test_entity_text_includes_attributes_and_relations():
    content = (
        "name: PullRequest\n"
        "description: a pull request\n"
        "identity:\n"
        "  pull_request_key: {type: STRING}\n"
        "attributes:\n"
        "  title: {type: STRING}\n"
        "  state: {type: STRING}\n"
        "relations:\n"
        "  author: {target: Identity, via: author_identity_key}\n"
    )
    text = build_embedding_text("entity", "PullRequest", content)
    assert "entity PullRequest" in text
    assert "pull_request_key" in text or "title" in text
    assert "author" in text


def test_application_text_includes_member_views():
    content = (
        "name: dora_dashboard\n"
        "description: team DORA metrics\n"
        "views: [dora_clt_trend, dora_deploy_freq]\n"
    )
    text = build_embedding_text("application", "dora_dashboard", content)
    assert "application dora_dashboard" in text
    assert "dora_clt_trend" in text
    assert "dora_deploy_freq" in text


def test_unknown_kind_falls_back_to_minimal_text():
    text = build_embedding_text("mapper", "foo", "irrelevant: yaml")
    assert "mapper foo" in text


def test_malformed_yaml_does_not_crash():
    text = build_embedding_text("query", "x", "::: not yaml :::")
    assert "query x" in text


# ---------------------------------------------------------------------------
# index_now — synchronous indexing path used by the CLI + tests
# ---------------------------------------------------------------------------


def test_index_now_stores_embedding_and_returns_true(fake_provider, fake_store, tmp_path):
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    ok = index_now(
        model_dir=model_dir, artifact_type="query", name="pr_count",
        content="name: pr_count\nroot: PullRequest\n",
        provider=fake_provider, store=fake_store,
    )
    assert ok is True
    key = ("demo", "query", "pr_count", fake_provider.model)
    assert key in fake_store.rows
    assert fake_store.rows[key]["content_hash"]


def test_index_now_skips_unchanged_content(fake_provider, fake_store, tmp_path):
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    content = "name: pr_count\nroot: PullRequest\n"
    first = index_now(model_dir=model_dir, artifact_type="query", name="pr_count",
                      content=content, provider=fake_provider, store=fake_store)
    second = index_now(model_dir=model_dir, artifact_type="query", name="pr_count",
                       content=content, provider=fake_provider, store=fake_store)
    assert first is True
    assert second is False
    # Provider only called once — second call should short-circuit.
    assert len(fake_provider.received) == 1


def test_index_now_re_embeds_when_content_changes(fake_provider, fake_store, tmp_path):
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    index_now(model_dir=model_dir, artifact_type="query", name="pr_count",
              content="name: pr_count\nroot: PullRequest\n",
              provider=fake_provider, store=fake_store)
    ok = index_now(model_dir=model_dir, artifact_type="query", name="pr_count",
                   content="name: pr_count\nroot: PullRequest\ndescription: new!\n",
                   provider=fake_provider, store=fake_store)
    assert ok is True
    assert len(fake_provider.received) == 2


def test_index_now_returns_false_for_non_indexed_kinds(fake_provider, fake_store, tmp_path):
    """Mappers aren't searched, so we shouldn't burn embedding tokens on them."""
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    ok = index_now(model_dir=model_dir, artifact_type="mapper", name="m",
                   content="name: m\n",
                   provider=fake_provider, store=fake_store)
    assert ok is False
    assert fake_provider.received == []


def test_index_now_returns_false_when_unconfigured(tmp_path):
    """Without a configured provider/store the indexer is a no-op."""
    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    ok = index_now(model_dir=model_dir, artifact_type="query", name="x", content="name: x")
    assert ok is False


def test_index_now_swallows_provider_errors(fake_store, tmp_path):
    """An embedding failure must never raise — saves stay on the happy path."""

    class Boom:
        model = "fake"
        dim = 8

        def embed_one(self, text):
            raise RuntimeError("rate limited")

        def embed_batch(self, texts):
            raise RuntimeError("rate limited")

    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    ok = index_now(model_dir=model_dir, artifact_type="query", name="x",
                   content="name: x\n", provider=Boom(), store=fake_store)
    assert ok is False
    assert fake_store.rows == {}
