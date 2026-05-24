# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/services/chat_publish_service.py (Epic 21 VG-240/241)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml as _yaml

from api.services.chat_publish_service import (
    _resolve_artifacts,
    slugify_title,
    unique_name,
)
from core import metadata_db

# ---------------------------------------------------------------------------
# slugify_title
# ---------------------------------------------------------------------------


class TestSlugifyTitle:
    def test_plain_words(self):
        assert slugify_title("dora clt by team") == "dora_clt_by_team"

    def test_punctuation_collapses_to_underscore(self):
        assert slugify_title("Top 10 PRs: by author!") == "top_10_prs_by_author"

    def test_mixed_case_lowered(self):
        assert slugify_title("DORA CLT By Team") == "dora_clt_by_team"

    def test_starts_with_digit_prepends_chat(self):
        assert slugify_title("2024 release stats") == "chat_2024_release_stats"

    def test_empty_falls_back_to_chat_untitled(self):
        assert slugify_title("") == "chat_untitled"
        assert slugify_title("   ") == "chat_untitled"
        assert slugify_title("!!!") == "chat_untitled"

    def test_max_length_capped(self):
        long = "a" * 200
        assert len(slugify_title(long)) == 80

    def test_runs_of_underscores_collapse(self):
        assert slugify_title("foo___bar") == "foo_bar"


# ---------------------------------------------------------------------------
# unique_name (dedupe against existing artifacts)
# ---------------------------------------------------------------------------


@pytest.fixture
def model_dir(tmp_path) -> Path:
    d = tmp_path / "demo_model"
    d.mkdir()
    return d


class TestUniqueName:
    def test_returns_base_when_free(self, model_dir):
        assert unique_name(model_dir, "view", "fresh") == "fresh"

    def test_appends_v2_when_base_taken(self, model_dir):
        metadata_db.record_version(model_dir, "view", "dora", "name: dora\n")
        assert unique_name(model_dir, "view", "dora") == "dora_v2"

    def test_keeps_walking_v_suffixes(self, model_dir):
        metadata_db.record_version(model_dir, "view", "dora", "name: dora\n")
        metadata_db.record_version(model_dir, "view", "dora_v2", "name: dora_v2\n")
        metadata_db.record_version(model_dir, "view", "dora_v3", "name: dora_v3\n")
        assert unique_name(model_dir, "view", "dora") == "dora_v4"

    def test_isolated_by_artifact_type(self, model_dir):
        """Views and queries can share names — only same-type collisions count."""
        metadata_db.record_version(model_dir, "view", "shared", "name: shared\n")
        # Query namespace is independent — "shared" is still free as a query.
        assert unique_name(model_dir, "query", "shared") == "shared"


# ---------------------------------------------------------------------------
# _resolve_artifacts — the three paths
#
# We stub the service layer's save_* functions so we can assert on call
# order + arguments without dragging in YAML validation that needs a real
# ontology / db backend.
# ---------------------------------------------------------------------------


class _Recorder:
    """Captures calls to create_or_replace_view / create_or_replace_query."""
    def __init__(self):
        self.views: list[tuple] = []
        self.queries: list[tuple] = []

    def save_view(self, model_dir, name, content, user_id=None, via=None):
        self.views.append((name, content, user_id, via))
        # Also write to the DB so a follow-up unique_name() check sees it.
        metadata_db.record_version(model_dir, "view", name, content)
        return {"name": name}

    def save_query(self, model_dir, name, content, user_id=None, via=None):
        self.queries.append((name, content, user_id, via))
        metadata_db.record_version(model_dir, "query", name, content)
        return {"name": name}


@pytest.fixture
def recorder(monkeypatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(
        "api.services.chat_publish_service.view_service.create_or_replace_view",
        rec.save_view,
    )
    monkeypatch.setattr(
        "api.services.chat_publish_service.query_service.create_or_replace_query",
        rec.save_query,
    )
    return rec


class TestResolveArtifactsPathA:
    def test_saved_view_returns_existing_name_no_writes(self, model_dir, recorder):
        view_name, query_name = _resolve_artifacts(
            model_dir,
            title_slug="anything",
            saved_view={"name": "dora_clt_by_team", "params": {}},
            inline_view=None,
            user_id="user-1",
        )
        assert view_name == "dora_clt_by_team"
        assert query_name is None
        assert recorder.views == []
        assert recorder.queries == []


class TestResolveArtifactsPathB:
    def test_saves_view_using_existing_query_ref(self, model_dir, recorder):
        view_yaml = "name: text2view\ntype: chart\nquery: top_pr_authors\n"
        view_name, query_name = _resolve_artifacts(
            model_dir,
            title_slug="top_pr_authors",
            saved_view=None,
            inline_view={"view_yaml": view_yaml, "query_yaml": None, "params": {}},
            user_id="user-1",
        )
        assert query_name is None
        # View was saved with the slug as its name.
        assert view_name == "top_pr_authors"
        assert len(recorder.views) == 1
        saved_name, saved_content, user_id, via = recorder.views[0]
        assert saved_name == "top_pr_authors"
        assert user_id == "user-1"
        assert via == "chat"
        # The saved YAML preserves the existing ``query:`` reference but
        # has the new view name written in.
        parsed = _yaml.safe_load(saved_content)
        assert parsed["name"] == "top_pr_authors"
        assert parsed["query"] == "top_pr_authors"  # the saved query, unchanged
        assert recorder.queries == []

    def test_view_without_query_field_raises(self, model_dir, recorder):
        with pytest.raises(ValueError, match="must reference a saved query"):
            _resolve_artifacts(
                model_dir,
                title_slug="x",
                saved_view=None,
                inline_view={"view_yaml": "name: bad\ntype: chart\n", "query_yaml": None, "params": {}},
                user_id="user-1",
            )


class TestResolveArtifactsPathC:
    def test_saves_query_then_view_with_rewritten_ref(self, model_dir, recorder):
        query_yaml = "name: text2query\nroot: PullRequest\nattributes:\n  - author_id\n"
        view_yaml = "name: text2view\ntype: chart\nquery: text2query\nvisualization:\n  chart_type: bar\n"
        view_name, query_name = _resolve_artifacts(
            model_dir,
            title_slug="prs_by_author",
            saved_view=None,
            inline_view={"view_yaml": view_yaml, "query_yaml": query_yaml, "params": {}},
            user_id="user-1",
        )
        assert query_name == "prs_by_author"
        assert view_name == "prs_by_author"
        # Query saved first.
        assert len(recorder.queries) == 1
        q_name, q_content, _, q_via = recorder.queries[0]
        assert q_name == "prs_by_author"
        assert q_via == "chat"
        assert _yaml.safe_load(q_content)["name"] == "prs_by_author"
        # View saved second; its ``query:`` field rewritten to the new
        # query name, not the "text2query" placeholder.
        assert len(recorder.views) == 1
        v_name, v_content, _, v_via = recorder.views[0]
        assert v_name == "prs_by_author"
        assert v_via == "chat"
        v_parsed = _yaml.safe_load(v_content)
        assert v_parsed["name"] == "prs_by_author"
        assert v_parsed["query"] == "prs_by_author"

    def test_dedupes_when_slug_already_taken(self, model_dir, recorder):
        # Pre-seed a view with the slug to force the v_2 suffix.
        metadata_db.record_version(model_dir, "view", "shared_slug", "existing\n")
        metadata_db.record_version(model_dir, "query", "shared_slug", "existing\n")

        query_yaml = "name: text2query\nroot: PullRequest\n"
        view_yaml = "name: text2view\ntype: chart\nquery: text2query\n"
        view_name, query_name = _resolve_artifacts(
            model_dir,
            title_slug="shared_slug",
            saved_view=None,
            inline_view={"view_yaml": view_yaml, "query_yaml": query_yaml, "params": {}},
            user_id="user-1",
        )
        assert view_name == "shared_slug_v2"
        assert query_name == "shared_slug_v2"
