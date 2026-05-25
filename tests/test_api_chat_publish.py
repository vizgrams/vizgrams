# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""HTTP tests for the chat-publish endpoint (VG-240 / VG-241).

POST /api/v1/model/{m}/chat/publish — turns a chat-turn payload
into a vizgram. Three paths (saved_view / inline path B / inline path C)
covered at the wire level; the deeper service-layer logic lives in
``test_chat_publish.py``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_base_dir, resolve_model_dir
from api.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DEV_USER", "creator@example.com")
    monkeypatch.setenv("VZ_CREATORS", "creator@example.com")
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))

    model_dir = tmp_path / "demo"
    (model_dir / "views").mkdir(parents=True)
    (model_dir / "queries").mkdir(parents=True)

    app.dependency_overrides[get_base_dir] = lambda: tmp_path
    app.dependency_overrides[resolve_model_dir] = lambda model: model_dir  # noqa: ARG005
    yield TestClient(app), model_dir
    app.dependency_overrides.clear()


def _ok_publish_result(view_name="dora_clt_by_team", query_name=None):
    return {
        "vizgram_id": "viz-uuid-123",
        "view_name": view_name,
        "query_name": query_name,
    }


# ---------------------------------------------------------------------------
# Path A — saved_view publish
# ---------------------------------------------------------------------------


def test_publish_saved_view_returns_view_name(client):
    c, _ = client
    with patch(
        "api.routers.chat.chat_publish_service.publish_from_chat",
        return_value=_ok_publish_result(),
    ) as mock:
        resp = c.post(
            "/api/v1/model/demo/chat/publish",
            json={
                "title": "DORA CLT by team",
                "caption": "Cycle time dropped 30%",
                "saved_view": {"name": "dora_clt_by_team", "params": {}},
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["vizgram_id"] == "viz-uuid-123"
    assert body["view_name"] == "dora_clt_by_team"
    assert body["query_name"] is None

    kwargs = mock.call_args.kwargs
    assert kwargs["title"] == "DORA CLT by team"
    assert kwargs["caption"] == "Cycle time dropped 30%"
    assert kwargs["saved_view"] == {"name": "dora_clt_by_team", "params": {}}
    assert kwargs["inline_view"] is None
    assert isinstance(kwargs["model_dir"], Path)


# ---------------------------------------------------------------------------
# Path C — inline_view with inline_query
# ---------------------------------------------------------------------------


def test_publish_inline_view_path_c_returns_both_names(client):
    c, _ = client
    with patch(
        "api.routers.chat.chat_publish_service.publish_from_chat",
        return_value=_ok_publish_result(view_name="prs_by_author", query_name="prs_by_author"),
    ) as mock:
        resp = c.post(
            "/api/v1/model/demo/chat/publish",
            json={
                "title": "PRs by author",
                "caption": None,
                "inline_view": {
                    "view_yaml": "name: text2view\ntype: chart\nquery: text2query\n",
                    "query_yaml": "name: text2query\nroot: PullRequest\n",
                    "params": {},
                },
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["view_name"] == "prs_by_author"
    assert body["query_name"] == "prs_by_author"

    kwargs = mock.call_args.kwargs
    assert kwargs["inline_view"]["query_yaml"]
    assert kwargs["saved_view"] is None


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


def test_publish_rejects_payload_without_view(client):
    c, _ = client
    resp = c.post(
        "/api/v1/model/demo/chat/publish",
        json={"title": "x", "caption": None},
    )
    assert resp.status_code == 422
    assert "saved_view" in resp.json()["detail"] or "inline_view" in resp.json()["detail"]


def test_publish_rejects_empty_title(client):
    c, _ = client
    resp = c.post(
        "/api/v1/model/demo/chat/publish",
        json={
            "title": "",
            "saved_view": {"name": "x", "params": {}},
        },
    )
    assert resp.status_code == 422  # pydantic min_length=1


def test_publish_wraps_value_error_as_400(client):
    c, _ = client
    with patch(
        "api.routers.chat.chat_publish_service.publish_from_chat",
        side_effect=ValueError("boom"),
    ):
        resp = c.post(
            "/api/v1/model/demo/chat/publish",
            json={
                "title": "x",
                "saved_view": {"name": "x", "params": {}},
            },
        )
    assert resp.status_code == 400
    assert "boom" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Auth gate — non-creators forbidden
# ---------------------------------------------------------------------------


def test_publish_rejects_non_creators(tmp_path, monkeypatch):
    monkeypatch.delenv("DEV_USER", raising=False)
    monkeypatch.setenv("VZ_CREATORS", "")
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "")
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))

    model_dir = tmp_path / "demo"
    (model_dir / "views").mkdir(parents=True)
    app.dependency_overrides[get_base_dir] = lambda: tmp_path
    app.dependency_overrides[resolve_model_dir] = lambda model: model_dir  # noqa: ARG005
    try:
        with TestClient(app) as c:
            resp = c.post(
                "/api/v1/model/demo/chat/publish",
                json={
                    "title": "x",
                    "saved_view": {"name": "x", "params": {}},
                },
                headers={"X-Auth-Request-Email": "viewer@example.com"},
            )
        assert resp.status_code == 403
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# VG-283 link-back: chat-publish attaches artifacts to the originating turn
# ---------------------------------------------------------------------------


def test_publish_attaches_artifacts_to_turn(client):
    """When turn_id is supplied + publish succeeds, the produced
    view+query land on the turn's ``saved_artifact_ids`` so the catalog
    can show a "view source chat" link on chat-spawned artifacts."""
    c, _ = client
    # Seed a turn we can attach to.
    from core import chat_history_db
    sid = chat_history_db.create_session(
        user_id="dev-user-id", model_id="demo",
    )
    tid = chat_history_db.append_turn(
        session_id=sid, role="assistant", response={"placeholder": True},
    )

    with patch(
        "api.routers.chat.chat_publish_service.publish_from_chat",
        return_value=_ok_publish_result(view_name="prs_by_author", query_name="prs_by_author"),
    ):
        resp = c.post(
            "/api/v1/model/demo/chat/publish",
            json={
                "title": "PRs by author",
                "saved_view": {"name": "prs_by_author", "params": {}},
                "turn_id": tid,
            },
        )
    assert resp.status_code == 200

    # The turn now records the publish output.
    turns = chat_history_db.list_turns_for_session(sid, user_id="dev-user-id")
    assert turns[0]["saved_artifact_ids"] == [
        {"kind": "view", "name": "prs_by_author"},
        {"kind": "query", "name": "prs_by_author"},
    ]


def test_publish_without_turn_id_still_works(client):
    """turn_id is optional — back-compat for callers that don't supply it
    + for one-off direct API uses."""
    c, _ = client
    with patch(
        "api.routers.chat.chat_publish_service.publish_from_chat",
        return_value=_ok_publish_result(),
    ):
        resp = c.post(
            "/api/v1/model/demo/chat/publish",
            json={"title": "T", "saved_view": {"name": "v", "params": {}}},
        )
    assert resp.status_code == 200


def test_publish_with_stale_turn_id_does_not_fail(client):
    """A turn id that no longer exists shouldn't 5xx — the artifact
    is published; the link-back is best-effort."""
    c, _ = client
    with patch(
        "api.routers.chat.chat_publish_service.publish_from_chat",
        return_value=_ok_publish_result(),
    ):
        resp = c.post(
            "/api/v1/model/demo/chat/publish",
            json={
                "title": "T",
                "saved_view": {"name": "v", "params": {}},
                "turn_id": "no-such-turn",
            },
        )
    assert resp.status_code == 200
