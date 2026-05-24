# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the /api/v1/model/{m}/chat endpoint."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_base_dir, resolve_model_dir
from api.main import app
from api.services.chat.service import ChatTurnResult


@pytest.fixture
def client(tmp_path, monkeypatch):
    # Auth bypass: DEV_USER + creator role implicit if VZ_CREATORS includes them
    monkeypatch.setenv("DEV_USER", "creator@example.com")
    monkeypatch.setenv("VZ_CREATORS", "creator@example.com")
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))

    model_dir = tmp_path / "demo"
    model_dir.mkdir()

    def _base_dir():
        return tmp_path

    def _model_dir(model: str):  # noqa: ARG001
        return str(model_dir)

    app.dependency_overrides[get_base_dir] = _base_dir
    app.dependency_overrides[resolve_model_dir] = _model_dir
    yield TestClient(app)
    app.dependency_overrides.clear()


def _ok_result() -> ChatTurnResult:
    """A path-A success — the chat invoked a saved view by name."""
    return ChatTurnResult(
        success=True,
        iterations=1,
        saved_view={"name": "top_pr_authors", "params": {}},
        query_yaml="name: top_pr_authors\nroot: PullRequest\n",
        view_yaml="name: top_pr_authors\ntype: chart\n",
        sql="SELECT ...",
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_chat_returns_saved_view_response_payload(client):
    with patch("api.routers.chat.service.chat_turn", return_value=_ok_result()) as mock:
        resp = client.post(
            "/api/v1/model/demo/chat",
            json={"message": "top PR authors"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    # New shape: saved_view ref, no rows/columns/chart_type in the response
    assert body["saved_view"] == {"name": "top_pr_authors", "params": {}}
    assert body["inline_view"] is None
    # Diagnostics still present
    assert body["query_yaml"].startswith("name: top_pr_authors")
    assert body["view_yaml"].startswith("name: top_pr_authors")
    assert body["sql"] == "SELECT ..."

    # Verify the service was called with the right arguments.
    assert mock.call_count == 1
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["message"] == "top PR authors"
    assert isinstance(call_kwargs["model_dir"], Path)


def test_chat_returns_inline_view_for_path_c(client):
    """Path C — query + view both authored inline. Both YAMLs in inline_view."""
    inline_result = ChatTurnResult(
        success=True, iterations=1,
        inline_view={
            "view_yaml": "name: text2view\ntype: chart\n",
            "query_yaml": "name: text2query\nroot: Widget\n",
            "params": {},
        },
        query_yaml="name: text2query\nroot: Widget\n",
        view_yaml="name: text2view\ntype: chart\n",
        sql="SELECT ...",
    )
    with patch("api.routers.chat.service.chat_turn", return_value=inline_result):
        resp = client.post("/api/v1/model/demo/chat", json={"message": "x"})

    body = resp.json()
    assert body["saved_view"] is None
    assert body["inline_view"]["view_yaml"].startswith("name: text2view")
    assert body["inline_view"]["query_yaml"].startswith("name: text2query")


def test_chat_passes_history_through_to_service(client):
    with patch("api.routers.chat.service.chat_turn", return_value=_ok_result()) as mock:
        client.post(
            "/api/v1/model/demo/chat",
            json={
                "message": "now by team",
                "history": [
                    {"role": "user", "content": "top authors"},
                    {"role": "assistant", "content": "I returned the top 10."},
                ],
            },
        )
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["history"] == [
        {"role": "user", "content": "top authors"},
        {"role": "assistant", "content": "I returned the top 10."},
    ]


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


def test_chat_returns_failure_when_orchestrator_returns_failure(client):
    failed = ChatTurnResult(
        success=False, error="Entity 'PR' not found", iterations=3,
    )
    with patch("api.routers.chat.service.chat_turn", return_value=failed):
        resp = client.post(
            "/api/v1/model/demo/chat",
            json={"message": "bad question"},
        )
    assert resp.status_code == 200  # endpoint succeeds; result indicates failure
    body = resp.json()
    assert body["success"] is False
    assert "Entity 'PR' not found" in body["error"]


def test_chat_returns_503_when_llm_unavailable(client):
    with patch(
        "api.routers.chat.service.chat_turn",
        side_effect=RuntimeError("OPENAI_API_KEY is not set"),
    ):
        resp = client.post(
            "/api/v1/model/demo/chat",
            json={"message": "x"},
        )
    assert resp.status_code == 503
    assert "OPENAI_API_KEY is not set" in resp.json()["detail"]


def test_chat_rejects_empty_message(client):
    resp = client.post(
        "/api/v1/model/demo/chat",
        json={"message": ""},
    )
    assert resp.status_code == 422  # Pydantic validation


def test_chat_rejects_overlong_message(client):
    resp = client.post(
        "/api/v1/model/demo/chat",
        json={"message": "x" * 5000},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Creator auth gate
# ---------------------------------------------------------------------------


def test_chat_rejects_non_creators(tmp_path, monkeypatch):
    # DEV_USER is the local-dev system-admin bypass — clear it so the test
    # is gated purely by VZ_CREATORS / VZ_SYSTEM_ADMINS plus the header.
    monkeypatch.delenv("DEV_USER", raising=False)
    monkeypatch.setenv("VZ_CREATORS", "")
    monkeypatch.setenv("VZ_SYSTEM_ADMINS", "")
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))

    model_dir = tmp_path / "demo"
    model_dir.mkdir()
    app.dependency_overrides[get_base_dir] = lambda: tmp_path
    app.dependency_overrides[resolve_model_dir] = lambda model: str(model_dir)  # noqa: ARG005
    # Defence: even if the auth gate fails we mustn't hit the real LLM.
    with patch("api.routers.chat.service.chat_turn", return_value=_ok_result()):
        try:
            with TestClient(app) as client:
                resp = client.post(
                    "/api/v1/model/demo/chat",
                    json={"message": "anything"},
                    headers={"X-Auth-Request-Email": "viewer@example.com"},
                )
            assert resp.status_code == 403
        finally:
            app.dependency_overrides.clear()
