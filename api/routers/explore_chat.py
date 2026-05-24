# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Explore-chat HTTP route (Epic 19 VG-205, reshaped in VG-237).

Single endpoint, single request shape, single response shape. The
orchestrator does the work; this layer just translates between Pydantic
and the dataclass and applies auth.

Response shape: each successful turn produces a saved_view ref or an
inline_view spec. The UI renders both via ``ViewContent`` (the same
component the entity explorer uses), so charts + drilldowns are uniform.

The route shares the ``/explore`` prefix with the existing entity
explorer (``api/routers/explore.py``) but doesn't conflict — the chat
endpoint is a single-segment POST, while the entity routes match
``/{entity}/{id}`` (two segments) and ``/{entity}/{id}/related/...``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from api.dependencies import get_current_user, require_creator, resolve_model_dir
from api.services import chat_publish_service
from api.services import explore_chat as service

router = APIRouter(prefix="/model/{model}/explore", tags=["explore-chat"])


class HistoryTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=4000)
    history: list[HistoryTurn] = Field(default_factory=list)


class TraceStep(BaseModel):
    """One tool invocation in the LLM's reasoning trace (VG-239)."""

    name: str
    arguments: dict
    success: bool
    summary: str
    payload: dict = Field(default_factory=dict)


class SavedViewRef(BaseModel):
    """Path A — render an existing saved view by name."""

    name: str
    params: dict[str, str] = Field(default_factory=dict)


class InlineView(BaseModel):
    """Paths B / C — render a transient view YAML.

    ``query_yaml`` is ``None`` when the view references an already-saved
    query (path B); set when the query was also authored this turn (path C).
    """

    view_yaml: str
    query_yaml: str | None = None
    params: dict[str, str] = Field(default_factory=dict)


class ChatResponse(BaseModel):
    """One assistant turn (Epic 20 VG-237 reshape).

    On success, exactly one of ``saved_view`` / ``inline_view`` is set.
    The UI fetches data via ``executeView`` (saved) or
    ``executeViewInline`` (inline) and renders through the same
    ``ViewContent`` component the explorer uses.

    Diagnostics (``query_yaml`` / ``view_yaml`` / ``sql`` / ``trace``)
    feed the "Show your work" tab; not user-facing chrome.
    """

    success: bool
    error: str | None = None
    iterations: int = 0
    trace: list[TraceStep] = Field(default_factory=list)
    saved_view: SavedViewRef | None = None
    inline_view: InlineView | None = None
    # Short factual title for the turn (e.g. "PR count by team, last 12
    # weeks"). The publish dialog seeds the Title input from this. Null
    # only when the LLM forgot to call present_view and the auto-present
    # safety net kicked in — UI falls back to a placeholder.
    title: str | None = None
    query_yaml: str | None = None
    view_yaml: str | None = None
    sql: str | None = None


@router.post("/chat", response_model=ChatResponse)
def chat(
    body: ChatRequest,
    model_dir: str = Depends(resolve_model_dir),
    _email: str = Depends(require_creator),
) -> ChatResponse:
    """One assistant turn: user message in, view (saved or inline) out.

    Creator-gated. Stateless — the client sends the full conversation
    history in every request.
    """
    try:
        result = service.chat_turn(
            model_dir=Path(model_dir),
            message=body.message,
            history=[turn.model_dump() for turn in body.history],
        )
    except RuntimeError as exc:
        # Most commonly: VZ_LLM_PROVIDER is set but no API key in env.
        raise HTTPException(status_code=503, detail=f"LLM unavailable: {exc}") from exc

    return ChatResponse(
        success=result.success,
        error=result.error,
        iterations=result.iterations,
        saved_view=SavedViewRef(**result.saved_view) if result.saved_view else None,
        inline_view=InlineView(**result.inline_view) if result.inline_view else None,
        title=result.title,
        query_yaml=result.query_yaml,
        view_yaml=result.view_yaml,
        sql=result.sql,
        trace=[
            TraceStep(
                name=t.name, arguments=t.arguments,
                success=t.success, summary=t.summary, payload=t.payload,
            )
            for t in result.trace
        ],
    )


# ---------------------------------------------------------------------------
# Publish (Epic 21 — VG-240 + VG-241)
# ---------------------------------------------------------------------------


class ChatPublishRequest(BaseModel):
    """Either ``saved_view`` or ``inline_view`` must be set — the same
    discriminated payload the chat response uses, plus title + caption."""

    title: str = Field(..., min_length=1, max_length=200)
    caption: str | None = None
    saved_view: SavedViewRef | None = None
    inline_view: InlineView | None = None
    params: dict[str, str] = Field(default_factory=dict)


class ChatPublishResponse(BaseModel):
    vizgram_id: str
    view_name: str
    query_name: str | None = None


@router.post("/chat/publish", response_model=ChatPublishResponse)
def chat_publish(
    body: ChatPublishRequest,
    model: str,
    model_dir: str = Depends(resolve_model_dir),
    user_id: str = Depends(get_current_user),
    _email: str = Depends(require_creator),
) -> ChatPublishResponse:
    """Publish a chat turn as a vizgram. Saves any inline artifacts first.

    Path A (saved_view ref): nothing new persisted; just snapshots +
    publishes. Paths B / C: save the wrapper view (and inline query for C)
    as artifacts stamped with ``created_via='chat'``, then publish.

    Returns the vizgram id + the saved-view name so the UI can build a
    "view live data" link (``/views/<name>``) — which is the shareable URL.
    """
    if body.saved_view is None and body.inline_view is None:
        raise HTTPException(
            status_code=422,
            detail="Either 'saved_view' or 'inline_view' must be set.",
        )
    try:
        result = chat_publish_service.publish_from_chat(
            model_dir=Path(model_dir),
            model_id=model,
            title=body.title,
            caption=body.caption,
            saved_view=body.saved_view.model_dump() if body.saved_view else None,
            inline_view=body.inline_view.model_dump() if body.inline_view else None,
            params=body.params,
            user_id=user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ChatPublishResponse(**result)
