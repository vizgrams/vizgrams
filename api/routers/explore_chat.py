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

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from api.dependencies import require_creator, resolve_model_dir
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
            model_dir=__import__("pathlib").Path(model_dir),
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
