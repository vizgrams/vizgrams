# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Explore-chat HTTP route (Epic 19 VG-205).

Single endpoint, single request shape, single response shape. The
orchestrator does the work; this layer just translates between Pydantic
and the dataclass and applies auth.

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


class ChatResponse(BaseModel):
    success: bool
    content: str = ""
    error: str | None = None
    query_yaml: str | None = None
    view_yaml: str | None = None
    sql: str | None = None
    columns: list[str] = Field(default_factory=list)
    rows: list[list] = Field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    chart_type: str | None = None
    x_field: str | None = None
    y_field: str | None = None
    color_field: str | None = None
    iterations: int = 0
    trace: list[TraceStep] = Field(default_factory=list)


@router.post("/chat", response_model=ChatResponse)
def chat(
    body: ChatRequest,
    model_dir: str = Depends(resolve_model_dir),
    _email: str = Depends(require_creator),
) -> ChatResponse:
    """One assistant turn: user message in, chart + caption out.

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
        content=result.content,
        error=result.error,
        query_yaml=result.query_yaml,
        view_yaml=result.view_yaml,
        sql=result.sql,
        columns=result.columns,
        rows=result.rows,
        row_count=result.row_count,
        truncated=result.truncated,
        chart_type=result.chart_type,
        x_field=result.x_field,
        y_field=result.y_field,
        color_field=result.color_field,
        iterations=result.iterations,
        trace=[
            TraceStep(
                name=t.name, arguments=t.arguments,
                success=t.success, summary=t.summary, payload=t.payload,
            )
            for t in result.trace
        ],
    )
