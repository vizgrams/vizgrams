# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Chat HTTP routes (Epic 19 VG-205, reshaped in VG-237, renamed in ADR-0001).

Two endpoints: one runs an assistant turn (the agentic tool loop);
the other publishes the resulting view as a vizgram. Both translate
between Pydantic and the dataclass + apply auth — the orchestrator
does the actual work.

Response shape (turn): each successful turn produces a saved_view ref
or an inline_view spec. The UI renders both via ``ViewContent`` (the
same component every other surface uses), so charts + drilldowns are
uniform.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from api.dependencies import get_current_user, require_creator, resolve_model_dir
from api.services.chat import publish as chat_publish_service
from api.services.chat import service
from core import chat_history_db

router = APIRouter(prefix="/model/{model}/chat", tags=["chat"])


# Used as the default session title when the user doesn't pick one —
# first ~60 chars of their first message. Real UX could regenerate via
# the LLM later; this is a good-enough placeholder.
_TITLE_FROM_MESSAGE_LIMIT = 60


def _session_title_from_message(message: str) -> str:
    msg = (message or "").strip().replace("\n", " ")
    if len(msg) <= _TITLE_FROM_MESSAGE_LIMIT:
        return msg
    return msg[: _TITLE_FROM_MESSAGE_LIMIT - 1].rstrip() + "…"


class HistoryTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=4000)
    history: list[HistoryTurn] = Field(default_factory=list)
    # VG-281: resume an existing session. Null/absent → new session.
    # Owner-scoped — if the id doesn't match a session owned by the
    # caller, we create a fresh one (treating the id as stale rather
    # than erroring keeps the UX forgiving when sessions are pruned).
    session_id: str | None = None


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
    # VG-281: id of the session this turn was persisted to. Caller
    # passes this back on follow-up requests to extend the same session.
    # Always populated on success (a fresh session is created when the
    # request didn't supply one).
    session_id: str | None = None
    turn_id: str | None = None        # id of THIS assistant turn (for VG-283 link-back)
    query_yaml: str | None = None
    view_yaml: str | None = None
    sql: str | None = None


@router.post("", response_model=ChatResponse)
def chat(
    body: ChatRequest,
    model: str,
    model_dir: str = Depends(resolve_model_dir),
    user_id: str = Depends(get_current_user),
    _email: str = Depends(require_creator),
) -> ChatResponse:
    """One assistant turn: user message in, view (saved or inline) out.

    Creator-gated. VG-281: turns are persisted under a chat_sessions row
    so users can resume + the publish flow can link artifacts back to
    the originating turn. The client passes ``session_id`` to extend an
    existing session; absent → fresh session.
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

    # VG-281 persistence — run regardless of success so failed turns
    # still show up in the user's history (debugging + resume affordance).
    # Best-effort: a DB error doesn't fail the turn (the user has the
    # response already; losing the history is degraded UX not a hard
    # error).
    session_id, turn_id = _persist_turn(
        user_id=user_id, model_id=model,
        requested_session_id=body.session_id,
        user_message=body.message, response=_response_to_json(result),
    )

    return ChatResponse(
        success=result.success,
        error=result.error,
        iterations=result.iterations,
        saved_view=SavedViewRef(**result.saved_view) if result.saved_view else None,
        inline_view=InlineView(**result.inline_view) if result.inline_view else None,
        title=result.title,
        session_id=session_id,
        turn_id=turn_id,
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


def _persist_turn(
    *,
    user_id: str,
    model_id: str,
    requested_session_id: str | None,
    user_message: str,
    response: dict,
) -> tuple[str | None, str | None]:
    """Append the user msg + assistant turn to chat_history_db. Returns
    ``(session_id, assistant_turn_id)``.

    Best-effort: any error here returns (None, None) — the chat
    response itself still ships. Logging the failure would be useful
    but we don't want to fail the turn the user just paid for.
    """
    import logging
    log = logging.getLogger(__name__)
    try:
        # Resolve the session. If the client supplied an id that's
        # actually theirs, append to it; otherwise (no id, or someone
        # else's id) start fresh.
        session_id = requested_session_id
        if session_id is not None:
            existing = chat_history_db.get_session(session_id, user_id=user_id)
            if existing is None:
                session_id = None
        if session_id is None:
            session_id = chat_history_db.create_session(
                user_id=user_id, model_id=model_id,
                title=_session_title_from_message(user_message),
            )
        # Append the user msg + the assistant response in order.
        chat_history_db.append_turn(
            session_id=session_id, role="user", content=user_message,
        )
        turn_id = chat_history_db.append_turn(
            session_id=session_id, role="assistant", response=response,
        )
        return session_id, turn_id
    except Exception as exc:  # noqa: BLE001 — never fail a chat turn
        log.warning("chat persistence failed: %s", exc, exc_info=True)
        return None, None


def _response_to_json(result) -> dict:
    """Project the orchestrator's ChatTurnResult into a JSON-safe dict
    for storage. Mirrors the wire response shape so the UI can
    re-hydrate a transcript byte-for-byte."""
    return {
        "success": result.success,
        "error": result.error,
        "iterations": result.iterations,
        "saved_view": result.saved_view,
        "inline_view": result.inline_view,
        "title": result.title,
        "query_yaml": result.query_yaml,
        "view_yaml": result.view_yaml,
        "sql": result.sql,
        "trace": [
            {
                "name": t.name, "arguments": t.arguments,
                "success": t.success, "summary": t.summary, "payload": t.payload,
            }
            for t in result.trace
        ],
    }


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
    # VG-283: id of the chat turn this publish came from. When set, the
    # produced artifacts get attached to the turn so the UI can show
    # "this view came from chat ⤴" + the catalog can show a
    # "view source chat" link on chat-spawned artifacts.
    turn_id: str | None = None


class ChatPublishResponse(BaseModel):
    vizgram_id: str
    view_name: str
    query_name: str | None = None


@router.post("/publish", response_model=ChatPublishResponse)
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

    # VG-283: attach the newly-created artifacts to the originating
    # turn. Best-effort — failure logs but doesn't fail the publish.
    if body.turn_id:
        try:
            artifacts: list[dict] = [{"kind": "view", "name": result["view_name"]}]
            if result.get("query_name"):
                artifacts.append({"kind": "query", "name": result["query_name"]})
            chat_history_db.attach_saved_artifacts(
                body.turn_id, artifacts=artifacts,
            )
        except Exception as exc:  # noqa: BLE001
            import logging
            logging.getLogger(__name__).warning(
                "attach_saved_artifacts failed for turn %s: %s",
                body.turn_id, exc,
            )

    return ChatPublishResponse(**result)


# ---------------------------------------------------------------------------
# Sessions API (Epic 25 VG-281)
# ---------------------------------------------------------------------------


class ChatSessionSummary(BaseModel):
    id: str
    title: str | None
    created_at: str
    updated_at: str


class ChatTurnOut(BaseModel):
    """One row from chat_history_db.list_turns_for_session.

    ``response`` carries the same shape as ChatResponse so the UI can
    re-hydrate the chat transcript without an extra round-trip per
    turn. Null for user turns.
    """
    id: str
    ord: int
    role: Literal["user", "assistant"]
    content: str | None = None
    response: dict | None = None     # named ``response_json`` on the DB row; remapped below
    saved_artifact_ids: list | None = None
    feedback: dict | None = None
    created_at: str

    @classmethod
    def from_row(cls, row: dict) -> ChatTurnOut:
        """Build from a chat_history_db.list_turns_for_session row."""
        return cls(
            id=row["id"],
            ord=row["ord"],
            role=row["role"],
            content=row.get("content"),
            response=row.get("response_json"),
            saved_artifact_ids=row.get("saved_artifact_ids"),
            feedback=row.get("feedback"),
            created_at=row["created_at"],
        )


class ChatSessionDetail(ChatSessionSummary):
    turns: list[ChatTurnOut]


@router.get("/sessions", response_model=list[ChatSessionSummary])
def list_chat_sessions(
    model: str,
    limit: int = 50,
    offset: int = 0,
    user_id: str = Depends(get_current_user),
    _email: str = Depends(require_creator),
):
    """List the caller's chat sessions for this model, newest-updated first."""
    rows = chat_history_db.list_sessions_for_user(
        user_id=user_id, model_id=model, limit=limit, offset=offset,
    )
    return [
        ChatSessionSummary(
            id=r["id"], title=r["title"],
            created_at=r["created_at"], updated_at=r["updated_at"],
        )
        for r in rows
    ]


@router.get("/sessions/{session_id}", response_model=ChatSessionDetail)
def get_chat_session(
    session_id: str,
    user_id: str = Depends(get_current_user),
    _email: str = Depends(require_creator),
):
    """Full transcript for one session. Owner-scoped — 404 for non-owners."""
    s = chat_history_db.get_session(session_id, user_id=user_id)
    if s is None:
        raise HTTPException(status_code=404, detail="Session not found")
    turns = chat_history_db.list_turns_for_session(session_id, user_id=user_id)
    return ChatSessionDetail(
        id=s["id"], title=s["title"],
        created_at=s["created_at"], updated_at=s["updated_at"],
        turns=[ChatTurnOut.from_row(t) for t in turns],
    )


@router.delete("/sessions/{session_id}", status_code=204)
def delete_chat_session(
    session_id: str,
    user_id: str = Depends(get_current_user),
    _email: str = Depends(require_creator),
):
    """Soft-delete a session (sets ended_at). Owner-scoped — 404 otherwise."""
    if not chat_history_db.end_session(session_id, user_id=user_id):
        raise HTTPException(status_code=404, detail="Session not found")
    return None


class ChatSessionRenameRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)


@router.put("/sessions/{session_id}", response_model=ChatSessionSummary)
def rename_chat_session(
    session_id: str,
    body: ChatSessionRenameRequest,
    user_id: str = Depends(get_current_user),
    _email: str = Depends(require_creator),
):
    """Rename a session. Owner-scoped — 404 otherwise."""
    if not chat_history_db.update_session_title(
        session_id, user_id=user_id, title=body.title,
    ):
        raise HTTPException(status_code=404, detail="Session not found")
    s = chat_history_db.get_session(session_id, user_id=user_id)
    return ChatSessionSummary(
        id=s["id"], title=s["title"],
        created_at=s["created_at"], updated_at=s["updated_at"],
    )
