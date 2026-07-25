# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""AG-UI event stream for the chat surface.

Phase 1: wraps the existing ``chat_turn`` (sync agentic loop) and turns
its final ``ChatTurnResult`` into a stream of AG-UI events matching the
protocol at https://docs.ag-ui.com/. The loop still runs to completion
before events flow — this is the plumbing seam; Phase 2 refactors
``chat_turn`` to yield events live inside the loop for real streaming.

Every turn emits:

    RUN_STARTED
      → TOOL_CALL_START / TOOL_CALL_ARGS / TOOL_CALL_END / TOOL_CALL_RESULT
        (one set per trace entry, in loop order)
      → TEXT_MESSAGE_START / TEXT_MESSAGE_CONTENT / TEXT_MESSAGE_END
        (final assistant text — the caption for the produced view, or
         an error message)
      → CUSTOM (vizgrams-specific: the saved_view or inline_view payload
        so the UI can render the chart card via generative UI)
    RUN_FINISHED   (or RUN_ERROR on failure)
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ag_ui.core import (
    BaseEvent,
    CustomEvent,
    RunErrorEvent,
    RunFinishedEvent,
    RunStartedEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
    ToolCallArgsEvent,
    ToolCallEndEvent,
    ToolCallResultEvent,
    ToolCallStartEvent,
)

from api.services.chat.service import ChatTurnResult, chat_turn


def stream_turn(
    *,
    model_dir: Path,
    message: str,
    thread_id: str,
    history: list[dict] | None = None,
) -> Iterator[BaseEvent]:
    """Run one chat turn and yield the corresponding AG-UI events.

    Errors are surfaced as ``RUN_ERROR`` rather than raised — an
    exception mid-stream would corrupt the SSE frame boundary and leave
    the browser thread in a stuck "generating" state. Callers that need
    stack traces can inspect logs.
    """
    run_id = _new_id("run")
    yield RunStartedEvent(thread_id=thread_id, run_id=run_id)

    try:
        result: ChatTurnResult = chat_turn(
            model_dir=model_dir,
            message=message,
            history=history or [],
        )
    except Exception as exc:  # noqa: BLE001 - all failures become RUN_ERROR events
        yield RunErrorEvent(
            message=f"chat_turn raised: {type(exc).__name__}: {exc}",
        )
        return

    # Per-tool trace events. Emit args as a single ToolCallArgs event
    # containing the full JSON payload — streaming args token-by-token
    # is a Phase 2 refactor.
    for step in result.trace:
        tool_call_id = _new_id("tc")
        yield ToolCallStartEvent(
            tool_call_id=tool_call_id,
            tool_call_name=step.name,
            parent_message_id=run_id,
        )
        yield ToolCallArgsEvent(
            tool_call_id=tool_call_id,
            delta=json.dumps(step.arguments),
        )
        yield ToolCallEndEvent(tool_call_id=tool_call_id)
        yield ToolCallResultEvent(
            message_id=_new_id("msg"),
            tool_call_id=tool_call_id,
            content=step.summary or ("ok" if step.success else "failed"),
        )

    # Final text — the caption on success, the error on failure.
    message_id = _new_id("msg")
    text = _final_text(result)
    yield TextMessageStartEvent(message_id=message_id, role="assistant")
    if text:
        yield TextMessageContentEvent(message_id=message_id, delta=text)
    yield TextMessageEndEvent(message_id=message_id)

    # Vizgrams-specific: publish the produced view payload as a CUSTOM
    # event so the assistant-ui runtime can render a chart card via a
    # generative-UI handler keyed on ``name``.
    payload = _view_payload(result)
    if payload is not None:
        yield CustomEvent(name="vizgrams.view", value=payload)

    if result.success:
        yield RunFinishedEvent(thread_id=thread_id, run_id=run_id)
    else:
        yield RunErrorEvent(message=result.error or "chat turn failed")


def _final_text(result: ChatTurnResult) -> str:
    """Extract the assistant's final text — caption on success, error on failure."""
    if not result.success:
        return result.error or "The assistant couldn't produce a view."
    if result.inline_view is not None:
        return _get(result.inline_view, "caption", "")
    if result.saved_view is not None:
        return _get(result.saved_view, "caption", "")
    return ""


def _view_payload(result: ChatTurnResult) -> dict[str, Any] | None:
    """Return the produced view — either an inline_view or a saved_view ref —
    as a JSON-serialisable dict, or None if the turn didn't produce one."""
    if result.inline_view is not None:
        return {"kind": "inline_view", "payload": _to_dict(result.inline_view)}
    if result.saved_view is not None:
        return {"kind": "saved_view", "payload": _to_dict(result.saved_view)}
    return None


def _to_dict(obj: Any) -> Any:
    """Best-effort dict coercion for pydantic v1/v2, dataclass, or plain dict."""
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict"):
        return obj.dict()
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    return obj


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"
