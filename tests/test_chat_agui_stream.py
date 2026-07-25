# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the AG-UI wrapper around ``chat_turn``.

The load-bearing property is protocol correctness: every turn opens
with ``RUN_STARTED`` and closes with exactly one of ``RUN_FINISHED`` or
``RUN_ERROR`` — a stream missing either bookend leaves the frontend
Thread stuck in "generating" state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

from ag_ui.core import EventType

from api.services.chat.agui_stream import stream_turn


@dataclass
class _FakeTraceStep:
    name: str
    arguments: dict
    success: bool
    summary: str
    payload: dict = field(default_factory=dict)


@dataclass
class _FakeResult:
    success: bool
    error: str | None = None
    iterations: int = 1
    trace: list = field(default_factory=list)
    saved_view: dict | None = None
    inline_view: dict | None = None
    title: str | None = None
    query_yaml: str | None = None
    view_yaml: str | None = None
    sql: str | None = None


def _run(result_or_exc, message: str = "hello") -> list:
    """Drive stream_turn with a patched chat_turn and return the events."""
    def _stub(**kwargs):
        if isinstance(result_or_exc, Exception):
            raise result_or_exc
        return result_or_exc

    with patch("api.services.chat.agui_stream.chat_turn", side_effect=_stub):
        return list(stream_turn(
            model_dir=Path("/tmp/x"),
            message=message,
            thread_id="t1",
            history=[],
        ))


class TestBookends:
    def test_successful_turn_opens_with_run_started_closes_with_run_finished(self):
        events = _run(_FakeResult(success=True, inline_view={"caption": "ok"}))
        assert events[0].type == EventType.RUN_STARTED
        assert events[-1].type == EventType.RUN_FINISHED

    def test_failed_turn_closes_with_run_error(self):
        events = _run(_FakeResult(success=False, error="LLM stopped"))
        assert events[0].type == EventType.RUN_STARTED
        assert events[-1].type == EventType.RUN_ERROR

    def test_uncaught_exception_becomes_run_error_not_traceback(self):
        """A raise mid-stream would corrupt the SSE frame boundary."""
        events = _run(RuntimeError("provider went away"))
        assert events[0].type == EventType.RUN_STARTED
        assert events[-1].type == EventType.RUN_ERROR
        assert "provider went away" in events[-1].message


class TestTraceMapping:
    def test_each_trace_step_emits_start_args_end_result(self):
        result = _FakeResult(
            success=True,
            inline_view={"caption": "done"},
            trace=[
                _FakeTraceStep("find_artifacts", {"q": "prs"},
                               success=True, summary="found 3"),
                _FakeTraceStep("run_saved_query", {"name": "prs_open"},
                               success=True, summary="42 rows"),
            ],
        )
        events = _run(result)
        types = [e.type for e in events]
        # Two tool calls → two start/args/end/result quads
        assert types.count(EventType.TOOL_CALL_START) == 2
        assert types.count(EventType.TOOL_CALL_ARGS) == 2
        assert types.count(EventType.TOOL_CALL_END) == 2
        assert types.count(EventType.TOOL_CALL_RESULT) == 2

    def test_tool_call_start_carries_the_tool_name(self):
        events = _run(_FakeResult(
            success=True, inline_view={"caption": ""},
            trace=[_FakeTraceStep("build_and_run_query", {}, True, "")],
        ))
        starts = [e for e in events if e.type == EventType.TOOL_CALL_START]
        assert starts[0].tool_call_name == "build_and_run_query"


class TestCustomViewEvent:
    def test_inline_view_emits_custom_event(self):
        events = _run(_FakeResult(
            success=True,
            inline_view={"view_yaml": "name: c\n", "caption": "yes"},
        ))
        customs = [e for e in events if e.type == EventType.CUSTOM]
        assert len(customs) == 1
        assert customs[0].name == "vizgrams.view"
        assert customs[0].value["kind"] == "inline_view"
        assert customs[0].value["payload"]["caption"] == "yes"

    def test_saved_view_emits_custom_event(self):
        events = _run(_FakeResult(
            success=True,
            saved_view={"name": "dora_clt_trend", "caption": "..."},
        ))
        customs = [e for e in events if e.type == EventType.CUSTOM]
        assert len(customs) == 1
        assert customs[0].value["kind"] == "saved_view"

    def test_failed_turn_emits_no_custom_view(self):
        events = _run(_FakeResult(success=False, error="oops"))
        customs = [e for e in events if e.type == EventType.CUSTOM]
        assert customs == []


class TestFinalText:
    def test_success_carries_caption_as_text(self):
        events = _run(_FakeResult(
            success=True, inline_view={"caption": "PRs merged per week"},
        ))
        content = [e for e in events if e.type == EventType.TEXT_MESSAGE_CONTENT]
        assert len(content) == 1
        assert content[0].delta == "PRs merged per week"

    def test_failure_carries_error_as_text(self):
        events = _run(_FakeResult(success=False, error="tool timeout"))
        content = [e for e in events if e.type == EventType.TEXT_MESSAGE_CONTENT]
        assert len(content) == 1
        assert content[0].delta == "tool timeout"
