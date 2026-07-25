# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for multi-turn chat context: structured history propagation +
"current view" system-prompt injection.

The load-bearing property: after a first turn produces a view, the LLM
on turn 2 must see (a) the previous tool_calls + tool_results as
structured messages, and (b) a "CURRENT VIEW" hint in its system prompt
pointing at the produced view. Without both, follow-ups like "chart the
summary" have no referent and the LLM either guesses wrong or refuses.
"""

from __future__ import annotations

import json

from api.routers.chat import _split_run_agent_input
from api.services.chat.service import (
    _extract_current_view,
    _history_to_openai,
    build_system_prompt,
)

# ---------------------------------------------------------------------------
# _split_run_agent_input — AG-UI wire → chat_turn history
# ---------------------------------------------------------------------------


class TestSplitRunAgentInputPreservesToolStructure:
    def test_assistant_tool_calls_preserved(self):
        """Losing tool_calls on assistant turns would strand the tool
        results that follow — SDKs reject dangling tool_call_ids."""
        _msg, history = _split_run_agent_input([
            {"role": "user", "content": "summarise"},
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {"name": "run_saved_view",
                                 "arguments": '{"name":"recent_activities"}'},
                }],
            },
            {"role": "tool", "tool_call_id": "tc_1",
             "content": '{"kind":"saved_view","payload":{"name":"recent_activities","params":{}}}'},
            {"role": "assistant", "content": "here's the summary"},
            {"role": "user", "content": "chart the summary"},
        ])
        # Everything before the trailing user turn is in history
        assert len(history) == 4
        assistant = history[1]
        assert assistant["role"] == "assistant"
        assert assistant["tool_calls"][0]["function"]["name"] == "run_saved_view"

    def test_tool_message_role_kept_with_id(self):
        """Tool results must land as ``role='tool'`` with a
        ``tool_call_id`` that matches an earlier assistant tool_call —
        otherwise the LLM adapter drops them or 400s."""
        _, history = _split_run_agent_input([
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "tc_x", "type": "function",
                 "function": {"name": "find_artifacts", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "tc_x", "content": "1 match"},
            {"role": "user", "content": "next"},
        ])
        tool_entry = next(h for h in history if h["role"] == "tool")
        assert tool_entry["tool_call_id"] == "tc_x"
        assert tool_entry["content"] == "1 match"

    def test_flat_tool_call_shape_normalised(self):
        """Some assistant-ui adapter versions emit tool_calls as
        ``{id, name, args}`` (locally-executed tools) rather than the
        OpenAI ``{id, type, function}`` shape. Normalise so the
        downstream LLM SDK sees consistent structure regardless."""
        _, history = _split_run_agent_input([
            {"role": "assistant", "content": "",
             "tool_calls": [{"id": "tc_a", "name": "run_query", "args": '{"q":1}'}]},
            {"role": "user", "content": "hi"},
        ])
        assistant = history[0]
        tc = assistant["tool_calls"][0]
        assert tc["type"] == "function"
        assert tc["function"]["name"] == "run_query"


# ---------------------------------------------------------------------------
# _history_to_openai — chat_turn history → LLM messages
# ---------------------------------------------------------------------------


class TestHistoryToOpenai:
    def test_tool_call_only_assistant_kept(self):
        """SDKs require the assistant tool_call turn to precede its
        tool result — dropping empty-content assistant turns would
        strand the tool result on the next call."""
        out = _history_to_openai([
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "tc_1", "type": "function",
                 "function": {"name": "run_saved_view", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "tc_1", "content": "ok"},
        ])
        assert out == [
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "tc_1", "type": "function",
                 "function": {"name": "run_saved_view", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "tc_1", "content": "ok"},
        ]

    def test_tool_message_without_id_dropped(self):
        """A tool message with no ``tool_call_id`` can't be paired with
        any assistant tool_call — safer to drop than to emit a dangling
        reference that 400s the request."""
        out = _history_to_openai([
            {"role": "tool", "content": "orphan"},
        ])
        assert out == []


# ---------------------------------------------------------------------------
# _extract_current_view — find the last rendered view
# ---------------------------------------------------------------------------


class TestExtractCurrentView:
    def test_finds_last_saved_view_payload(self):
        """The streaming layer embeds ``{kind, payload}`` JSON on the
        terminal tool's result. That's the signal we key off — the last
        such tool result IS the current view."""
        view = _extract_current_view([
            {"role": "user", "content": "top 5"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "tc", "type": "function",
                 "function": {"name": "run_saved_view", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "tc",
             "content": json.dumps({"kind": "saved_view",
                                    "payload": {"name": "top5_by_tss"}})},
            {"role": "assistant", "content": "here"},
        ])
        assert view == {"kind": "saved_view", "payload": {"name": "top5_by_tss"}}

    def test_prefers_most_recent_view_when_multiple(self):
        """If turn 1 rendered view A and turn 3 rendered view B, "chart
        this" refers to B, not A."""
        view = _extract_current_view([
            {"role": "tool", "tool_call_id": "1",
             "content": json.dumps({"kind": "saved_view", "payload": {"name": "old"}})},
            {"role": "tool", "tool_call_id": "2",
             "content": json.dumps({"kind": "saved_view", "payload": {"name": "new"}})},
        ])
        assert view["payload"]["name"] == "new"

    def test_no_view_produced_yet_returns_none(self):
        assert _extract_current_view([
            {"role": "user", "content": "hi"},
        ]) is None

    def test_non_view_tool_results_ignored(self):
        """A tool result like ``"5 matches"`` from find_artifacts is
        not a view — must not be misclassified as one."""
        assert _extract_current_view([
            {"role": "tool", "tool_call_id": "tc", "content": "5 matches"},
        ]) is None

    def test_malformed_json_ignored_not_raised(self):
        """A partial or corrupt tool result must not crash the turn —
        just skip it."""
        assert _extract_current_view([
            {"role": "tool", "tool_call_id": "tc", "content": "{not: json"},
        ]) is None


# ---------------------------------------------------------------------------
# build_system_prompt — CURRENT VIEW injection
# ---------------------------------------------------------------------------


class TestSystemPromptCurrentView:
    def test_saved_view_names_the_view(self):
        """The prompt must name the specific view — 'chart the view'
        the LLM saw last is ambiguous if there could be more than one."""
        prompt = build_system_prompt(
            "iagai", "some schema",
            current_view={"kind": "saved_view",
                          "payload": {"name": "dora_clt_trend"}},
        )
        assert "CURRENT VIEW" in prompt
        assert "dora_clt_trend" in prompt

    def test_inline_view_gets_generic_hint(self):
        """Inline views don't have a stable name to cite; the prompt
        describes them by class so the LLM knows there IS a referent
        without inventing a name."""
        prompt = build_system_prompt(
            "iagai", "some schema",
            current_view={"kind": "inline_view", "payload": {}},
        )
        assert "CURRENT VIEW" in prompt
        assert "inline view" in prompt

    def test_no_current_view_no_hint(self):
        """First-turn prompt should not carry a phantom 'you were
        looking at ...' hint — the LLM might invent a referent."""
        prompt = build_system_prompt("iagai", "schema", current_view=None)
        assert "CURRENT VIEW" not in prompt

    def test_default_argument_backward_compatible(self):
        """Callers that pre-date this change (there are some in tests)
        must keep working."""
        assert "CURRENT VIEW" not in build_system_prompt("iagai", "schema")
