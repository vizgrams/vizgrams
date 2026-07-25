# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the AG-UI RunAgentInput parser in the /chat/stream endpoint.

The frontend's HttpAgent POSTs a full ``RunAgentInput`` (threadId,
runId, messages, tools, context, forwardedProps). Our stream endpoint
extracts the fresh user message + prior history and hands off to
``chat_turn`` — this parser is what makes the wire protocol round-trip
cleanly. Getting it wrong looks like "the LLM never sees the user's
last message" or "history includes an echo of the current turn".
"""

from api.routers.chat import _extract_text, _split_run_agent_input


class TestExtractText:
    def test_string_content_passthrough(self):
        assert _extract_text("hello") == "hello"

    def test_parts_array_concatenates_text_only(self):
        # AG-UI can send content as [{type: 'text', text: 'a'}, {type: 'image', ...}]
        content = [
            {"type": "text", "text": "top 5 "},
            {"type": "image", "url": "x"},
            {"type": "text", "text": "activities"},
        ]
        assert _extract_text(content) == "top 5 activities"

    def test_empty_or_none_content_is_empty_string(self):
        assert _extract_text(None) == ""
        assert _extract_text([]) == ""


class TestSplitRunAgentInput:
    def test_single_user_message_yields_message_and_empty_history(self):
        msg, hist = _split_run_agent_input([
            {"role": "user", "content": "hello"},
        ])
        assert msg == "hello"
        assert hist == []

    def test_prior_history_returned_without_trailing_user(self):
        msg, hist = _split_run_agent_input([
            {"role": "user", "content": "prs?"},
            {"role": "assistant", "content": "here"},
            {"role": "user", "content": "and this week?"},
        ])
        assert msg == "and this week?"
        # History is everything BEFORE the trailing user message —
        # including it would make the LLM see a duplicate of the current
        # turn in its context.
        assert hist == [
            {"role": "user", "content": "prs?"},
            {"role": "assistant", "content": "here"},
        ]

    def test_content_parts_are_collapsed_to_text(self):
        msg, _ = _split_run_agent_input([
            {"role": "user", "content": [{"type": "text", "text": "hey"}]},
        ])
        assert msg == "hey"

    def test_no_user_message_returns_empty(self):
        """Endpoint responds 422 on empty user input — the split must
        surface the absence rather than silently invent a message."""
        msg, hist = _split_run_agent_input([
            {"role": "assistant", "content": "unprompted"},
        ])
        assert msg == ""

    def test_empty_message_list(self):
        assert _split_run_agent_input([]) == ("", [])

    def test_system_message_flows_into_history(self):
        """System prompts from the client are kept as history — the
        server may prepend its own, but not swallowing the client's is
        the safer default."""
        _, hist = _split_run_agent_input([
            {"role": "system", "content": "you're helpful"},
            {"role": "user", "content": "hi"},
        ])
        assert hist == [{"role": "system", "content": "you're helpful"}]
