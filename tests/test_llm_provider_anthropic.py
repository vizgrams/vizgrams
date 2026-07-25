# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the Anthropic ↔ OpenAI message translation in ``AnthropicClient``.

The Anthropic API doesn't accept an OpenAI-shaped history verbatim —
system prompts move to an out-of-band field, tool calls become
``tool_use`` content blocks on an assistant turn, and tool results
become ``tool_result`` blocks on the *next* user turn. Getting this
wrong looks like intermittent 400s from Anthropic that only surface
when the LLM decides to call a tool, so it's worth pinning down.
"""

from semantic.llm.provider import AnthropicClient


class TestSystemPromptExtraction:
    def test_system_role_hoisted_out_of_messages(self):
        system, msgs = AnthropicClient._to_anthropic_messages([
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hi"},
        ])
        assert system == "You are helpful."
        assert msgs == [{"role": "user", "content": "Hi"}]

    def test_multiple_system_messages_concatenate(self):
        """Some flows layer a base prompt + a per-model addendum as two
        role='system' messages; both need to survive."""
        system, _ = AnthropicClient._to_anthropic_messages([
            {"role": "system", "content": "Base."},
            {"role": "system", "content": "Addendum."},
            {"role": "user", "content": "Hi"},
        ])
        assert "Base." in system and "Addendum." in system


class TestToolUseBlocks:
    def test_assistant_with_tool_calls_becomes_content_block(self):
        _, msgs = AnthropicClient._to_anthropic_messages([
            {"role": "user", "content": "get prs"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": {"name": "find_prs", "arguments": '{"state": "open"}'},
                }],
            },
        ])
        # The assistant turn should carry a tool_use block, not a text string
        assistant = next(m for m in msgs if m["role"] == "assistant")
        assert isinstance(assistant["content"], list)
        tool_use = next(b for b in assistant["content"] if b["type"] == "tool_use")
        assert tool_use["id"] == "tc_1"
        assert tool_use["name"] == "find_prs"
        assert tool_use["input"] == {"state": "open"}

    def test_tool_result_lands_on_next_user_turn(self):
        _, msgs = AnthropicClient._to_anthropic_messages([
            {"role": "user", "content": "get prs"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [{
                    "id": "tc_1", "type": "function",
                    "function": {"name": "find_prs", "arguments": "{}"},
                }],
            },
            {"role": "tool", "tool_call_id": "tc_1", "content": "3 rows"},
        ])
        # Tool-result must be wrapped as a user turn with a tool_result block
        # — Anthropic rejects a standalone 'tool' role.
        last = msgs[-1]
        assert last["role"] == "user"
        block = last["content"][0]
        assert block["type"] == "tool_result"
        assert block["tool_use_id"] == "tc_1"
        assert block["content"] == "3 rows"


class TestToolDefTranslation:
    def test_openai_function_def_becomes_anthropic_tool(self):
        tools = AnthropicClient._to_anthropic_tools([{
            "type": "function",
            "function": {
                "name": "run_query",
                "description": "Execute a saved query.",
                "parameters": {"type": "object", "properties": {"name": {"type": "string"}}},
            },
        }])
        assert tools == [{
            "name": "run_query",
            "description": "Execute a saved query.",
            "input_schema": {"type": "object", "properties": {"name": {"type": "string"}}},
        }]

    def test_missing_parameters_gets_empty_object_schema(self):
        """Anthropic rejects tools without an input_schema; OpenAI accepts
        omission. Fill in a permissive default so the round-trip doesn't
        400 on a no-arg tool."""
        tools = AnthropicClient._to_anthropic_tools([{
            "type": "function",
            "function": {"name": "ping", "description": "no args"},
        }])
        assert tools[0]["input_schema"] == {"type": "object", "properties": {}}
