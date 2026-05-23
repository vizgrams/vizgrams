# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Test doubles for semantic/llm modules.

The two protocols (``LLMClient`` and ``QueryExecutor``) are the only
seams text2X needs to be unit-testable end-to-end. These fakes record
what they received so tests can assert on call sequences.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from semantic.llm.provider import LLMResponse, ToolCall
from semantic.llm.text2query import QueryExecutionResult
from semantic.query import QueryDef


@dataclass
class FakeLLMClient:
    """LLMClient that returns canned responses in order."""

    responses: list[LLMResponse] = field(default_factory=list)
    received: list[dict] = field(default_factory=list)

    def complete(
        self,
        *,
        messages,
        tools=None,
        model=None,
        max_tokens=1024,
        temperature=0.0,
    ):
        self.received.append({
            "messages": list(messages),
            "tools": list(tools) if tools else None,
            "model": model,
        })
        if not self.responses:
            raise AssertionError(
                "FakeLLMClient ran out of canned responses — test missed an LLM call"
            )
        return self.responses.pop(0)


@dataclass
class FakeQueryExecutor:
    """QueryExecutor that returns canned results in order."""

    results: list[QueryExecutionResult] = field(default_factory=list)
    received: list[QueryDef] = field(default_factory=list)

    def execute(self, query: QueryDef) -> QueryExecutionResult:
        self.received.append(query)
        if not self.results:
            raise AssertionError(
                "FakeQueryExecutor ran out of canned results — test missed an execute call"
            )
        return self.results.pop(0)


# ---------------------------------------------------------------------------
# Helpers for building LLMResponses concisely
# ---------------------------------------------------------------------------


def tool_call(name: str, arguments: dict, call_id: str = "call_1") -> ToolCall:
    return ToolCall(id=call_id, name=name, arguments=arguments)


def response_with_tool(name: str, arguments: dict, call_id: str = "call_1") -> LLMResponse:
    return LLMResponse(content=None, tool_calls=[tool_call(name, arguments, call_id)])


def response_text(text: str) -> LLMResponse:
    return LLMResponse(content=text, tool_calls=[])


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_llm():
    """Empty FakeLLMClient — tests append canned responses as needed."""
    return FakeLLMClient()


@pytest.fixture
def fake_executor():
    """Empty FakeQueryExecutor — tests append canned results as needed."""
    return FakeQueryExecutor()


@pytest.fixture
def schema_iagai_tiny():
    """Minimal schema string for tests that don't depend on a real model."""
    return (
        "MODEL: iagai\n\n"
        "ENTITY PullRequest — a pull request\n"
        "  identity: pull_request_key\n"
        "  attributes: title:STRING, state:STRING, created_at:TIMESTAMP\n"
        "  relations: author (N→1 Identity), belongs_to (N→1 Repository)\n"
        "\n"
        "ENTITY Identity\n"
        "  identity: identity_key\n"
        "  attributes: name:STRING, email:STRING\n"
    )
