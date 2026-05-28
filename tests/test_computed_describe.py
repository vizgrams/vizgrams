# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the LLM "Describe a computed feature" helper (Epic 26 VG-293).

Service-layer tests with a FakeLLMClient — verify the prompt-shaping +
tool-call extraction + error paths without burning real LLM calls. The
HTTP route is a thin wrapper that delegates here; its router-level
behavior (404 / 401 / 400) follows the same pattern as every other
entities endpoint and is exercised by ``test_api_entities.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from api.services.computed_describe_service import (
    _build_system_prompt,
    describe_computed,
)
from semantic.llm.provider import LLMResponse, ToolCall

_ENTITY_YAML = """\
entity: PullRequest
identity:
  pull_request_key:
    type: STRING
    semantic: PRIMARY_KEY
attributes:
  created_at:
    type: STRING
    semantic: TIMESTAMP
  merged_at:
    type: STRING
    semantic: TIMESTAMP
  state:
    type: STRING
    semantic: STATUS
"""


# ---------------------------------------------------------------------------
# Fake LLM client
# ---------------------------------------------------------------------------


@dataclass
class FakeLLMClient:
    """Records every call + returns canned responses."""
    response: LLMResponse | None = None
    calls: list[dict] = field(default_factory=list)

    def complete(self, **kwargs) -> LLMResponse:
        self.calls.append(kwargs)
        return self.response or LLMResponse(content="", tool_calls=[])


def _tool_response(name: str, expr: str) -> LLMResponse:
    return LLMResponse(
        content=None,
        tool_calls=[ToolCall(id="t1", name="make_computed",
                             arguments={"name": name, "expr": expr})],
    )


# ---------------------------------------------------------------------------
# Service — prompt shaping + response parsing
# ---------------------------------------------------------------------------


def test_system_prompt_lists_entity_attributes_and_relations():
    schema = {
        "attributes": [
            {"name": "title", "type": "STRING", "semantic": "IDENTIFIER"},
            {"name": "merged_at", "type": "TIMESTAMP", "semantic": "TIMESTAMP"},
        ],
        "relations": [
            {"name": "author", "target": "Person", "cardinality": "many-to-one"},
        ],
    }
    prompt = _build_system_prompt("PullRequest", schema)
    assert "PullRequest" in prompt
    assert "- title: STRING" in prompt
    assert "- merged_at: TIMESTAMP" in prompt
    assert "author → Person (many-to-one)" in prompt


def test_system_prompt_renders_none_when_lists_empty():
    prompt = _build_system_prompt("Lonely", {"attributes": [], "relations": []})
    assert "- (none)" in prompt


def test_describe_returns_name_and_expr_from_tool_call():
    fake = FakeLLMClient(response=_tool_response(
        "lead_time_hours",
        'datetime_diff(merged_at, created_at, "hours")',
    ))
    out = describe_computed(
        entity_name="PullRequest",
        entity_schema={"attributes": [], "relations": []},
        description="lead time in hours from created to merged",
        llm_client=fake,
    )
    assert out == {
        "name": "lead_time_hours",
        "expr": 'datetime_diff(merged_at, created_at, "hours")',
    }


def test_describe_passes_description_to_llm_as_user_message():
    fake = FakeLLMClient(response=_tool_response("x", "1"))
    describe_computed(
        entity_name="Widget",
        entity_schema={"attributes": [], "relations": []},
        description="count of widgets",
        llm_client=fake,
    )
    assert fake.calls
    msgs = fake.calls[0]["messages"]
    assert msgs[1] == {"role": "user", "content": "count of widgets"}
    assert msgs[0]["role"] == "system"


def test_describe_forces_tool_call_temperature_zero():
    """We want deterministic structured output — temperature pinned to 0."""
    fake = FakeLLMClient(response=_tool_response("x", "1"))
    describe_computed(
        entity_name="Widget",
        entity_schema={"attributes": [], "relations": []},
        description="x",
        llm_client=fake,
    )
    assert fake.calls[0]["temperature"] == 0.0
    assert fake.calls[0]["tools"]  # tool list passed


def test_describe_strips_whitespace_around_description():
    fake = FakeLLMClient(response=_tool_response("x", "1"))
    describe_computed(
        entity_name="Widget", entity_schema={}, description="  count of widgets  \n",
        llm_client=fake,
    )
    assert fake.calls[0]["messages"][1]["content"] == "count of widgets"


# ---------------------------------------------------------------------------
# Service — error paths
# ---------------------------------------------------------------------------


def test_describe_raises_when_description_empty():
    fake = FakeLLMClient()
    with pytest.raises(ValueError, match="description is required"):
        describe_computed(
            entity_name="Widget", entity_schema={}, description="   ",
            llm_client=fake,
        )
    # LLM should not have been called.
    assert fake.calls == []


def test_describe_raises_when_llm_returns_prose_instead_of_tool_call():
    """Real-world failure mode — model ignores the tool and writes prose.
    Caller surfaces this as a user-visible error so they can retry."""
    fake = FakeLLMClient(response=LLMResponse(content="Sure!", tool_calls=[]))
    with pytest.raises(ValueError, match="prose instead of"):
        describe_computed(
            entity_name="Widget", entity_schema={}, description="anything",
            llm_client=fake,
        )


def test_describe_raises_on_malformed_tool_args():
    fake = FakeLLMClient(response=LLMResponse(
        content=None,
        tool_calls=[ToolCall(id="t", name="make_computed",
                             arguments={"name": "x"})],  # missing expr
    ))
    with pytest.raises(ValueError, match="malformed"):
        describe_computed(
            entity_name="Widget", entity_schema={}, description="x",
            llm_client=fake,
        )


# ---------------------------------------------------------------------------

# Wire-level HTTP tests are skipped — the route is a thin wrapper that
# delegates to describe_computed (covered above) and follows the
# same error-translation pattern as other endpoints in the entities
# router. Adding HTTP tests would re-test the FastAPI router wiring
# rather than the describe-it behavior.
