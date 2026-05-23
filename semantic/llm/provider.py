# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""LLM client abstraction for the text2X tool family.

Differs from ``core/caption_provider.py`` in two important ways:
  - Tool-use support (caption gen is a single-shot text completion)
  - Multi-turn message history (text2X tools may retry on validation error)

Message format is OpenAI Chat Completion shape — the de facto lingua franca
across SDKs. Adapters for other providers (Anthropic, Bedrock) translate to
their native shapes inside ``complete()``.

The protocol is intentionally small: one method, ``complete``. Streaming
and function-style helpers are out of scope; callers that need them wrap a
client themselves.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class ToolCall:
    """One tool invocation requested by the LLM."""

    id: str
    name: str
    arguments: dict  # parsed JSON; empty dict if the LLM passed no args


@dataclass
class LLMResponse:
    """A single response from the LLM — text, tool calls, or both."""

    content: str | None
    tool_calls: list[ToolCall] = field(default_factory=list)
    # Opaque provider-specific extras (usage stats, raw SDK response, etc.).
    # Callers may inspect this for diagnostics but should not rely on shape.
    raw: Any = None


@runtime_checkable
class LLMClient(Protocol):
    """Protocol every LLM provider must implement.

    ``complete`` is one round-trip: take an OpenAI-shape message history,
    return the LLM's next response. Tool definitions are passed each call
    so the caller can vary them per turn.
    """

    def complete(
        self,
        *,
        messages: list[dict],
        tools: list[dict] | None = None,
        model: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.0,
    ) -> LLMResponse: ...


# ---------------------------------------------------------------------------
# OpenAI implementation
# ---------------------------------------------------------------------------


class OpenAIClient:
    """LLMClient backed by the OpenAI Chat Completions API."""

    def __init__(self, *, api_key: str, default_model: str = "gpt-4o-mini") -> None:
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise ImportError(
                "openai package is required for OpenAIClient. "
                "Install with: poetry add openai"
            ) from exc
        self._client = OpenAI(api_key=api_key)
        self._default_model = default_model

    def complete(
        self,
        *,
        messages: list[dict],
        tools: list[dict] | None = None,
        model: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.0,
    ) -> LLMResponse:
        kwargs: dict[str, Any] = {
            "model": model or self._default_model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"

        resp = self._client.chat.completions.create(**kwargs)
        msg = resp.choices[0].message
        calls: list[ToolCall] = []
        for tc in msg.tool_calls or []:
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
            calls.append(ToolCall(id=tc.id, name=tc.function.name, arguments=args))
        return LLMResponse(content=msg.content, tool_calls=calls, raw=resp)


# ---------------------------------------------------------------------------
# Factory — read provider choice from environment
# ---------------------------------------------------------------------------


def get_default_client() -> LLMClient:
    """Construct an LLMClient from environment variables.

    Reads ``VZ_LLM_PROVIDER`` (default ``openai``). Each provider has its
    own credential env vars; missing credentials raise ``RuntimeError`` —
    callers that want graceful degradation should catch and fall back.
    """
    provider = os.environ.get("VZ_LLM_PROVIDER", "openai").lower()
    model_override = os.environ.get("VZ_LLM_MODEL")

    if provider == "openai":
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            raise RuntimeError(
                "VZ_LLM_PROVIDER=openai but OPENAI_API_KEY is not set."
            )
        return OpenAIClient(
            api_key=api_key,
            default_model=model_override or "gpt-4o-mini",
        )

    raise ValueError(
        f"Unknown VZ_LLM_PROVIDER: {provider!r}. "
        f"Currently supported: 'openai'."
    )
