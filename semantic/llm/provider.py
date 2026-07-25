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
# Anthropic implementation
# ---------------------------------------------------------------------------


class AnthropicClient:
    """LLMClient backed by the Anthropic Messages API.

    Translates OpenAI-shape messages to Anthropic's ``messages`` array +
    ``system`` string (OpenAI carries system prompts as a role='system'
    message; Anthropic wants them out-of-band). Tool defs translate 1:1:
    OpenAI's ``{type: 'function', function: {name, description,
    parameters}}`` becomes Anthropic's ``{name, description,
    input_schema}``.
    """

    def __init__(self, *, api_key: str, default_model: str = "claude-haiku-4-5-20251001") -> None:
        try:
            from anthropic import Anthropic
        except ImportError as exc:
            raise ImportError(
                "anthropic package is required for AnthropicClient. "
                "Install with: poetry add anthropic"
            ) from exc
        self._client = Anthropic(api_key=api_key)
        self._default_model = default_model

    @staticmethod
    def _to_anthropic_messages(messages: list[dict]) -> tuple[str, list[dict]]:
        """Split OpenAI-shape history into (system_prompt, message_list).

        OpenAI: ``[{role: 'system', content: ...}, {role: 'user', ...}, …]``
        Anthropic: ``system="…", messages=[{role: 'user'/'assistant', content: …}]``

        Assistant messages with tool_calls translate to Anthropic
        ``content`` blocks (`{type: 'tool_use', …}`); the paired
        ``role='tool'`` result translates to the next user message's
        ``content`` block (`{type: 'tool_result', …}`).
        """
        system_parts: list[str] = []
        out: list[dict] = []
        for m in messages:
            role = m.get("role")
            if role == "system":
                system_parts.append(m.get("content") or "")
                continue
            if role == "tool":
                # Anthropic packs tool_result into the *next* user message.
                # If the previous assistant emitted tool_use, and now we're
                # feeding back its result, wrap as a user-turn tool_result.
                out.append({
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": m.get("tool_call_id"),
                        "content": m.get("content") or "",
                    }],
                })
                continue
            if role == "assistant" and m.get("tool_calls"):
                blocks: list[dict] = []
                if m.get("content"):
                    blocks.append({"type": "text", "text": m["content"]})
                for tc in m["tool_calls"]:
                    args = tc.get("function", {}).get("arguments") or "{}"
                    try:
                        parsed = json.loads(args) if isinstance(args, str) else args
                    except json.JSONDecodeError:
                        parsed = {}
                    blocks.append({
                        "type": "tool_use",
                        "id": tc.get("id"),
                        "name": tc.get("function", {}).get("name"),
                        "input": parsed,
                    })
                out.append({"role": "assistant", "content": blocks})
                continue
            out.append({"role": role, "content": m.get("content") or ""})
        return ("\n\n".join(system_parts).strip(), out)

    @staticmethod
    def _to_anthropic_tools(tools: list[dict]) -> list[dict]:
        result = []
        for t in tools:
            fn = t.get("function", t)
            result.append({
                "name": fn["name"],
                "description": fn.get("description", ""),
                "input_schema": fn.get("parameters", {"type": "object", "properties": {}}),
            })
        return result

    def complete(
        self,
        *,
        messages: list[dict],
        tools: list[dict] | None = None,
        model: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.0,
    ) -> LLMResponse:
        system, anthropic_messages = self._to_anthropic_messages(messages)
        kwargs: dict[str, Any] = {
            "model": model or self._default_model,
            "messages": anthropic_messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if system:
            kwargs["system"] = system
        if tools:
            kwargs["tools"] = self._to_anthropic_tools(tools)

        resp = self._client.messages.create(**kwargs)
        text_parts: list[str] = []
        calls: list[ToolCall] = []
        for block in resp.content:
            btype = getattr(block, "type", None)
            if btype == "text":
                text_parts.append(block.text)
            elif btype == "tool_use":
                calls.append(ToolCall(
                    id=block.id,
                    name=block.name,
                    arguments=block.input if isinstance(block.input, dict) else {},
                ))
        return LLMResponse(
            content="".join(text_parts) or None,
            tool_calls=calls,
            raw=resp,
        )


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

    if provider == "anthropic":
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise RuntimeError(
                "VZ_LLM_PROVIDER=anthropic but ANTHROPIC_API_KEY is not set."
            )
        return AnthropicClient(
            api_key=api_key,
            default_model=model_override or "claude-haiku-4-5-20251001",
        )

    raise ValueError(
        f"Unknown VZ_LLM_PROVIDER: {provider!r}. "
        f"Currently supported: 'openai', 'anthropic'."
    )
