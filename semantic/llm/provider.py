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


class LLMError(Exception):
    """Base class for LLM-provider errors surfaced to end users. Carries a
    short human-facing message; the underlying exception (if any) is
    preserved via ``__cause__`` for logs and debugging.
    """


class ChatDisabledError(LLMError):
    """Raised when ``VZ_LLM_PROVIDER=none``. Chat endpoints should catch
    this and return 503 with a friendly "chat is disabled on this
    deployment" message rather than a stack trace.
    """


class ChatCreditsExhaustedError(LLMError):
    """The provider refused the request because the account is out of
    credit / over-quota. Distinct from other auth failures because the
    fix is different (top up, not rotate keys)."""


class ChatAuthError(LLMError):
    """The provider rejected the request as unauthenticated — usually a
    missing, mistyped, or revoked API key. Prompts users to check their
    ``.env`` / SSM key rather than debug the code."""


class ChatRateLimitError(LLMError):
    """Provider rate-limited the request. Transient — usually retrying
    after a short pause works."""


def translate_provider_error(exc: Exception) -> LLMError | None:
    """Best-effort translation of provider SDK errors into vizgrams'
    friendly ``LLMError`` hierarchy. Returns ``None`` if the exception
    doesn't match a known provider error — callers should propagate the
    original in that case.
    """
    name = type(exc).__name__
    text = str(exc).lower()

    # Credits / quota — Anthropic and OpenAI both use billing-adjacent
    # error strings. Anthropic's ``PermissionDeniedError`` covers the
    # "credit balance is too low" case; OpenAI's is under
    # ``RateLimitError`` with ``insufficient_quota``.
    credit_signals = ("credit balance", "insufficient_quota", "quota exceeded",
                      "insufficient credits", "credits", "out of credits")
    if any(sig in text for sig in credit_signals):
        return ChatCreditsExhaustedError(
            "Your LLM provider account is out of credit. Top up and retry."
        )

    if name in ("AuthenticationError", "PermissionDeniedError"):
        return ChatAuthError(
            "The LLM provider rejected the API key. Check ANTHROPIC_API_KEY "
            "or OPENAI_API_KEY in your .env, then restart the API."
        )
    if name in ("RateLimitError",):
        return ChatRateLimitError(
            "The LLM provider rate-limited the request. Wait a few seconds "
            "and try again."
        )
    if name in ("BadRequestError", "UnprocessableEntityError"):
        # 400s from the provider almost always mean our request shape is
        # off (wrong tool schema, empty message, invalid model id). The
        # provider's own error text is the most useful signal — surface
        # it verbatim rather than a generic "bad request".
        body = _extract_provider_body(exc)
        return LLMBadRequestError(
            f"The LLM provider rejected our request as invalid. "
            f"Details: {body or str(exc)}"
        )
    return None


class LLMBadRequestError(LLMError):
    """Provider returned 400/422 — usually a request-shape bug (tool
    schema, empty message, invalid model). Distinct from ``ChatAuthError``
    (401/403) so users don't waste time rotating keys.
    """


def _extract_provider_body(exc: Exception) -> str | None:
    """Best-effort extraction of the provider's error body. Anthropic
    packs a JSON body on ``exc.body`` / ``exc.response.text``; OpenAI
    similar. Falls back to None if nothing structured is available so
    the caller can still show ``str(exc)``.
    """
    # Anthropic + OpenAI both put a dict on ``.body`` with an ``error``
    # object containing a ``message``.
    body = getattr(exc, "body", None)
    if isinstance(body, dict):
        inner = body.get("error")
        if isinstance(inner, dict):
            msg = inner.get("message")
            if msg:
                return str(msg)
    resp = getattr(exc, "response", None)
    if resp is not None:
        text = getattr(resp, "text", None)
        if isinstance(text, str) and text:
            return text[:500]
    return None


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

        try:
            resp = self._client.messages.create(**kwargs)
        except Exception as exc:  # noqa: BLE001 - re-raised after logging
            # Anthropic 400s are near-impossible to diagnose from
            # ``str(exc)`` alone. Log the outgoing payload shape (roles
            # + tool names, NOT full content) so ops can spot a bad
            # tool schema or empty message without turning on request
            # tracing.
            import logging
            logging.getLogger(__name__).warning(
                "Anthropic call failed",
                extra={
                    "model": kwargs.get("model"),
                    "message_roles": [m.get("role") for m in anthropic_messages],
                    "tool_names": [t.get("name") for t in kwargs.get("tools", [])],
                    "has_system": bool(system),
                    "provider_body": _extract_provider_body(exc),
                },
            )
            raise
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
# NoOp implementation — the "chat capability disabled" state
# ---------------------------------------------------------------------------


class NoOpClient:
    """An ``LLMClient`` that raises ``ChatDisabledError`` on every call.

    Selected by ``VZ_LLM_PROVIDER=none``. Lets a deployment turn the chat
    surface off cleanly without leaving keys lying around — the endpoint
    catches the specific error and returns a friendly 503 instead of a
    stack trace.
    """

    def complete(self, **_kwargs) -> LLMResponse:
        raise ChatDisabledError(
            "Chat is disabled on this deployment (VZ_LLM_PROVIDER=none)."
        )


def is_chat_enabled() -> bool:
    """Cheap check callers can use to hide chat affordances (nav item,
    landing prompts) without construing an actual client."""
    return os.environ.get("VZ_LLM_PROVIDER", "openai").lower() != "none"


# ---------------------------------------------------------------------------
# Factory — read provider choice from environment
# ---------------------------------------------------------------------------


def get_default_client() -> LLMClient:
    """Construct an LLMClient from environment variables.

    Reads ``VZ_LLM_PROVIDER`` (default ``openai``). Each provider has its
    own credential env vars; missing credentials raise ``RuntimeError`` —
    callers that want graceful degradation should catch and fall back.
    ``VZ_LLM_PROVIDER=none`` returns a ``NoOpClient`` (chat disabled).
    """
    provider = os.environ.get("VZ_LLM_PROVIDER", "openai").lower()
    model_override = os.environ.get("VZ_LLM_MODEL")

    if provider == "none":
        return NoOpClient()

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
        f"Currently supported: 'openai', 'anthropic', 'none'."
    )
