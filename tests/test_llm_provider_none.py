# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for VZ_LLM_PROVIDER=none (chat capability disabled) and the
provider-error translation layer.

The disabled state must be first-class:
- Factory returns a NoOpClient without raising (deployment doesn't need
  to leave keys lying around to disable chat).
- Calling ``complete()`` raises ``ChatDisabledError``, which is a
  distinct type so the /chat/stream endpoint can catch and emit a
  RUN_ERROR with a friendly ``code=chat_disabled`` marker.
- ``is_chat_enabled()`` is cheap so the config endpoint doesn't need
  to construct a full client.
"""

import pytest

from semantic.llm.provider import (
    ChatAuthError,
    ChatCreditsExhaustedError,
    ChatDisabledError,
    ChatRateLimitError,
    LLMBadRequestError,
    NoOpClient,
    get_default_client,
    is_chat_enabled,
    translate_provider_error,
)


class TestNoOpClient:
    def test_provider_none_returns_noop(self, monkeypatch):
        monkeypatch.setenv("VZ_LLM_PROVIDER", "none")
        client = get_default_client()
        assert isinstance(client, NoOpClient)

    def test_provider_none_does_not_require_api_keys(self, monkeypatch):
        """The whole point of ``none`` is disabling chat WITHOUT keys."""
        monkeypatch.setenv("VZ_LLM_PROVIDER", "none")
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        # Should not raise
        get_default_client()

    def test_complete_raises_chat_disabled(self):
        client = NoOpClient()
        with pytest.raises(ChatDisabledError, match="disabled"):
            client.complete(messages=[])

    def test_is_chat_enabled_reflects_provider(self, monkeypatch):
        monkeypatch.setenv("VZ_LLM_PROVIDER", "none")
        assert is_chat_enabled() is False
        monkeypatch.setenv("VZ_LLM_PROVIDER", "openai")
        assert is_chat_enabled() is True
        monkeypatch.delenv("VZ_LLM_PROVIDER", raising=False)
        assert is_chat_enabled() is True  # default (openai)


class TestErrorTranslation:
    """The point of translation is to turn opaque provider SDK errors
    (which surface stack traces in the UI) into a small hierarchy of
    user-actionable messages."""

    def test_anthropic_credit_balance_maps_to_credits_exhausted(self):
        # Real message shape returned by anthropic-python when the
        # account is out of funds; PermissionDeniedError with a body.
        exc = _FakeSdkError(
            "PermissionDeniedError",
            "Your credit balance is too low to access the Anthropic API.",
        )
        result = translate_provider_error(exc)
        assert isinstance(result, ChatCreditsExhaustedError)
        assert "out of credit" in str(result).lower()

    def test_openai_insufficient_quota_maps_to_credits_exhausted(self):
        # OpenAI packs quota errors inside RateLimitError with an
        # ``insufficient_quota`` code — must NOT be treated as a
        # transient rate limit (top up, not retry).
        exc = _FakeSdkError(
            "RateLimitError",
            "You exceeded your current quota. insufficient_quota",
        )
        result = translate_provider_error(exc)
        assert isinstance(result, ChatCreditsExhaustedError)

    def test_authentication_error_maps_to_auth(self):
        exc = _FakeSdkError("AuthenticationError", "Invalid API key.")
        result = translate_provider_error(exc)
        assert isinstance(result, ChatAuthError)
        # Message steers to the env var, not the code
        assert "API key" in str(result)

    def test_rate_limit_error_maps_to_rate_limit(self):
        """A plain rate-limit (no quota keyword) is transient — user
        should be told to retry rather than top up their account."""
        exc = _FakeSdkError("RateLimitError", "Rate limit exceeded. Try again in 30s.")
        result = translate_provider_error(exc)
        assert isinstance(result, ChatRateLimitError)

    def test_unknown_error_returns_none(self):
        """Callers should fall through to a generic message when the
        translation layer doesn't recognise the error — silently
        remapping novel failures would hide real bugs."""
        exc = _FakeSdkError("SomeInternalTypeError", "unexpected shape")
        assert translate_provider_error(exc) is None

    def test_bad_request_surfaces_provider_body(self):
        """400s are usually our fault (wrong tool schema, empty
        message). The provider's own error text is the diagnostic —
        surface it verbatim so users don't waste time on the wrong
        theory."""
        exc = _FakeSdkError(
            "BadRequestError",
            "tools.0.input_schema: Unknown keyword 'strict'",
        )
        # Anthropic packs the human message on ``exc.body.error.message``
        exc.body = {"error": {"message": "tools.0.input_schema: Unknown keyword 'strict'"}}
        result = translate_provider_error(exc)
        assert isinstance(result, LLMBadRequestError)
        assert "strict" in str(result)  # provider body flowed through
        assert "invalid" in str(result).lower()


class _FakeSdkError(Exception):
    """Exception with an overridden class-name for the translator to
    match on. The real anthropic/openai SDKs use module-level classes
    (anthropic.PermissionDeniedError etc.); the translator matches on
    ``type(exc).__name__`` so we can stand in a stub without importing
    the real SDKs into the test file.
    """

    def __init__(self, name: str, msg: str):
        super().__init__(msg)
        self.__class__ = type(name, (Exception,), {})
