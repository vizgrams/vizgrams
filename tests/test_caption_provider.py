# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for core/caption_provider.py"""

import os
import pytest
from unittest.mock import MagicMock, patch

from core.caption_provider import (
    NullProvider,
    AnthropicProvider,
    BedrockProvider,
    OllamaProvider,
    build_caption_prompt,
    compute_snapshot_hash,
    get_caption_provider,
)


# ---------------------------------------------------------------------------
# NullProvider
# ---------------------------------------------------------------------------

def test_null_provider_returns_empty():
    assert NullProvider().generate(prompt="anything") == ""


# ---------------------------------------------------------------------------
# build_caption_prompt
# ---------------------------------------------------------------------------

def test_build_caption_prompt_contains_key_fields():
    prompt = build_caption_prompt(
        title="Deploy frequency by team",
        query_ref="deploy_freq",
        dataset_ref="acme",
        chart_type="chart",
        columns=["team", "deploys"],
        sample_rows=[["platform", 42], ["mobile", 17]],
    )
    assert "Deploy frequency by team" in prompt
    assert "deploy_freq" in prompt
    assert "acme" in prompt
    assert "chart" in prompt
    assert "team" in prompt
    assert "platform" in prompt


def test_build_caption_prompt_handles_empty_rows():
    prompt = build_caption_prompt(
        title="T", query_ref="q", dataset_ref="d",
        chart_type="table", columns=["a"], sample_rows=[],
    )
    assert "(no data)" in prompt


def test_build_caption_prompt_truncates_to_five_rows():
    rows = [[i] for i in range(20)]
    prompt = build_caption_prompt(
        title="T", query_ref="q", dataset_ref="d",
        chart_type="table", columns=["n"], sample_rows=rows,
    )
    # Only the first 5 rows should appear; row index 5 should not
    assert "[5]" not in prompt


# ---------------------------------------------------------------------------
# compute_snapshot_hash
# ---------------------------------------------------------------------------

def test_compute_snapshot_hash_is_deterministic():
    rows = [["a", 1], ["b", 2]]
    assert compute_snapshot_hash(rows) == compute_snapshot_hash(rows)


def test_compute_snapshot_hash_differs_for_different_data():
    assert compute_snapshot_hash([["a", 1]]) != compute_snapshot_hash([["b", 2]])


def test_compute_snapshot_hash_handles_none():
    h = compute_snapshot_hash(None)
    assert isinstance(h, str) and len(h) == 64


# ---------------------------------------------------------------------------
# get_caption_provider factory
# ---------------------------------------------------------------------------

def test_factory_returns_null_when_provider_is_none(monkeypatch):
    monkeypatch.setenv("VZ_CAPTION_PROVIDER", "none")
    assert isinstance(get_caption_provider(), NullProvider)


def test_factory_falls_back_to_null_when_anthropic_key_missing(monkeypatch):
    monkeypatch.setenv("VZ_CAPTION_PROVIDER", "anthropic")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert isinstance(get_caption_provider(), NullProvider)


def test_factory_returns_anthropic_when_key_present(monkeypatch):
    monkeypatch.setenv("VZ_CAPTION_PROVIDER", "anthropic")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-key")
    with patch.dict("sys.modules", {"anthropic": MagicMock()}):
        provider = get_caption_provider()
    assert isinstance(provider, AnthropicProvider)


def test_factory_respects_model_override(monkeypatch):
    monkeypatch.setenv("VZ_CAPTION_PROVIDER", "anthropic")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-key")
    monkeypatch.setenv("VZ_CAPTION_MODEL", "claude-opus-4-6")
    with patch.dict("sys.modules", {"anthropic": MagicMock()}):
        provider = get_caption_provider()
    assert provider._model == "claude-opus-4-6"


def test_factory_returns_bedrock(monkeypatch):
    monkeypatch.setenv("VZ_CAPTION_PROVIDER", "bedrock")
    with patch.dict("sys.modules", {"boto3": MagicMock()}):
        provider = get_caption_provider()
    assert isinstance(provider, BedrockProvider)


def test_factory_returns_ollama(monkeypatch):
    monkeypatch.setenv("VZ_CAPTION_PROVIDER", "ollama")
    provider = get_caption_provider()
    assert isinstance(provider, OllamaProvider)


def test_factory_raises_on_unknown_provider(monkeypatch):
    monkeypatch.setenv("VZ_CAPTION_PROVIDER", "gemini")
    with pytest.raises(ValueError, match="Unknown VZ_CAPTION_PROVIDER"):
        get_caption_provider()


# ---------------------------------------------------------------------------
# AnthropicProvider — mocked
# ---------------------------------------------------------------------------

def test_anthropic_provider_calls_sdk(monkeypatch):
    mock_anthropic = MagicMock()
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text="  Deploys rose 30% quarter-on-quarter.  ")]
    mock_anthropic.return_value.messages.create.return_value = mock_msg

    with patch.dict("sys.modules", {"anthropic": MagicMock(Anthropic=mock_anthropic)}):
        provider = AnthropicProvider(api_key="sk-test", model="claude-haiku-4-5-20251001")
        result = provider.generate(prompt="test prompt")

    assert result == "Deploys rose 30% quarter-on-quarter."
    mock_anthropic.return_value.messages.create.assert_called_once()


# ---------------------------------------------------------------------------
# OllamaProvider — mocked
# ---------------------------------------------------------------------------

def test_ollama_provider_calls_api(monkeypatch):
    mock_response = MagicMock()
    mock_response.json.return_value = {"response": "  Insight text.  "}
    mock_response.raise_for_status = MagicMock()

    mock_httpx = MagicMock()
    mock_httpx.post.return_value = mock_response

    with patch.dict("sys.modules", {"httpx": mock_httpx}):
        provider = OllamaProvider(base_url="http://localhost:11434", model="llama3")
        result = provider.generate(prompt="test")

    assert result == "Insight text."
    mock_httpx.post.assert_called_once()
    call_kwargs = mock_httpx.post.call_args
    assert "llama3" in str(call_kwargs)
