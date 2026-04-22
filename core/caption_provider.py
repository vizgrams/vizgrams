# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""caption_provider.py — pluggable LLM backend for vizgram caption generation.

Provider is selected by the ``VZ_CAPTION_PROVIDER`` environment variable:

  anthropic (default) — Anthropic API via the ``anthropic`` SDK.
                        Requires: ANTHROPIC_API_KEY
  openai              — OpenAI API via the ``openai`` SDK.
                        Requires: OPENAI_API_KEY
  bedrock             — AWS Bedrock via ``boto3`` (Converse API).
                        Requires: AWS credentials in environment; AWS_REGION (default us-east-1)
  ollama              — Local LLM via Ollama HTTP API.
                        Requires: OLLAMA_BASE_URL (default http://localhost:11434)
  none                — Disabled. Vizgrams are published without captions.

Optional override for all providers: VZ_CAPTION_MODEL

The single required method is ``generate(*, prompt: str) -> str``.  Callers
build the prompt via ``build_caption_prompt()``.  All provider details are
hidden behind this interface so swapping backends requires only an env var
change.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------

class CaptionProvider(ABC):
    @abstractmethod
    def generate(self, *, prompt: str) -> str:
        """Generate a caption for the given prompt. Returns the caption text."""


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def build_caption_prompt(
    *,
    title: str,
    query_ref: str,
    dataset_ref: str,
    chart_type: str,
    columns: list[str],
    sample_rows: list,
) -> str:
    """Build the caption generation prompt from vizgram metadata."""
    rows_text = "\n".join(str(r) for r in sample_rows[:5]) if sample_rows else "(no data)"
    return (
        f"Generate a short, insight-focused caption for a data visualization.\n\n"
        f"Chart type: {chart_type}\n"
        f"Title: {title}\n"
        f"Source: {dataset_ref} / {query_ref}\n"
        f"Columns: {', '.join(columns)}\n"
        f"Sample data (first rows):\n{rows_text}\n\n"
        f"Write 1-2 sentences highlighting the key insight. Be specific about numbers where "
        f"visible. Do not start with 'This chart' or 'The data shows'. Be direct and concise."
    )


def compute_snapshot_hash(data_snapshot: list | None) -> str:
    """Return a stable SHA-256 hex digest of the data snapshot."""
    payload = json.dumps(data_snapshot or [], sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


# ---------------------------------------------------------------------------
# NullProvider — captions disabled
# ---------------------------------------------------------------------------

class NullProvider(CaptionProvider):
    def generate(self, *, prompt: str) -> str:
        return ""


# ---------------------------------------------------------------------------
# AnthropicProvider — Anthropic API
# ---------------------------------------------------------------------------

class AnthropicProvider(CaptionProvider):
    def __init__(self, *, api_key: str, model: str) -> None:
        try:
            import anthropic
        except ImportError as exc:
            raise ImportError(
                "anthropic package is required for AnthropicProvider. "
                "Install it with: pip install anthropic"
            ) from exc
        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model

    def generate(self, *, prompt: str) -> str:
        msg = self._client.messages.create(
            model=self._model,
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()


# ---------------------------------------------------------------------------
# OpenAIProvider — OpenAI API
# ---------------------------------------------------------------------------

class OpenAIProvider(CaptionProvider):
    def __init__(self, *, api_key: str, model: str) -> None:
        try:
            import openai
        except ImportError as exc:
            raise ImportError(
                "openai package is required for OpenAIProvider. "
                "Install it with: pip install openai"
            ) from exc
        self._client = openai.OpenAI(api_key=api_key)
        self._model = model

    def generate(self, *, prompt: str) -> str:
        response = self._client.chat.completions.create(
            model=self._model,
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# BedrockProvider — AWS Bedrock (Converse API, model-agnostic)
# ---------------------------------------------------------------------------

class BedrockProvider(CaptionProvider):
    def __init__(self, *, model: str, region: str) -> None:
        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                "boto3 is required for BedrockProvider. "
                "Install it with: pip install boto3"
            ) from exc
        self._client = boto3.client("bedrock-runtime", region_name=region)
        self._model = model

    def generate(self, *, prompt: str) -> str:
        response = self._client.converse(
            modelId=self._model,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": 256},
        )
        return response["output"]["message"]["content"][0]["text"].strip()


# ---------------------------------------------------------------------------
# OllamaProvider — local LLM via Ollama HTTP API
# ---------------------------------------------------------------------------

class OllamaProvider(CaptionProvider):
    def __init__(self, *, base_url: str, model: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._model = model

    def generate(self, *, prompt: str) -> str:
        try:
            import httpx
        except ImportError as exc:
            raise ImportError(
                "httpx is required for OllamaProvider. "
                "Install it with: pip install httpx"
            ) from exc
        response = httpx.post(
            f"{self._base_url}/api/generate",
            json={"model": self._model, "prompt": prompt, "stream": False},
            timeout=60.0,
        )
        response.raise_for_status()
        return response.json()["response"].strip()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_caption_provider() -> CaptionProvider:
    """Return the configured CaptionProvider instance.

    Reads VZ_CAPTION_PROVIDER (default: 'anthropic'). Falls back to
    NullProvider if the required credentials are absent rather than raising,
    so a missing API key degrades gracefully instead of breaking publish.
    """
    name = os.environ.get("VZ_CAPTION_PROVIDER", "anthropic").lower()
    model_override = os.environ.get("VZ_CAPTION_MODEL")

    if name == "none":
        return NullProvider()

    if name == "anthropic":
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            logger.warning(
                "VZ_CAPTION_PROVIDER=anthropic but ANTHROPIC_API_KEY is not set — "
                "captions disabled. Set ANTHROPIC_API_KEY to enable."
            )
            return NullProvider()
        model = model_override or "claude-haiku-4-5-20251001"
        return AnthropicProvider(api_key=api_key, model=model)

    if name == "openai":
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            logger.warning(
                "VZ_CAPTION_PROVIDER=openai but OPENAI_API_KEY is not set — "
                "captions disabled. Set OPENAI_API_KEY to enable."
            )
            return NullProvider()
        model = model_override or "gpt-4o-mini"
        return OpenAIProvider(api_key=api_key, model=model)

    if name == "bedrock":
        region = os.environ.get("AWS_REGION", "us-east-1")
        model = model_override or "anthropic.claude-3-haiku-20240307-v1:0"
        return BedrockProvider(model=model, region=region)

    if name == "ollama":
        base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
        model = model_override or "llama3"
        return OllamaProvider(base_url=base_url, model=model)

    raise ValueError(
        f"Unknown VZ_CAPTION_PROVIDER value: {name!r}. "
        f"Valid options: 'anthropic', 'openai', 'bedrock', 'ollama', 'none'."
    )
