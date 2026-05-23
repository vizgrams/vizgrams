# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""LLM-backed semantic-layer tools (text2X family).

Each ``text2X_yaml`` function is a pure capability: given a natural-language
prompt plus the relevant context (model schema, query result, etc.), return
a validated artifact YAML and any execution side effects.

The functions take protocol-typed dependencies (``LLMClient``,
``QueryExecutor``) so tests can substitute fakes without touching the
production wiring.
"""

from semantic.llm.provider import (
    LLMClient,
    LLMResponse,
    OpenAIClient,
    ToolCall,
    get_default_client,
)
from semantic.llm.schema_context import build_schema_context

__all__ = [
    "LLMClient",
    "LLMResponse",
    "OpenAIClient",
    "ToolCall",
    "build_schema_context",
    "get_default_client",
]
