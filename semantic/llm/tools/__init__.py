# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Pluggable LLM tools (Epic 20 VG-231).

A small registry pattern decouples tool definitions from the orchestrators
(``text2query``, ``text2view``, future text2X). Each tool is a
``Tool(name, description, parameters_schema, handler)`` registered in
a ``ToolRegistry``; orchestrators ask the registry to render tools for
the LLM (OpenAI shape or MCP shape) and to dispatch calls back to
handlers.

Why bother:
  - New tools (``find_artifacts``, ``run_saved_query``, ``run_saved_view``
    in VG-232..234) slot in without touching ``text2query.py``.
  - Customers can register their own tools via the same registry — the
    plugin path is the same as in-process registration.
  - The Tool dataclass maps cleanly to MCP's
    ``{name, description, inputSchema}`` shape (see
    ``ToolRegistry.to_mcp_definitions``), so an MCP server can serve
    the same tools to Claude Desktop / IDE integrations later
    without re-implementation.
"""

from semantic.llm.tools.registry import (
    Tool,
    ToolCallTrace,
    ToolContext,
    ToolRegistry,
    ToolResult,
    build_default_registry,
    summarize_tool_result,
)

__all__ = [
    "Tool",
    "ToolCallTrace",
    "ToolContext",
    "ToolRegistry",
    "ToolResult",
    "build_default_registry",
    "summarize_tool_result",
]
