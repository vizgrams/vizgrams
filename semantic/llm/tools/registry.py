# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tool, ToolContext, ToolResult, ToolRegistry — the pluggable spine.

A ``Tool`` is the static definition: name, description, JSON Schema for
its arguments, and a handler function. The registry holds them all and
renders them in whichever shape the LLM provider wants (OpenAI's
``{type, function: {name, description, parameters}}`` envelope or MCP's
``{name, description, inputSchema}`` envelope).

A ``ToolContext`` is the per-call bag of dependencies handlers need to
do their work (executor, semantic search, model_dir, etc.) — populated
by the orchestrator just before the LLM loop. Handlers take
``(args: dict, ctx: ToolContext)`` and return a ``ToolResult``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Forward refs — ToolContext carries optional deps from elsewhere in
# the codebase. Typing them as Protocols keeps the import graph clean.
# ---------------------------------------------------------------------------


@runtime_checkable
class _QueryExecutor(Protocol):
    """Subset of ``semantic.llm.text2query.QueryExecutor`` needed by tools."""

    def execute(self, query: Any) -> Any: ...  # returns QueryExecutionResult


@runtime_checkable
class _SemanticSearch(Protocol):
    """Subset of ``semantic.llm.embeddings.SemanticSearch`` needed by tools."""

    def find(self, query: str, **kwargs: Any) -> list[Any]: ...


# ---------------------------------------------------------------------------
# ToolContext / ToolResult
# ---------------------------------------------------------------------------


@dataclass
class ToolContext:
    """Per-call deps for tool handlers.

    The orchestrator (``chat_turn``) populates this before the LLM loop.
    Tools pull what they need; new tools that need new deps add fields
    here without changing existing tool signatures.

    Every field is optional — present_view, for example, needs nothing
    from the context, and tests can pass ``ToolContext()`` when the tool
    under test doesn't read anything from it.
    """

    model_id: str = ""
    model_dir: Path | None = None
    executor: _QueryExecutor | None = None
    search: _SemanticSearch | None = None
    # Free-form bag for tool-specific extras (e.g. param overrides). Keep
    # it small — prefer typed fields above when something becomes shared.
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResult:
    """Output of a tool handler.

    ``payload`` is the LLM-visible structured result — gets rendered to
    JSON in the ``tool`` role message (with row truncation; see
    ``to_tool_message_content``).

    ``extras`` is for the orchestrator only — typed objects the handler
    wants the caller to use (e.g. a ``QueryDef`` dataclass, the
    pre-compiled SQL string) that aren't meant for the LLM and don't
    need to survive JSON serialisation.
    """

    payload: dict
    success: bool = True
    # Some tools (e.g. ``present_view``) are "terminal" — calling them
    # signals the orchestrator to end the loop. Most tools set this False
    # and let the orchestrator decide based on payload content.
    terminate: bool = False
    extras: dict = field(default_factory=dict)

    def to_tool_message_content(self, *, max_rows: int = 40) -> str:
        """Render a JSON string for the LLM ``tool`` role message.

        Truncates ``payload['rows']`` to ``max_rows`` so wide / long
        result sets don't blow the context window. The full payload
        stays available on the result object for post-loop use.
        """
        out = dict(self.payload)
        rows = out.get("rows")
        if isinstance(rows, list) and len(rows) > max_rows:
            out["rows"] = rows[:max_rows]
            out["rows_shown"] = max_rows
            out["truncated"] = True
        return json.dumps(out, default=str)


# ---------------------------------------------------------------------------
# Tool + ToolRegistry
# ---------------------------------------------------------------------------


ToolHandler = Callable[[dict, ToolContext], ToolResult]


@dataclass
class Tool:
    """Static definition of one LLM-callable tool.

    ``parameters_schema`` is a JSON Schema dict for the *arguments* only —
    no provider envelope. The registry adds whatever envelope the target
    LLM wants (OpenAI vs. MCP) at render time.
    """

    name: str
    description: str
    parameters_schema: dict
    handler: ToolHandler
    # Tags let orchestrators select subsets (e.g. ``query_authoring``,
    # ``view_selection``) without hard-coded name lists.
    tags: tuple[str, ...] = ()
    # If True, calling this tool ends the orchestrator loop. Set on
    # tools whose semantics are "this is the final answer" (the LLM
    # equivalent of ``return``).
    terminal: bool = False


class ToolRegistry:
    """Hold a set of ``Tool``s and render them for an LLM.

    Not thread-safe by itself — orchestrators typically construct one
    per request and discard, so contention is not a concern.
    """

    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    # ------------------------------------------------------------------
    # Registration / lookup
    # ------------------------------------------------------------------

    def register(self, tool: Tool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"Tool {tool.name!r} already registered")
        self._tools[tool.name] = tool

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    def list(self, *, tags: tuple[str, ...] | None = None) -> list[Tool]:
        """Return every tool, optionally filtered to those matching any tag."""
        if not tags:
            return list(self._tools.values())
        tagset = set(tags)
        return [t for t in self._tools.values() if tagset.intersection(t.tags)]

    def names(self) -> list[str]:
        return list(self._tools.keys())

    # ------------------------------------------------------------------
    # Rendering for LLM providers
    # ------------------------------------------------------------------

    def _select(self, names: list[str] | None, tags: tuple[str, ...] | None) -> list[Tool]:
        if names is not None:
            return [self._tools[n] for n in names if n in self._tools]
        return self.list(tags=tags)

    def to_openai_tools(
        self, *, names: list[str] | None = None, tags: tuple[str, ...] | None = None,
    ) -> list[dict]:
        """Render selected tools in OpenAI Chat Completions tool shape."""
        return [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters_schema,
                },
            }
            for t in self._select(names, tags)
        ]

    def to_mcp_definitions(
        self, *, names: list[str] | None = None, tags: tuple[str, ...] | None = None,
    ) -> list[dict]:
        """Render selected tools in MCP server-listing shape.

        Used when we eventually stand up an MCP server (Epic 24 / later)
        to expose the same tools to Claude Desktop, IDE integrations,
        etc. The shape mirrors Anthropic's MCP spec.
        """
        return [
            {
                "name": t.name,
                "description": t.description,
                "inputSchema": t.parameters_schema,
            }
            for t in self._select(names, tags)
        ]

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def dispatch(self, name: str, args: dict, ctx: ToolContext) -> ToolResult:
        """Look up and invoke a tool by name. Raises KeyError if unknown.

        Handlers MUST NOT raise — they should return ``ToolResult(success=False, ...)``.
        The orchestrator catches stray exceptions defensively but tool
        contract is no-raise.
        """
        tool = self._tools.get(name)
        if tool is None:
            raise KeyError(name)
        return tool.handler(args, ctx)


# ---------------------------------------------------------------------------
# Default registry
# ---------------------------------------------------------------------------


def build_default_registry() -> ToolRegistry:
    """Return a registry pre-loaded with the in-process tools we ship.

    Today: ``build_and_run_query`` + ``present_view`` + ``find_artifacts``.
    VG-233/234 will add ``run_saved_query`` and ``run_saved_view`` behind
    this same function.
    """
    # Imported here to avoid a circular import (tools import the registry).
    from semantic.llm.tools.build_and_run_query import BUILD_AND_RUN_QUERY
    from semantic.llm.tools.find_artifacts import FIND_ARTIFACTS
    from semantic.llm.tools.present_view import PRESENT_VIEW

    reg = ToolRegistry()
    reg.register(BUILD_AND_RUN_QUERY)
    reg.register(FIND_ARTIFACTS)
    reg.register(PRESENT_VIEW)
    return reg
