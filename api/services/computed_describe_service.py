# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""LLM helper: natural-language description → ``{name, expression}`` for a
computed feature (Epic 26 VG-293).

The Schema tab's "Describe it" affordance posts a one-sentence brief
(e.g. *"lead time in hours from created to merged"*) and gets back a
snake_case name + expression DSL string. Creators can author without
knowing the DSL; admins can iterate faster on the bits they do know.

Uses the same provider seam as the chat orchestrator
(``get_default_client``) so configuration / cost telemetry stays
unified."""

from __future__ import annotations

import logging
from typing import Any

from semantic.llm.provider import LLMClient, get_default_client

logger = logging.getLogger(__name__)

# Tool schema for the LLM's structured response. Single mandatory call —
# we don't want prose, just the two fields we'll plug into the feature
# YAML.
_TOOL_SCHEMA: dict = {
    "type": "function",
    "function": {
        "name": "make_computed",
        "description": "Emit a computed feature definition.",
        "parameters": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": (
                        "snake_case identifier (e.g. lead_time_hours)."
                    ),
                },
                "expr": {
                    "type": "string",
                    "description": (
                        "vizgrams expression DSL string. Reference attributes by "
                        "their column name. Use registered functions when needed: "
                        "datetime_diff, format_time, format_date, concat, coalesce, "
                        "count, sum, avg, min, max, json_has_key."
                    ),
                },
            },
            "required": ["name", "expr"],
        },
    },
}


def _build_system_prompt(entity_name: str, entity_schema: dict) -> str:
    """Compose the system prompt with the entity's available attributes +
    relations so the model can reference the right columns."""
    attrs = entity_schema.get("attributes") or []
    rels = entity_schema.get("relations") or []
    attr_lines = "\n".join(f"- {a['name']}: {a['type']}" for a in attrs) or "- (none)"
    rel_lines = "\n".join(
        f"- {r.get('name') or r['target']} → {r['target']} ({r['cardinality']})"
        for r in rels
    ) or "- (none)"
    return (
        f"You convert natural-language descriptions of computed values into "
        f"vizgrams expression DSL definitions for the {entity_name} entity.\n\n"
        f"Available attributes:\n{attr_lines}\n\n"
        f"Available relations:\n{rel_lines}\n\n"
        f"Always respond by calling the `make_computed` tool — no prose. "
        f"Pick a short snake_case name (≤ 30 chars). Prefer simple "
        f"expressions over clever ones."
    )


def describe_computed(
    *,
    entity_name: str,
    entity_schema: dict,
    description: str,
    llm_client: LLMClient | None = None,
    model: str | None = None,
) -> dict:
    """Round-trip the LLM and return ``{name, expr}``.

    Raises ``ValueError`` if the model returns prose instead of a tool
    call (provider-side failure mode) — caller surfaces this as a user-
    visible error so the user can retry or fall back to hand-authoring.
    """
    if not description.strip():
        raise ValueError("description is required")
    client = llm_client or get_default_client()
    system = _build_system_prompt(entity_name, entity_schema)
    response = client.complete(
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": description.strip()},
        ],
        tools=[_TOOL_SCHEMA],
        model=model,
        max_tokens=400,
        temperature=0.0,
    )
    if not response.tool_calls:
        logger.warning("describe_computed: LLM returned no tool call")
        raise ValueError(
            "Model returned prose instead of a structured response. Try "
            "rephrasing your description, or author the expression by hand.",
        )
    args: Any = response.tool_calls[0].arguments
    if not isinstance(args, dict) or "name" not in args or "expr" not in args:
        raise ValueError("Model returned malformed structured response.")
    return {"name": str(args["name"]), "expr": str(args["expr"])}
