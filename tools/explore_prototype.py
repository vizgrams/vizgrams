# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""explore_prototype — VG-200 spike for natural-language → vizgrams query.

Single-turn (or scripted multi-turn) CLI that wires an LLM tool-use loop
to the existing semantic query stack. Throwaway by design: success means
we know the tool schema + system prompt shape needed for the production
`POST /explore/chat` endpoint (VG-202).

Run::

    poetry run python -m tools.explore_prototype ask iagai \\
        "How many PRs were created in April 2026?"

    poetry run python -m tools.explore_prototype ask iagai \\
        "Top 10 PR authors by count" --verbose

Multi-turn (drilldown) — pass `--follow-up` strings; each is sent as a
new user turn with the prior conversation as context::

    poetry run python -m tools.explore_prototype ask iagai \\
        "Top 10 PR authors by count" \\
        --follow-up "Now break that down by team"
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

import click

from core.db import get_backend
from core.model_config import load_database_config
from engine.query_runner import build_aggregate_query, build_detail_query
from semantic.query import (
    PaginationDef,
    QueryAttribute,
    QueryDef,
    QueryMetric,
    SliceDef,
)
from semantic.types import Cardinality, EntityDef
from semantic.yaml_adapter import YAMLAdapter

# Result-row cap fed back to the LLM. Charts only need shape; full data
# would blow the context window on wide queries.
ROWS_TO_LLM = 40

MODEL_NAME = os.environ.get("VZ_EXPLORE_MODEL", "gpt-4o-mini")

# ---------------------------------------------------------------------------
# Model context — entities + features summarised for the system prompt
# ---------------------------------------------------------------------------


def _model_dir(model: str) -> Path:
    base = Path(os.environ.get("VZ_MODELS_DIR", "models"))
    return base / model


def _load_entities(model: str) -> dict[str, EntityDef]:
    """Read entity definitions via the same seam the API uses."""
    md = _model_dir(model)
    # YAMLAdapter only uses .parent of the passed path, so the "ontology"
    # suffix is a no-op for the DB-backed loader.
    return {e.name: e for e in YAMLAdapter.load_entities(md / "ontology")}


def _load_features_by_entity(model: str) -> dict[str, list]:
    md = _model_dir(model)
    result: dict[str, list] = {}
    for fd in YAMLAdapter.load_features(md / "features"):
        result.setdefault(fd.entity_type, []).append(fd)
    return result


def _summarise_entity(e: EntityDef, features: list) -> str:
    """One paragraph per entity — fits in ~150 tokens."""
    lines = [f"ENTITY {e.name}" + (f" — {e.description}" if e.description else "")]
    if e.identity:
        ids = ", ".join(a.name for a in e.identity)
        lines.append(f"  identity: {ids}")
    if e.attributes:
        attrs = ", ".join(f"{a.name}:{a.col_type.value}" for a in e.attributes[:20])
        lines.append(f"  attributes: {attrs}")
    if e.relations:
        rels = []
        for r in e.relations:
            card = "1→N" if r.cardinality == Cardinality.ONE_TO_MANY else "N→1"
            rels.append(f"{r.name} ({card} {r.target})")
        lines.append(f"  relations: {', '.join(rels)}")
    if features:
        feat_names = ", ".join(f.feature_id.split(".")[-1] for f in features[:10])
        lines.append(f"  features: {feat_names}")
    return "\n".join(lines)


def build_system_prompt(model: str) -> str:
    entities = _load_entities(model)
    features = _load_features_by_entity(model)
    entity_blocks = [_summarise_entity(e, features.get(e.name, [])) for e in entities.values()]

    return (
        f"You are a data-exploration assistant for the `{model}` model. The user "
        f"asks questions in natural language; you author queries against the "
        f"semantic layer below, run them with `build_and_run_query`, then call "
        f"`present_result` exactly once to deliver a chart spec + caption.\n\n"
        f"Rules:\n"
        f"- The `root_entity` argument MUST be one of the ENTITY names listed "
        f"below (case-sensitive).\n"
        f"- Field paths in attributes/measures/filters use dotted traversal: "
        f"`author.name`, `repository.product.name`. The first segment must be "
        f"either a column on the root entity or one of its relations.\n"
        f"- For aggregations, populate `group_by` AND `measures`. For raw rows, "
        f"leave `measures` empty and use `attributes` only.\n"
        f"- For `count` rollup, set `field` to the entity's primary key column "
        f"(visible in the `identity:` line). Never use `*` — this layer does "
        f"not understand it.\n"
        f"- `filters` are SQL-ish expression strings: "
        f"`created_at >= '2026-04-01'`, `state == 'merged'`.\n"
        f"- After receiving the query result, decide on a chart type and call "
        f"`present_result`. Use `table` if the result is heterogeneous, `bar` "
        f"for a categorical x-axis + single measure, `line` for time series, "
        f"`kpi` for a single scalar, `scatter` for two measures.\n"
        f"- If `build_and_run_query` returns an error, fix the args and retry. "
        f"Do not retry more than twice for the same conceptual query.\n\n"
        f"=== MODEL SCHEMA ===\n\n" + "\n\n".join(entity_blocks)
    )


# ---------------------------------------------------------------------------
# Tool schemas — JSON Schema fed to the LLM
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "build_and_run_query",
            "description": (
                "Construct a query against the semantic layer and execute it. "
                "Returns up to 40 rows plus column metadata, or an error if the "
                "query fails to validate or execute."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "root_entity": {
                        "type": "string",
                        "description": "Root entity (case-sensitive) — must match an ENTITY name in the schema",
                    },
                    "group_by": {
                        "type": "array",
                        "description": "Slice/group-by fields. Each entry has a field path and optional time-bucket format.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "field": {"type": "string"},
                                "format": {
                                    "type": "string",
                                    "description": "Time bucket: YYYY-MM-DD / YYYY-WW / YYYY-MM / YYYY",
                                },
                                "alias": {"type": "string"},
                            },
                            "required": ["field"],
                        },
                    },
                    "measures": {
                        "type": "array",
                        "description": "Aggregations. Empty for detail queries.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string", "description": "Output column name"},
                                "field": {"type": "string", "description": "Field path being aggregated; use '*' for count(*)"},
                                "rollup": {
                                    "type": "string",
                                    "enum": ["count", "sum", "avg", "min", "max", "count_distinct"],
                                },
                            },
                            "required": ["name", "field", "rollup"],
                        },
                    },
                    "attributes": {
                        "type": "array",
                        "description": "Columns for detail (non-aggregate) queries. Ignored when measures is non-empty.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "field": {"type": "string"},
                                "alias": {"type": "string"},
                            },
                            "required": ["field"],
                        },
                    },
                    "filters": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Expression strings, e.g. \"state == 'merged' && created_at >= '2026-04-01'\"",
                    },
                    "order_by": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string", "description": "Output column name (alias)"},
                                "direction": {"type": "string", "enum": ["ASC", "DESC"]},
                            },
                            "required": ["column", "direction"],
                        },
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
                },
                "required": ["root_entity"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "present_result",
            "description": (
                "Terminal tool — present the chart spec + caption to the user. "
                "Call exactly once after you have a query result you're happy with."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "chart_type": {
                        "type": "string",
                        "enum": ["bar", "line", "table", "scatter", "kpi"],
                    },
                    "x_field": {"type": "string", "description": "Column name for the x-axis (or category)"},
                    "y_field": {"type": "string", "description": "Column name for the y-axis (or value)"},
                    "color_field": {"type": "string", "description": "Optional column for series/colour split"},
                    "caption": {
                        "type": "string",
                        "description": "1-2 sentences highlighting the insight. Be specific about numbers.",
                    },
                },
                "required": ["chart_type", "caption"],
            },
        },
    },
]


# ---------------------------------------------------------------------------
# Query execution — convert tool args → QueryDef → SQL → rows
# ---------------------------------------------------------------------------


def _build_querydef(args: dict) -> QueryDef:
    """Convert flat tool args into the existing QueryDef dataclass."""
    root = args["root_entity"]
    measures_in = args.get("measures") or []
    group_by_in = args.get("group_by") or []

    slices = [
        SliceDef(field=g["field"], alias=g.get("alias"), format_pattern=g.get("format"))
        for g in group_by_in
    ]

    metrics: dict = {}
    for m in measures_in:
        field = m["field"]
        rollup = m["rollup"]
        metrics[m["name"]] = QueryMetric(field=field, rollup=rollup)

    order_by = [(o["column"], o["direction"].upper()) for o in args.get("order_by") or []]

    attrs: list[QueryAttribute] = []
    if not metrics:
        for a in args.get("attributes") or []:
            field = a["field"]
            attrs.append(QueryAttribute(parts=field.split("."), label=a.get("alias")))

    limit = args.get("limit")
    pag = PaginationDef(page=1, page_size=limit) if limit else PaginationDef()

    return QueryDef(
        name="_explore_prototype",
        entity=root,
        detail=not metrics,
        attributes=attrs,
        filters=list(args.get("filters") or []),
        slices=slices,
        metrics=metrics,
        order_by=order_by,
        pagination=pag,
    )


def execute_query(model: str, args: dict) -> dict:
    """Run a query built from tool args. Returns rows + columns, or {error}."""
    model_dir = _model_dir(model)
    try:
        q = _build_querydef(args)
        entities_list = YAMLAdapter.load_entities(model_dir / "ontology")
        entities = {e.name: e for e in entities_list}
        from semantic.types import expand_event_entities
        entities = expand_event_entities(entities)

        features_by_entity = {}
        for fd in YAMLAdapter.load_features(model_dir / "features"):
            features_by_entity.setdefault(fd.entity_type, {})[
                fd.feature_id.split(".")[-1]
            ] = fd

        dialect = load_database_config(model_dir).get("backend", "sqlite")
        if q.is_aggregate:
            sql = build_aggregate_query(
                q, entities, features_by_entity=features_by_entity, dialect=dialect,
            )
        else:
            sql = build_detail_query(
                q, entities, page=1, page_size=args.get("limit") or 100,
                features_by_entity=features_by_entity, dialect=dialect,
            )

        backend = get_backend(model_dir)
        backend.connect()
        rows = list(backend.execute(sql))
        columns = list(backend.last_columns or [])
        truncated = len(rows) > ROWS_TO_LLM
        return {
            "columns": columns,
            "rows": [list(r) for r in rows[:ROWS_TO_LLM]],
            "row_count": len(rows),
            "truncated": truncated,
            "sql": sql,
        }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# LLM tool-use loop
# ---------------------------------------------------------------------------


def _openai_client():
    try:
        from openai import OpenAI
    except ImportError as e:
        click.echo(f"openai SDK not installed: {e}", err=True)
        sys.exit(2)
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        click.echo("OPENAI_API_KEY not set in environment.", err=True)
        sys.exit(2)
    return OpenAI(api_key=key)


def run_turn(
    client, model: str, messages: list[dict], *, max_iter: int = 5, verbose: bool,
) -> dict | None:
    """Run the tool-use loop for one user turn. Returns the present_result args."""
    for iter_idx in range(max_iter):
        if verbose:
            click.echo(f"\n[iter {iter_idx + 1}] → LLM", err=True)
        resp = client.chat.completions.create(
            model=MODEL_NAME, messages=messages, tools=TOOLS, tool_choice="auto",
        )
        msg = resp.choices[0].message
        # Append the assistant message verbatim (preserve tool_calls structure)
        messages.append(
            {
                "role": "assistant",
                "content": msg.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                    }
                    for tc in (msg.tool_calls or [])
                ]
                or None,
            }
        )
        if msg.content and verbose:
            click.echo(f"  assistant: {msg.content[:300]}", err=True)
        if not msg.tool_calls:
            click.echo("LLM produced no tool calls and no terminal result.", err=True)
            return None
        for tc in msg.tool_calls:
            name = tc.function.name
            args = json.loads(tc.function.arguments or "{}")
            if verbose:
                click.echo(f"  tool_call: {name}({json.dumps(args)[:300]})", err=True)
            if name == "present_result":
                # Acknowledge the terminal tool so the assistant message has a
                # paired tool response — required if the conversation continues
                # with a follow-up user turn.
                messages.append(
                    {"role": "tool", "tool_call_id": tc.id,
                     "content": json.dumps({"presented": True})}
                )
                return args
            if name == "build_and_run_query":
                result = execute_query(model, args)
                if verbose:
                    if "error" in result:
                        click.echo(f"  tool_result: ERROR — {result['error']}", err=True)
                    else:
                        click.echo(
                            f"  tool_result: {result['row_count']} rows, "
                            f"cols={result['columns']}", err=True,
                        )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": json.dumps(result, default=str),
                    }
                )
            else:
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": json.dumps({"error": f"unknown tool {name!r}"}),
                    }
                )
    click.echo(f"Max iterations ({max_iter}) reached without present_result.", err=True)
    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group()
def cli() -> None:
    """Prototype for natural-language → vizgrams query (VG-200 spike)."""


@cli.command()
@click.argument("model")
@click.argument("question")
@click.option("--follow-up", "follow_ups", multiple=True, help="Additional user turns (drilldown)")
@click.option("--max-iter", default=5, show_default=True, help="Max tool-use iterations per turn")
@click.option("--verbose/--quiet", default=True)
def ask(model: str, question: str, follow_ups: tuple[str, ...], max_iter: int, verbose: bool) -> None:
    """Ask QUESTION of MODEL, optionally followed by --follow-up turns."""
    client = _openai_client()
    sys_prompt = build_system_prompt(model)
    if verbose:
        click.echo(f"System prompt: {len(sys_prompt)} chars", err=True)
    messages: list[dict] = [
        {"role": "system", "content": sys_prompt},
    ]
    for turn_idx, user_msg in enumerate([question, *follow_ups]):
        click.echo(f"\n══ TURN {turn_idx + 1}: {user_msg}", err=True)
        messages.append({"role": "user", "content": user_msg})
        result = run_turn(client, model, messages, max_iter=max_iter, verbose=verbose)
        if result is None:
            click.echo("(no result)", err=True)
            continue
        click.echo("")
        click.echo(json.dumps(result, indent=2))


if __name__ == "__main__":
    cli()
