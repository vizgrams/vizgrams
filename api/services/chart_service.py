# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Chart service: collapse query + view YAML into one authorable chart YAML.

Storage is unchanged — a chart still writes two artifacts (a query and a
view sharing the same name). This module is the seam that lets the UI ship
a single-file editor: it splits a chart YAML into the pair on save, and
joins the pair back into a chart YAML on load.

Keeping storage split preserves the legacy "one query, many views" case
(shared queries), the standalone query/view endpoints, and every existing
YAML file on disk — no migration.
"""

from __future__ import annotations

from pathlib import Path

import yaml

# Fields that live on the view side of the split. Anything not in this set
# (plus the shared ``name``) is treated as a query-side field. Keep in sync
# with schemas/view.yaml — a new view-level knob added there without being
# added here would silently leak into the query YAML and fail validation.
_VIEW_FIELDS = frozenset({
    "type",
    "visualization",
    "measure",
    "inputs",
})

# ``description`` conceptually applies to the whole chart, but historically
# lives on the query. Keep it on the query so ``list_queries`` still returns
# a useful description, and so a shared query used by many charts keeps a
# single canonical description rather than one per view.
_SHARED_FIELDS = frozenset({"name"})


def split_chart_yaml(chart_yaml: str, chart_name: str) -> tuple[str, str]:
    """Split a single chart YAML into (query_yaml, view_yaml).

    View gets: name, type, visualization, measure, inputs, plus a
    ``query: <chart_name>`` reference back to the query it wraps.

    Query gets: name + everything else. When a caller inlines a chart
    where the query is meant to be a reference (rare — the UI drills
    through the standalone query endpoint for that case), pass the
    reference name via ``chart_yaml``'s ``query`` field and this function
    will preserve it verbatim, splitting an empty query body.

    Raises ``ValueError`` if the chart YAML doesn't parse to a mapping.
    """
    parsed = yaml.safe_load(chart_yaml) or {}
    if not isinstance(parsed, dict):
        raise ValueError(
            "Chart YAML must be a mapping at the top level; "
            f"got {type(parsed).__name__}."
        )

    view_out: dict = {"name": chart_name}
    query_out: dict = {"name": chart_name}

    for key, value in parsed.items():
        if key in _SHARED_FIELDS:
            continue  # name is set explicitly above
        if key == "query" and isinstance(value, str):
            # Rare: the chart references an external query. Emit the view
            # with the reference and skip writing a query at all — caller
            # can detect empty query body and use the standalone endpoint.
            view_out["query"] = value
            continue
        if key in _VIEW_FIELDS:
            view_out[key] = value
        else:
            query_out[key] = value

    # The view always needs a ``query`` field pointing at the query it wraps
    # unless the caller already set it to an external reference above.
    view_out.setdefault("query", chart_name)

    return (
        yaml.safe_dump(query_out, sort_keys=False),
        yaml.safe_dump(view_out, sort_keys=False),
    )


def join_chart_yaml(query_yaml: str | None, view_yaml: str | None) -> str:
    """Compose a chart YAML from the query + view pair backing it.

    View-side fields (visualization, measure, inputs, type) win over query-
    side ones on the (currently impossible) key clash — visualization is
    the user's mental model of what a chart *is*, so it shouldn't be
    silently overridden by a query field slipping in with the same name.

    The synthetic ``query: <name>`` field the split emits is dropped —
    users don't need to see it in the single-file editor.
    """
    query = yaml.safe_load(query_yaml) if query_yaml else {}
    view = yaml.safe_load(view_yaml) if view_yaml else {}
    if not isinstance(query, dict):
        query = {}
    if not isinstance(view, dict):
        view = {}

    # Query fields first so view fields can win on clashes. The
    # synthetic ``query: <name>`` self-reference from the split is
    # dropped — it's implementation detail, not something the user
    # should see in the composed YAML.
    merged: dict = dict(query)
    merged.update({k: v for k, v in view.items() if k != "query"})

    return yaml.safe_dump(merged, sort_keys=False)


def compose_chart_yaml(model_dir: Path, chart_name: str) -> str:
    """Read the query + view artifacts for a chart and return the composed
    chart YAML. Missing artifacts contribute an empty mapping so partially-
    saved charts (query without view, view without query) still round-trip
    to something editable.
    """
    from core import metadata_db

    query_yaml = metadata_db.get_current_content(model_dir, "query", chart_name)
    view_yaml = metadata_db.get_current_content(model_dir, "view", chart_name)
    return join_chart_yaml(query_yaml, view_yaml)
