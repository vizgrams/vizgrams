# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Chat publish service (Epic 21 VG-240 + VG-241).

Turns a chat turn's view payload into:
  - (if needed) a saved query artifact
  - (if needed) a saved view artifact
  - a published vizgram in the feed

Mirrors the existing /views publish UX: same vizgram shape, same caption
flow. The extra step here is *creating* the underlying artifacts first,
since chat turns produce inline YAML rather than referencing saved names.

The three response shapes from ``api/services/explore_chat.py``:

  Path A — saved_view ref (find_artifacts hit on a saved view)
    Nothing new to save. Just execute the view, snapshot, publish.

  Path B — inline_view wrapping a saved query
    Save the wrapper view as an artifact. The query is already saved.

  Path C — inline_view + inline_query
    Save the query, rewrite the view's ``query:`` ref to match, save
    the view, then publish.

In all paths the saved artifacts inherit ``created_via='chat'`` and are
uncertified — the library filter chip (VG-260) hides them from the
default catalog so they don't pollute the namespace.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml as _yaml

from api.services import (
    application_service,  # noqa: F401  (avoids circular import via tests)
    query_service,
    view_service,
)
from core import metadata_db
from core.caption_provider import compute_snapshot_hash
from core.significance import compute_significance_score
from core.vizgrams_db import create_vizgram, get_user_display_name, update_caption

logger = logging.getLogger(__name__)

# Saved artifact names must match `[a-z][a-z0-9_]*` per the backend slug
# rules. Chat-published artifacts hit those routes, so the title slug must
# satisfy them too. Cap length to keep DB rows + URLs readable.
_NAME_MAX_LEN = 80


def slugify_title(title: str) -> str:
    """Convert a free-text title to a backend-valid artifact name.

    Lowercases, collapses runs of non-alphanumeric chars to ``_``, strips
    leading/trailing underscores, and prepends ``chat_`` if the result
    doesn't start with a letter. Empty-after-cleanup titles fall back to
    ``chat_untitled``.
    """
    s = title.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    if not s:
        return "chat_untitled"
    if not s[0].isalpha():
        s = "chat_" + s
    return s[:_NAME_MAX_LEN]


def unique_name(model_dir: Path, artifact_type: str, base: str) -> str:
    """Pick the first ``base``, ``base_v2``, ``base_v3``, ... not taken.

    Existence is checked against ``artifact_versions`` (the source of
    truth — a name is "taken" if any current version exists for that
    (model, type, name)).
    """
    existing = set(metadata_db.list_artifact_names(model_dir, artifact_type))
    if base not in existing:
        return base
    n = 2
    while f"{base}_v{n}" in existing:
        n += 1
    return f"{base}_v{n}"


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def _rewrite_query_name(yaml_str: str, new_name: str) -> str:
    """Set the ``name:`` field of a query YAML to ``new_name``."""
    data = _yaml.safe_load(yaml_str) or {}
    data["name"] = new_name
    return _yaml.dump(data, sort_keys=False, default_flow_style=False)


def _rewrite_view_yaml(yaml_str: str, *, view_name: str, query_name: str) -> str:
    """Set the ``name:`` and ``query:`` fields of a view YAML."""
    data = _yaml.safe_load(yaml_str) or {}
    data["name"] = view_name
    data["query"] = query_name
    return _yaml.dump(data, sort_keys=False, default_flow_style=False)


def _resolve_artifacts(
    model_dir: Path,
    *,
    title_slug: str,
    saved_view: dict | None,
    inline_view: dict | None,
    user_id: str,
) -> tuple[str, str | None]:
    """Save any artifacts that need to exist, return ``(view_name, query_name?)``.

    ``query_name`` is None for path A (the saved view's query stays under
    its existing name; we don't need to surface it to the caller).
    """
    if saved_view is not None:
        # Path A — view already saved; nothing to write.
        return saved_view["name"], None

    if inline_view is None:
        raise ValueError("Chat publish requires either saved_view or inline_view.")

    view_yaml = inline_view.get("view_yaml")
    query_yaml = inline_view.get("query_yaml")
    if not view_yaml:
        raise ValueError("inline_view must have view_yaml.")

    if query_yaml:
        # Path C — save query, then view that references it.
        query_name = unique_name(model_dir, "query", title_slug)
        view_name = unique_name(model_dir, "view", title_slug)
        rewritten_query = _rewrite_query_name(query_yaml, query_name)
        query_service.create_or_replace_query(
            model_dir, query_name, rewritten_query,
            user_id=user_id, via="chat",
        )
        rewritten_view = _rewrite_view_yaml(
            view_yaml, view_name=view_name, query_name=query_name,
        )
        view_service.create_or_replace_view(
            model_dir, view_name, rewritten_view,
            user_id=user_id, via="chat",
        )
        return view_name, query_name

    # Path B — query already saved (view_yaml's ``query:`` field names it).
    # Just save the wrapper view; preserve its existing ``query:`` ref.
    parsed = _yaml.safe_load(view_yaml) or {}
    existing_query_ref = parsed.get("query")
    if not existing_query_ref:
        raise ValueError("inline_view (path B) must reference a saved query.")
    view_name = unique_name(model_dir, "view", title_slug)
    parsed["name"] = view_name
    rewritten_view = _yaml.dump(parsed, sort_keys=False, default_flow_style=False)
    view_service.create_or_replace_view(
        model_dir, view_name, rewritten_view,
        user_id=user_id, via="chat",
    )
    return view_name, None


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def publish_from_chat(
    model_dir: Path,
    *,
    model_id: str,
    title: str,
    caption: str | None,
    saved_view: dict | None,
    inline_view: dict | None,
    params: dict | None,
    user_id: str,
) -> dict:
    """Save any needed artifacts, execute the view, publish the vizgram.

    Returns ``{vizgram_id, view_name, query_name?}`` so the UI can show
    the "view live data" link.
    """
    if not title or not title.strip():
        raise ValueError("Title is required.")

    title_slug = slugify_title(title)
    view_name, query_name = _resolve_artifacts(
        model_dir,
        title_slug=title_slug,
        saved_view=saved_view,
        inline_view=inline_view,
        user_id=user_id,
    )

    # Execute the (now-saved) view to capture the snapshot the feed shows.
    # Path A may have a longer limit on map views; everything else is fine
    # at the default 1000. We mirror ViewsPage's MAX_ROWS cap on the
    # snapshot (50 for tables, 500 for charts) so the wire payload stays
    # small while keeping the chart legible.
    result = view_service.execute_view(
        model_dir, view_name, limit=1000, offset=0, params=params,
    )
    max_rows = 50 if result.get("type") == "table" else 500
    snapshot_rows = (result.get("rows") or [])[:max_rows]

    chart_config = {
        "type": result.get("type"),
        "visualization": result.get("visualization", {}),
        "columns": result.get("columns", []),
    }
    slice_config = {
        "parameters": params or {},
        # Recorded so the UI can show "snapshot as of …" later.
        "snapshot_at": result.get("snapshot_at") or "",
    }

    significance = compute_significance_score(snapshot_rows, chart_config)
    vizgram_id = create_vizgram(
        dataset_ref=model_id,
        query_ref=result.get("query") or view_name,  # underlying query name
        title=title.strip(),
        author_id=user_id,
        author_display_name=get_user_display_name(user_id),
        slice_config=slice_config,
        chart_config=chart_config,
        live=False,
        data_snapshot=snapshot_rows,
        significance_score=significance,
    )
    if caption:
        data_hash = compute_snapshot_hash(snapshot_rows)
        update_caption(vizgram_id, caption, data_hash)

    return {
        "vizgram_id": vizgram_id,
        "view_name": view_name,
        "query_name": query_name,
    }
