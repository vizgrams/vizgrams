# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""vizgrams router — publish and retrieve platform-level vizgrams."""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import get_current_user, optional_user, require_creator
from core.vizgrams_db import get_user_display_name
from core.caption_provider import (
    build_caption_prompt,
    compute_snapshot_hash,
    get_caption_provider,
)
from core.significance import compute_significance_score
from core.vizgrams_db import (
    add_engagement,
    create_vizgram,
    find_caption_by_hash,
    get_engagement_counts,
    get_viewer_engagement,
    get_vizgram,
    list_feed,
    remove_engagement,
    update_caption,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/vizgrams", tags=["vizgrams"])


@router.get("")
def get_feed(
    limit: int = 20,
    offset: int = 0,
    dataset_ref: str | None = None,
    author_id: str | None = None,
    saved_only: bool = False,
    viewer_id: str | None = Depends(optional_user),
):
    """Return the vizgram feed ranked by freshness × significance × diversity.

    Pass ``saved_only=true`` to return only the viewer's bookmarked vizgrams.
    """
    return list_feed(
        limit=limit,
        offset=offset,
        dataset_ref=dataset_ref,
        author_id=author_id,
        saved_only=saved_only,
        viewer_id=viewer_id,
    )


class VizgramPayload(BaseModel):
    """Shared payload for preview-caption and publish."""
    model: str
    query_ref: str
    title: str
    slice_config: dict = {}
    chart_config: dict = {}
    data_snapshot: list | None = None


class PublishVizgramRequest(VizgramPayload):
    caption: str | None = None


@router.post("/preview-caption")
def preview_caption(
    body: VizgramPayload,
    author_id: str = Depends(require_creator),
):
    """Generate a caption for a vizgram without saving it.

    Checks the caption cache first (same data hash). If no cached caption
    exists, calls the configured LLM provider synchronously and returns the
    draft. The caller can edit the text before confirming publish.
    """
    data_hash = compute_snapshot_hash(body.data_snapshot)

    cached = find_caption_by_hash(data_hash)
    if cached:
        return {"caption": cached, "cached": True}

    provider = get_caption_provider()
    prompt = build_caption_prompt(
        title=body.title,
        query_ref=body.query_ref,
        dataset_ref=body.model,
        chart_type=body.chart_config.get("type", ""),
        columns=body.chart_config.get("columns", []),
        sample_rows=body.data_snapshot or [],
    )
    try:
        caption = provider.generate(prompt=prompt)
    except Exception as exc:
        logger.warning("Caption generation failed: %s", exc)
        return {"caption": None, "cached": False, "error": str(exc)}
    return {"caption": caption, "cached": False}


@router.post("")
def publish_vizgram(
    body: PublishVizgramRequest,
    author_id: str = Depends(require_creator),
):
    """Publish a static vizgram snapshot. Requires Creator role or higher."""
    significance_score = compute_significance_score(body.data_snapshot, body.chart_config)
    vizgram_id = create_vizgram(
        dataset_ref=body.model,
        query_ref=body.query_ref,
        title=body.title,
        author_id=author_id,
        author_display_name=get_user_display_name(author_id),
        slice_config=body.slice_config,
        chart_config=body.chart_config,
        live=False,
        data_snapshot=body.data_snapshot,
        significance_score=significance_score,
    )
    if body.caption:
        data_hash = compute_snapshot_hash(body.data_snapshot)
        update_caption(vizgram_id, body.caption, data_hash)
    return {"id": vizgram_id}


class EngageRequest(BaseModel):
    type: str  # "like" | "save"


@router.post("/{vizgram_id}/engage")
def engage_vizgram(
    vizgram_id: str,
    body: EngageRequest,
    user_id: str = Depends(get_current_user),
):
    """Toggle a like or save on a vizgram. Returns updated counts and viewer state."""
    if body.type not in ("like", "save"):
        raise HTTPException(status_code=422, detail="type must be 'like' or 'save'")

    if not get_vizgram(vizgram_id):
        raise HTTPException(status_code=404, detail="Vizgram not found")

    added = add_engagement(vizgram_id, user_id, body.type)
    if not added:
        remove_engagement(vizgram_id, user_id, body.type)

    counts = get_engagement_counts(vizgram_id)
    viewer = get_viewer_engagement(vizgram_id, user_id)
    return {
        "like_count": counts["like"],
        "save_count": counts["save"],
        "liked": viewer["liked"],
        "saved": viewer["saved"],
    }
