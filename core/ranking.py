# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Feed ranking algorithm v1 — freshness × significance × diversity.

Public API
----------
rank_feed(items, *, limit, offset, user_context=None) -> list[dict]

Algorithm
---------
1. Base score
   score = significance_score / (1 + age_hours / FRESHNESS_HALFLIFE_HOURS)

   significance_score  in [0, 10] — computed at publish time (see core/significance.py)
   age_hours           hours elapsed since published_at
   FRESHNESS_HALFLIFE  72 h — a vizgram at its half-life scores 50 % of its significance

2. Diversity re-ranking
   Items are selected greedily.  Each time a dataset is picked, the effective
   score for remaining items from that dataset is multiplied by DIVERSITY_PENALTY
   (default 0.5).  This prevents a single prolific dataset from dominating the
   top of the feed without fully suppressing it.

3. Pagination
   Ranking always operates on the full candidate pool so that page offsets are
   stable — page 2 always returns ranks 21-40 of the same ordering regardless
   of how many pages were fetched before it.

Extending for personalisation (VG-030+)
----------------------------------------
Pass ``user_context`` — a dict that may contain:
    liked_datasets   : set[str]   — datasets the user has engaged with
    liked_authors    : set[str]   — authors the user follows
    engagement_boost : float      — multiplier for preferred content (default 1.5)

These are not yet wired; the hook is here so the calling code does not need to
change when personalisation is added.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime


# ---------------------------------------------------------------------------
# Tuning constants — override via env vars to adjust feed feel
# ---------------------------------------------------------------------------

FRESHNESS_HALFLIFE_HOURS: float = float(
    os.environ.get("VZ_FEED_FRESHNESS_HALFLIFE_HOURS", "72")
)
DIVERSITY_PENALTY: float = float(
    os.environ.get("VZ_FEED_DIVERSITY_PENALTY", "0.5")
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def rank_feed(
    items: list[dict],
    *,
    limit: int,
    offset: int = 0,
    user_context: dict | None = None,
) -> list[dict]:
    """Return *limit* vizgrams starting at *offset*, ranked by the feed algorithm.

    Args:
        items:        Full candidate pool — all eligible vizgrams for this feed
                      (already filtered by dataset_ref / author_id if applicable).
        limit:        Number of items to return.
        offset:       Number of top-ranked items to skip (for pagination).
        user_context: Reserved for personalisation (currently ignored).

    Returns:
        Ranked and diversity-adjusted list of vizgrams, length ≤ limit.
    """
    if not items:
        return []

    now = datetime.now(UTC)
    scored = [(_base_score(item, now), item) for item in items]

    ranked = _diversity_rank(scored)

    return ranked[offset: offset + limit]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _base_score(item: dict, now: datetime) -> float:
    """Freshness-weighted significance score."""
    significance = float(item.get("significance_score") or 0.0)

    published_at_str = item.get("published_at") or ""
    age_hours = _age_hours(published_at_str, now)

    # Wilson-style time decay: halves every FRESHNESS_HALFLIFE_HOURS
    decay = 1.0 / (1.0 + age_hours / FRESHNESS_HALFLIFE_HOURS)
    return significance * decay


def _age_hours(published_at: str, now: datetime) -> float:
    """Return hours elapsed since published_at (ISO string). Returns 0 on parse error."""
    if not published_at:
        return 0.0
    try:
        # Handle both offset-aware and naive ISO strings
        dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        delta = now - dt
        return max(delta.total_seconds() / 3600.0, 0.0)
    except (ValueError, OverflowError):
        return 0.0


def _diversity_rank(scored: list[tuple[float, dict]]) -> list[dict]:
    """Greedy diversity-aware selection.

    Iterates until all candidates are placed.  On each step the candidate with
    the highest *effective* score is selected; its dataset's count is incremented
    so subsequent items from the same dataset are penalised.

    Time complexity: O(n²) — acceptable for feed sizes up to a few thousand.
    """
    dataset_picks: dict[str, int] = {}
    remaining = list(scored)          # list of (base_score, item)
    result: list[dict] = []

    while remaining:
        best_idx = max(
            range(len(remaining)),
            key=lambda i: _effective_score(remaining[i][0], remaining[i][1], dataset_picks),
        )
        base, item = remaining.pop(best_idx)
        result.append(item)
        ds = item.get("dataset_ref", "")
        dataset_picks[ds] = dataset_picks.get(ds, 0) + 1

    return result


def _effective_score(base: float, item: dict, dataset_picks: dict[str, int]) -> float:
    n = dataset_picks.get(item.get("dataset_ref", ""), 0)
    return base * (DIVERSITY_PENALTY ** n)
