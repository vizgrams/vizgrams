# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for core/ranking.py"""

from datetime import UTC, datetime, timedelta

import pytest

from core.ranking import DIVERSITY_PENALTY, FRESHNESS_HALFLIFE_HOURS, rank_feed


def _item(id: str, significance: float, age_hours: float, dataset: str = "ds") -> dict:
    published_at = (datetime.now(UTC) - timedelta(hours=age_hours)).isoformat()
    return {
        "id": id,
        "significance_score": significance,
        "published_at": published_at,
        "dataset_ref": dataset,
    }


# ---------------------------------------------------------------------------
# Empty / trivial
# ---------------------------------------------------------------------------


def test_empty():
    assert rank_feed([], limit=10) == []


def test_fewer_items_than_limit():
    items = [_item("a", 5.0, 0), _item("b", 3.0, 0)]
    result = rank_feed(items, limit=10)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# Freshness: a fresh low-significance item can outrank a stale high one
# ---------------------------------------------------------------------------


def test_fresh_beats_stale():
    stale_high = _item("stale", significance=10.0, age_hours=FRESHNESS_HALFLIFE_HOURS * 10)
    fresh_low = _item("fresh", significance=2.0, age_hours=0)
    result = rank_feed([stale_high, fresh_low], limit=2)
    # stale_high base ≈ 10/(1+10) ≈ 0.91; fresh_low base = 2/(1+0) = 2.0
    assert result[0]["id"] == "fresh"


def test_high_significance_beats_fresh_with_small_age():
    # Both fresh — significance should win
    high = _item("high", significance=9.0, age_hours=1)
    low = _item("low", significance=2.0, age_hours=0)
    result = rank_feed([high, low], limit=2)
    assert result[0]["id"] == "high"


# ---------------------------------------------------------------------------
# Diversity: same dataset gets penalised after first pick
# ---------------------------------------------------------------------------


def test_diversity_interleaves_datasets():
    # Three items from "ds_a", one from "ds_b" — ds_b should appear early
    items = [
        _item("a1", 9.0, 0, dataset="ds_a"),
        _item("a2", 8.0, 0, dataset="ds_a"),
        _item("a3", 7.0, 0, dataset="ds_a"),
        _item("b1", 5.0, 0, dataset="ds_b"),
    ]
    result = rank_feed(items, limit=4)
    ids = [r["id"] for r in result]
    # a1 should be first (highest base score)
    assert ids[0] == "a1"
    # b1 should appear before a3 due to diversity penalty on ds_a
    assert ids.index("b1") < ids.index("a3")


def test_diversity_does_not_suppress_entirely():
    # Even a lower-significance item from a dominant dataset should appear
    items = [_item(f"a{i}", 8.0 - i, 0, dataset="ds_a") for i in range(5)]
    items.append(_item("b1", 1.0, 0, dataset="ds_b"))
    result = rank_feed(items, limit=6)
    ids = [r["id"] for r in result]
    assert "b1" in ids


# ---------------------------------------------------------------------------
# Pagination: offset is applied after ranking (stable across pages)
# ---------------------------------------------------------------------------


def test_pagination_stable():
    items = [_item(str(i), float(10 - i), 0) for i in range(10)]
    page1 = rank_feed(items, limit=5, offset=0)
    page2 = rank_feed(items, limit=5, offset=5)
    all_ids = [r["id"] for r in page1 + page2]
    # All 10 items should appear exactly once across two pages
    assert len(all_ids) == 10
    assert len(set(all_ids)) == 10


def test_offset_beyond_results():
    items = [_item("a", 5.0, 0)]
    assert rank_feed(items, limit=10, offset=5) == []


# ---------------------------------------------------------------------------
# user_context parameter accepted (no-op for now)
# ---------------------------------------------------------------------------


def test_user_context_accepted():
    items = [_item("a", 5.0, 0), _item("b", 3.0, 0)]
    result = rank_feed(items, limit=2, user_context={"user_id": "alice"})
    assert len(result) == 2


# ---------------------------------------------------------------------------
# Integration: list_feed uses ranking
# ---------------------------------------------------------------------------


def test_list_feed_uses_ranking(tmp_path):
    from core.vizgrams_db import create_vizgram, list_feed

    db = tmp_path / "vg.db"
    # Create a stale high-significance item and a fresh low-significance item
    stale_id = create_vizgram(
        dataset_ref="ds", query_ref="q", title="Stale",
        author_id="alice", significance_score=10.0,
        db_path=db,
    )
    # Manually backdate published_at so it appears old
    import sqlite3
    old_ts = (datetime.now(UTC) - timedelta(hours=FRESHNESS_HALFLIFE_HOURS * 10)).isoformat()
    conn = sqlite3.connect(str(db))
    conn.execute("UPDATE vizgrams SET published_at=?, updated_at=? WHERE id=?",
                 (old_ts, old_ts, stale_id))
    conn.commit()
    conn.close()

    fresh_id = create_vizgram(
        dataset_ref="ds2", query_ref="q", title="Fresh",
        author_id="alice", significance_score=2.0,
        db_path=db,
    )

    results = list_feed(limit=2, db_path=db)
    assert results[0]["id"] == fresh_id  # fresh wins despite lower significance
