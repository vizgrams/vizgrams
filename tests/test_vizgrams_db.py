# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for core/vizgrams_db.py."""

import pytest
from pathlib import Path

from core.vizgrams_db import (
    create_vizgram,
    get_vizgram,
    list_feed,
    update_caption,
    update_data,
    delete_vizgram,
    add_engagement,
    remove_engagement,
    get_engagement_counts,
)


@pytest.fixture
def db(tmp_path):
    return tmp_path / "test_vizgrams.db"


# ---------------------------------------------------------------------------
# create / get
# ---------------------------------------------------------------------------

def test_create_and_get_vizgram(db):
    vid = create_vizgram(
        dataset_ref="acme",
        query_ref="dora_clt_by_team",
        title="DORA CLT by Team",
        author_id="oliver@example.com",
        db_path=db,
    )
    assert vid

    vg = get_vizgram(vid, db_path=db)
    assert vg["id"] == vid
    assert vg["dataset_ref"] == "acme"
    assert vg["query_ref"] == "dora_clt_by_team"
    assert vg["title"] == "DORA CLT by Team"
    assert vg["author_id"] == "oliver@example.com"
    assert vg["live"] is True
    assert vg["is_deleted"] is False
    assert vg["slice_config"] == {}
    assert vg["chart_config"] == {}
    assert vg["tags"] == []
    assert vg["caption"] is None
    assert vg["significance_score"] == 0.0


def test_create_with_all_fields(db):
    vid = create_vizgram(
        dataset_ref="openflights",
        query_ref="routes_by_airline",
        title="Top Airlines by Route Count",
        author_id="alice@example.com",
        slice_config={"parameters": {"region": "EU"}},
        chart_config={"chart_type": "bar", "x_key": "airline", "y_keys": ["route_count"]},
        tags=["aviation", "routes"],
        live=False,
        data_snapshot=[{"airline": "BA", "route_count": 200}],
        db_path=db,
    )
    vg = get_vizgram(vid, db_path=db)
    assert vg["slice_config"] == {"parameters": {"region": "EU"}}
    assert vg["chart_config"]["chart_type"] == "bar"
    assert vg["tags"] == ["aviation", "routes"]
    assert vg["live"] is False
    assert vg["data_snapshot"] == [{"airline": "BA", "route_count": 200}]


def test_get_nonexistent_returns_none(db):
    assert get_vizgram("no-such-id", db_path=db) is None


# ---------------------------------------------------------------------------
# list_feed
# ---------------------------------------------------------------------------

def test_list_feed_returns_all(db):
    for i in range(3):
        create_vizgram(
            dataset_ref="acme", query_ref=f"q{i}", title=f"Title {i}",
            author_id="oliver@example.com", db_path=db,
        )
    feed = list_feed(db_path=db)
    assert len(feed) == 3


def test_list_feed_excludes_deleted(db):
    vid = create_vizgram(
        dataset_ref="acme", query_ref="q1", title="T1",
        author_id="a@b.com", db_path=db,
    )
    create_vizgram(
        dataset_ref="acme", query_ref="q2", title="T2",
        author_id="a@b.com", db_path=db,
    )
    delete_vizgram(vid, db_path=db)
    feed = list_feed(db_path=db)
    assert len(feed) == 1
    assert feed[0]["query_ref"] == "q2"


def test_list_feed_filter_by_dataset(db):
    create_vizgram(dataset_ref="acme", query_ref="q1", title="A", author_id="x@x.com", db_path=db)
    create_vizgram(dataset_ref="openflights", query_ref="q2", title="B", author_id="x@x.com", db_path=db)
    feed = list_feed(dataset_ref="acme", db_path=db)
    assert len(feed) == 1
    assert feed[0]["dataset_ref"] == "acme"


def test_list_feed_filter_by_author(db):
    create_vizgram(dataset_ref="d", query_ref="q1", title="A", author_id="alice@x.com", db_path=db)
    create_vizgram(dataset_ref="d", query_ref="q2", title="B", author_id="bob@x.com", db_path=db)
    feed = list_feed(author_id="alice@x.com", db_path=db)
    assert len(feed) == 1
    assert feed[0]["author_id"] == "alice@x.com"


def test_list_feed_ordered_by_significance_then_published(db):
    v1 = create_vizgram(dataset_ref="d", query_ref="q1", title="Low", author_id="a@b.com", db_path=db)
    v2 = create_vizgram(dataset_ref="d", query_ref="q2", title="High", author_id="a@b.com", db_path=db)
    update_data(v2, data_hash="h", last_data_updated="2026-01-01", significance_score=0.9, db_path=db)
    update_data(v1, data_hash="h", last_data_updated="2026-01-01", significance_score=0.1, db_path=db)
    feed = list_feed(db_path=db)
    assert feed[0]["id"] == v2
    assert feed[1]["id"] == v1


def test_list_feed_pagination(db):
    for i in range(5):
        create_vizgram(dataset_ref="d", query_ref=f"q{i}", title=f"T{i}", author_id="a@b.com", db_path=db)
    page1 = list_feed(limit=3, offset=0, db_path=db)
    page2 = list_feed(limit=3, offset=3, db_path=db)
    assert len(page1) == 3
    assert len(page2) == 2
    ids1 = {v["id"] for v in page1}
    ids2 = {v["id"] for v in page2}
    assert ids1.isdisjoint(ids2)


# ---------------------------------------------------------------------------
# update_caption
# ---------------------------------------------------------------------------

def test_update_caption(db):
    vid = create_vizgram(dataset_ref="d", query_ref="q", title="T", author_id="a@b.com", db_path=db)
    update_caption(vid, caption="This shows CLT trends.", caption_hash="abc123", db_path=db)
    vg = get_vizgram(vid, db_path=db)
    assert vg["caption"] == "This shows CLT trends."
    assert vg["caption_hash"] == "abc123"


# ---------------------------------------------------------------------------
# update_data
# ---------------------------------------------------------------------------

def test_update_data(db):
    vid = create_vizgram(dataset_ref="d", query_ref="q", title="T", author_id="a@b.com", db_path=db)
    update_data(
        vid, data_hash="deadbeef", last_data_updated="2026-04-20T00:00:00+00:00",
        significance_score=0.75, data_snapshot=[{"x": 1}], db_path=db,
    )
    vg = get_vizgram(vid, db_path=db)
    assert vg["data_hash"] == "deadbeef"
    assert vg["significance_score"] == 0.75
    assert vg["data_snapshot"] == [{"x": 1}]


# ---------------------------------------------------------------------------
# soft delete
# ---------------------------------------------------------------------------

def test_delete_vizgram(db):
    vid = create_vizgram(dataset_ref="d", query_ref="q", title="T", author_id="a@b.com", db_path=db)
    delete_vizgram(vid, db_path=db)
    assert get_vizgram(vid, db_path=db) is None


# ---------------------------------------------------------------------------
# engagements
# ---------------------------------------------------------------------------

def test_add_and_remove_like(db):
    vid = create_vizgram(dataset_ref="d", query_ref="q", title="T", author_id="a@b.com", db_path=db)
    result = add_engagement(vid, "user1@x.com", "like", db_path=db)
    assert result is True
    counts = get_engagement_counts(vid, db_path=db)
    assert counts["like"] == 1
    assert counts["save"] == 0

    remove_engagement(vid, "user1@x.com", "like", db_path=db)
    counts = get_engagement_counts(vid, db_path=db)
    assert counts["like"] == 0


def test_duplicate_engagement_returns_false(db):
    vid = create_vizgram(dataset_ref="d", query_ref="q", title="T", author_id="a@b.com", db_path=db)
    assert add_engagement(vid, "user1@x.com", "like", db_path=db) is True
    assert add_engagement(vid, "user1@x.com", "like", db_path=db) is False
    counts = get_engagement_counts(vid, db_path=db)
    assert counts["like"] == 1


def test_like_and_save_are_independent(db):
    vid = create_vizgram(dataset_ref="d", query_ref="q", title="T", author_id="a@b.com", db_path=db)
    add_engagement(vid, "user1@x.com", "like", db_path=db)
    add_engagement(vid, "user1@x.com", "save", db_path=db)
    counts = get_engagement_counts(vid, db_path=db)
    assert counts["like"] == 1
    assert counts["save"] == 1


def test_multiple_users_can_like(db):
    vid = create_vizgram(dataset_ref="d", query_ref="q", title="T", author_id="a@b.com", db_path=db)
    for i in range(5):
        add_engagement(vid, f"user{i}@x.com", "like", db_path=db)
    counts = get_engagement_counts(vid, db_path=db)
    assert counts["like"] == 5


def test_invalid_engagement_type_raises(db):
    vid = create_vizgram(dataset_ref="d", query_ref="q", title="T", author_id="a@b.com", db_path=db)
    with pytest.raises(ValueError, match="Invalid engagement type"):
        add_engagement(vid, "user@x.com", "clap", db_path=db)


def test_engagement_counts_empty(db):
    vid = create_vizgram(dataset_ref="d", query_ref="q", title="T", author_id="a@b.com", db_path=db)
    counts = get_engagement_counts(vid, db_path=db)
    assert counts == {"like": 0, "save": 0}
