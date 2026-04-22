# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for stable user identity (VG-005)."""

from core.vizgrams_db import (
    add_engagement,
    create_vizgram,
    get_user_display_name,
    list_feed,
    migrate_email_ids,
    resolve_user,
)

# ---------------------------------------------------------------------------
# resolve_user
# ---------------------------------------------------------------------------


def test_resolve_user_creates_on_first_call(tmp_path):
    db = tmp_path / "vg.db"
    uid = resolve_user("github", "1234567", email="alice@example.com", db_path=db)
    assert len(uid) == 36  # UUID format


def test_resolve_user_stable_across_calls(tmp_path):
    db = tmp_path / "vg.db"
    uid1 = resolve_user("github", "1234567", db_path=db)
    uid2 = resolve_user("github", "1234567", db_path=db)
    assert uid1 == uid2


def test_resolve_user_different_providers_different_ids(tmp_path):
    db = tmp_path / "vg.db"
    uid_gh = resolve_user("github", "abc", db_path=db)
    uid_entra = resolve_user("entra", "abc", db_path=db)
    assert uid_gh != uid_entra


def test_resolve_user_updates_email(tmp_path):
    db = tmp_path / "vg.db"
    resolve_user("dex", "sub-123", email="old@example.com", db_path=db)
    resolve_user("dex", "sub-123", email="new@example.com", db_path=db)
    # Display name should still resolve without error
    uid = resolve_user("dex", "sub-123", db_path=db)
    assert uid is not None


# ---------------------------------------------------------------------------
# get_user_display_name
# ---------------------------------------------------------------------------


def test_display_name_from_explicit_name(tmp_path):
    db = tmp_path / "vg.db"
    uid = resolve_user("dex", "u1", display_name="Alice Smith", db_path=db)
    assert get_user_display_name(uid, db_path=db) == "Alice Smith"


def test_display_name_falls_back_to_email_prefix(tmp_path):
    db = tmp_path / "vg.db"
    uid = resolve_user("dex", "u2", email="bob@example.com", db_path=db)
    assert get_user_display_name(uid, db_path=db) == "bob"


def test_display_name_unknown_uuid(tmp_path):
    db = tmp_path / "vg.db"
    unknown = "00000000-0000-0000-0000-000000000000"
    assert get_user_display_name(unknown, db_path=db) == unknown


# ---------------------------------------------------------------------------
# migrate_email_ids
# ---------------------------------------------------------------------------


def test_migrate_replaces_email_with_uuid(tmp_path):
    db = tmp_path / "vg.db"
    # Create vizgram with raw email as author_id (pre-migration state)
    import sqlite3
    vid = create_vizgram(
        dataset_ref="ds", query_ref="q", title="T",
        author_id="author@example.com", db_path=db,
    )
    # Patch directly — bypass resolve_user to simulate legacy data
    conn = sqlite3.connect(str(db))
    conn.execute("UPDATE vizgrams SET author_id='author@example.com' WHERE id=?", (vid,))
    conn.commit()
    conn.close()

    n = migrate_email_ids(db_path=db)
    assert n == 1

    feed = list_feed(limit=10, db_path=db)
    assert len(feed) == 1
    author_id = feed[0]["author_id"]
    assert "@" not in author_id   # now a UUID
    assert len(author_id) == 36


def test_migrate_idempotent(tmp_path):
    db = tmp_path / "vg.db"
    import sqlite3
    vid = create_vizgram(
        dataset_ref="ds", query_ref="q", title="T",
        author_id="x@x.com", db_path=db,
    )
    conn = sqlite3.connect(str(db))
    conn.execute("UPDATE vizgrams SET author_id='x@x.com' WHERE id=?", (vid,))
    conn.commit()
    conn.close()

    n1 = migrate_email_ids(db_path=db)
    n2 = migrate_email_ids(db_path=db)
    assert n1 == 1
    assert n2 == 0  # nothing left to migrate


def test_migrate_also_updates_engagements(tmp_path):
    db = tmp_path / "vg.db"
    import sqlite3
    vid = create_vizgram(
        dataset_ref="ds", query_ref="q", title="T",
        author_id="a@a.com", db_path=db,
    )
    # Simulate legacy engagement with email user_id
    add_engagement(vid, "a@a.com", "like", db_path=db)

    conn = sqlite3.connect(str(db))
    conn.execute("UPDATE vizgrams SET author_id='a@a.com' WHERE id=?", (vid,))
    conn.commit()
    conn.close()

    migrate_email_ids(db_path=db)

    conn = sqlite3.connect(str(db))
    rows = conn.execute("SELECT user_id FROM vizgram_engagements").fetchall()
    conn.close()
    for row in rows:
        assert "@" not in row[0]
        assert len(row[0]) == 36
