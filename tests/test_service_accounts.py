# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for core/service_accounts.py."""

import pytest

from core.service_accounts import (
    TOKEN_PREFIX,
    create_service_account,
    get_service_account,
    hash_token,
    list_service_accounts,
    revoke_service_account,
    verify_token,
)


@pytest.fixture
def db(tmp_path):
    return tmp_path / "api.db"


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------

def test_create_returns_plaintext_token_with_prefix(db):
    sa = create_service_account("iagai", "ci-bot", "user-1", db_path=db)
    assert sa["token"].startswith(TOKEN_PREFIX)
    assert len(sa["token"]) > len(TOKEN_PREFIX) + 30  # enough entropy
    assert sa["model_id"] == "iagai"
    assert sa["name"] == "ci-bot"
    assert sa["created_by"] == "user-1"
    assert sa["is_active"] is True
    assert sa["last_used_at"] is None
    assert "token_hash" not in sa


def test_create_persists_only_hash_not_plaintext(db):
    sa = create_service_account("iagai", "ci-bot", "user-1", db_path=db)
    # Reload from DB — token should be gone, only the metadata.
    fetched = get_service_account(sa["id"], db_path=db)
    assert fetched is not None
    assert "token" not in fetched
    assert "token_hash" not in fetched  # also stripped from the public dict
    assert fetched["name"] == "ci-bot"


def test_create_distinct_tokens_per_account(db):
    a = create_service_account("iagai", "bot-a", "u1", db_path=db)
    b = create_service_account("iagai", "bot-b", "u1", db_path=db)
    assert a["token"] != b["token"]
    assert a["id"] != b["id"]


def test_unique_active_name_per_model(db):
    create_service_account("iagai", "ci-bot", "u1", db_path=db)
    with pytest.raises(Exception):  # noqa: B017 — sqlite IntegrityError variant
        create_service_account("iagai", "ci-bot", "u1", db_path=db)


def test_same_name_allowed_across_models(db):
    a = create_service_account("iagai", "ci-bot", "u1", db_path=db)
    b = create_service_account("default", "ci-bot", "u1", db_path=db)
    assert a["model_id"] != b["model_id"]
    assert a["name"] == b["name"] == "ci-bot"


def test_name_reusable_after_revoke(db):
    """Active uniqueness must not block reusing the name once revoked."""
    a = create_service_account("iagai", "ci-bot", "u1", db_path=db)
    assert revoke_service_account(a["id"], db_path=db)
    # Reusing the same name on a fresh active row should now succeed.
    b = create_service_account("iagai", "ci-bot", "u1", db_path=db)
    assert b["id"] != a["id"]


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

def test_verify_valid_token_returns_account_and_updates_last_used(db):
    sa = create_service_account("iagai", "ci-bot", "u1", db_path=db)
    result = verify_token(sa["token"], db_path=db)
    assert result is not None
    assert result["id"] == sa["id"]
    assert result["model_id"] == "iagai"
    assert "token_hash" not in result
    # last_used_at populated after verify
    refreshed = get_service_account(sa["id"], db_path=db)
    assert refreshed["last_used_at"] is not None


def test_verify_unknown_token_returns_none(db):
    create_service_account("iagai", "ci-bot", "u1", db_path=db)
    assert verify_token(f"{TOKEN_PREFIX}not-a-real-token", db_path=db) is None


def test_verify_missing_prefix_returns_none(db):
    sa = create_service_account("iagai", "ci-bot", "u1", db_path=db)
    # Strip the prefix — must fail fast without DB lookup.
    bare = sa["token"][len(TOKEN_PREFIX):]
    assert verify_token(bare, db_path=db) is None


def test_verify_empty_returns_none(db):
    assert verify_token("", db_path=db) is None


def test_verify_revoked_token_returns_none(db):
    sa = create_service_account("iagai", "ci-bot", "u1", db_path=db)
    revoke_service_account(sa["id"], db_path=db)
    assert verify_token(sa["token"], db_path=db) is None


# ---------------------------------------------------------------------------
# list / get / revoke
# ---------------------------------------------------------------------------

def test_list_filters_inactive_by_default(db):
    a = create_service_account("iagai", "active", "u1", db_path=db)
    b = create_service_account("iagai", "to-revoke", "u1", db_path=db)
    revoke_service_account(b["id"], db_path=db)

    active = list_service_accounts("iagai", db_path=db)
    assert {x["id"] for x in active} == {a["id"]}

    everything = list_service_accounts("iagai", include_inactive=True, db_path=db)
    assert {x["id"] for x in everything} == {a["id"], b["id"]}


def test_list_scopes_by_model(db):
    create_service_account("iagai", "a", "u1", db_path=db)
    create_service_account("default", "b", "u1", db_path=db)
    iagai = list_service_accounts("iagai", db_path=db)
    default = list_service_accounts("default", db_path=db)
    assert [x["name"] for x in iagai] == ["a"]
    assert [x["name"] for x in default] == ["b"]


def test_revoke_idempotent(db):
    sa = create_service_account("iagai", "ci-bot", "u1", db_path=db)
    assert revoke_service_account(sa["id"], db_path=db) is True
    assert revoke_service_account(sa["id"], db_path=db) is False


def test_revoke_unknown_returns_false(db):
    assert revoke_service_account("not-a-real-id", db_path=db) is False


# ---------------------------------------------------------------------------
# hash_token
# ---------------------------------------------------------------------------

def test_hash_token_is_deterministic():
    assert hash_token("vzsa_abc") == hash_token("vzsa_abc")
    assert hash_token("vzsa_abc") != hash_token("vzsa_abd")


def test_hash_token_is_hex_sha256():
    h = hash_token("vzsa_anything")
    assert len(h) == 64
    int(h, 16)  # raises if not hex
