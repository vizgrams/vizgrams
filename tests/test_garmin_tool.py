# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for the Garmin tool's client bootstrap.

Focused on ``_get_client`` — the token-restore path used to set
``display_name`` from ``client.get_full_name()``, which returns ``None``
on token-restored sessions. Several endpoints (heart_rate, personal
records, anything under wellness/personalrecord services) bake
``display_name`` into their URL path, so a None here yielded silent
403s and 0 rows written for entire tasks.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def garmin_module(tmp_path, monkeypatch):
    """Import the tool with the module-level client cache cleared and
    ``garminconnect`` mocked so no real HTTP happens."""
    fake_garminconnect = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "garminconnect", fake_garminconnect)

    from tools.garmin import tool as garmin_tool
    garmin_tool._client_cache.clear()
    return garmin_tool


def _fake_garth_client(*, display_name: str | None = "ofenton") -> MagicMock:
    """A ``Garmin(...)`` return value with the garth profile populated the
    way ``garth.loads(token_json)`` does after restoring from disk."""
    client = MagicMock()
    client.garth.profile = {"displayName": display_name}
    # get_full_name is what the old code called — return None to model
    # the real library behaviour on token-restored sessions.
    client.get_full_name.return_value = None
    return client


def test_display_name_populated_from_garth_profile_on_token_restore(
    garmin_module, tmp_path,
):
    """Regression: ``display_name`` MUST come from
    ``client.garth.profile['displayName']``, not ``get_full_name()``.

    Baking this into a test because the previous code silently produced
    URLs like ``/wellness/dailyHeartRate/None?date=…`` and Garmin
    returned 403s the extractor logged as WARNINGs — the top-level job
    reported ``completed`` with 0 records and the user had to trace it
    manually.
    """
    token_file = tmp_path / "tokens.json"
    token_file.write_text('{"fake": "tokens"}')

    fake_client = _fake_garth_client(display_name="ofenton")

    tool = garmin_module.GarminTool({
        "email": "test@example.com",
        "password": "pw",
        "token_store": f"file:{token_file}",
    })
    # Patch the Garmin() constructor bound onto the instance
    tool._Garmin = MagicMock(return_value=fake_client)

    got = tool._get_client()

    assert got is fake_client
    # The fix: display_name comes from garth.profile, not get_full_name.
    assert fake_client.display_name == "ofenton"


def test_display_name_missing_profile_leaves_it_none(garmin_module, tmp_path):
    """If garth.profile has no displayName (very rare — corrupted token
    file, library upgrade), we still get ``None`` rather than an
    AttributeError. The subsequent 403 is worth surfacing as the
    diagnostic — better than a KeyError on module init."""
    token_file = tmp_path / "tokens.json"
    token_file.write_text("{}")

    fake_client = _fake_garth_client(display_name=None)
    fake_client.garth.profile = {}  # no displayName key at all

    tool = garmin_module.GarminTool({
        "email": "test@example.com",
        "password": "pw",
        "token_store": f"file:{token_file}",
    })
    tool._Garmin = MagicMock(return_value=fake_client)

    got = tool._get_client()

    assert got is fake_client
    assert fake_client.display_name is None


def test_token_restore_failure_falls_back_to_interactive_login(
    garmin_module, tmp_path,
):
    """Corrupt / expired token file. Old fallback path used ``client.login()``
    and never touched ``display_name`` — verify the login-based path still
    works (get_full_name populates display_name during login itself,
    same as the library documents)."""
    token_file = tmp_path / "tokens.json"
    token_file.write_text("not-json")

    fake_client = _fake_garth_client()
    # Simulate garth.loads raising on bad JSON.
    fake_client.garth.loads.side_effect = ValueError("bad JSON")

    tool = garmin_module.GarminTool({
        "email": "test@example.com",
        "password": "pw",
        "token_store": f"file:{token_file}",
    })
    tool._Garmin = MagicMock(return_value=fake_client)

    with patch.object(tool, "_persist_tokens"):
        got = tool._get_client()

    assert got is fake_client
    fake_client.login.assert_called_once()  # fallback landed


# ---------------------------------------------------------------------------
# env: token_store — for prod deployment where writing to disk is not
# practical (containers with read-only root FS, SSM-injected secrets).
# ---------------------------------------------------------------------------


def test_env_token_store_reads_from_env_var(garmin_module, monkeypatch):
    """``token_store: env:GARMIN_TOKEN_JSON`` reads the raw JSON from the
    named env var. This is how prod bootstrap works — .env.prod carries the
    token JSON that garth wrote out on a successful login on the dev
    machine; no SCP, no tokens-on-disk in the container image."""
    monkeypatch.setenv("GARMIN_TOKEN_JSON", '{"fake": "prod-token-payload"}')

    fake_client = _fake_garth_client(display_name="ofenton")

    tool = garmin_module.GarminTool({
        "email": "test@example.com",
        "password": "pw",
        "token_store": "env:GARMIN_TOKEN_JSON",
    })
    tool._Garmin = MagicMock(return_value=fake_client)

    got = tool._get_client()

    assert got is fake_client
    fake_client.garth.loads.assert_called_once_with('{"fake": "prod-token-payload"}')
    assert fake_client.display_name == "ofenton"


def test_env_token_store_missing_var_raises_clear_error(garmin_module, monkeypatch):
    """When the referenced env var is unset the tool falls into the same
    "could not restore tokens" path as a corrupt file, which drops through
    to interactive ``login()``. Better than a silent None-token restore
    that produces the 403-URL bug we fixed with display_name."""
    monkeypatch.delenv("GARMIN_TOKEN_JSON", raising=False)

    fake_client = _fake_garth_client()
    tool = garmin_module.GarminTool({
        "email": "test@example.com",
        "password": "pw",
        "token_store": "env:GARMIN_TOKEN_JSON",
    })
    tool._Garmin = MagicMock(return_value=fake_client)

    with patch.object(tool, "_persist_tokens"):
        got = tool._get_client()

    assert got is fake_client
    fake_client.login.assert_called_once()  # fell through to interactive login


def test_env_token_store_does_not_persist(garmin_module, monkeypatch):
    """Env-backed token stores are read-only — writing tokens back to
    /proc/self/environ is nonsense. Guards against the daily scheduler
    trying (and failing) to persist refreshed tokens on every run."""
    monkeypatch.setenv("GARMIN_TOKEN_JSON", '{"fake": "tokens"}')

    fake_client = _fake_garth_client(display_name="ofenton")
    tool = garmin_module.GarminTool({
        "email": "test@example.com",
        "password": "pw",
        "token_store": "env:GARMIN_TOKEN_JSON",
    })
    tool._Garmin = MagicMock(return_value=fake_client)

    # Spy on _persist_tokens — it's called after successful restore, but
    # should no-op when the store is env-backed.
    with patch.object(tool, "_persist_tokens", wraps=tool._persist_tokens) as spy:
        tool._get_client()

    spy.assert_called_once()  # called...
    # ...but writing garth.dumps to disk is what we're guarding against.
    # If the guard ever regresses, this call will fail because MagicMock's
    # ``garth.dumps()`` returns a MagicMock, not JSON-serialisable data.
    fake_client.garth.dumps.assert_not_called()
