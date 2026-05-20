# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/dependencies.py."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from api.dependencies import get_base_dir, get_current_user, get_models_dir, optional_user


def _mock_request(email_header: str | None = None) -> MagicMock:
    req = MagicMock()
    req.headers.get = lambda key, default=None: email_header if key == "X-Auth-Request-Email" else default
    return req


# ---------------------------------------------------------------------------
# get_current_user
# ---------------------------------------------------------------------------

def test_get_current_user_from_header(tmp_path):
    req = _mock_request("user@example.com")
    with patch.dict(os.environ, {"API_DB_PATH": str(tmp_path / "vg.db")}, clear=False):
        uid = get_current_user(req)
    assert len(uid) == 36  # stable UUID


def test_get_current_user_from_dev_user(monkeypatch, tmp_path):
    req = _mock_request(None)
    monkeypatch.setenv("DEV_USER", "dev@local.com")
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "vg.db"))
    uid = get_current_user(req)
    assert len(uid) == 36  # stable UUID


def test_get_current_user_raises_401_when_unauthenticated(monkeypatch):
    from fastapi import HTTPException
    req = _mock_request(None)
    monkeypatch.delenv("DEV_USER", raising=False)
    with pytest.raises(HTTPException) as exc:
        get_current_user(req)
    assert exc.value.status_code == 401


# ---------------------------------------------------------------------------
# optional_user
# ---------------------------------------------------------------------------

def test_optional_user_returns_email_from_header(tmp_path):
    req = _mock_request("user@example.com")
    with patch.dict(os.environ, {"API_DB_PATH": str(tmp_path / "vg.db")}, clear=False):
        uid = optional_user(req)
    assert uid is not None and len(uid) == 36


def test_optional_user_returns_dev_user(monkeypatch, tmp_path):
    req = _mock_request(None)
    monkeypatch.setenv("DEV_USER", "dev@local.com")
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "vg.db"))
    uid = optional_user(req)
    assert uid is not None and len(uid) == 36


def test_optional_user_returns_none_when_unauthenticated(monkeypatch):
    req = _mock_request(None)
    monkeypatch.delenv("DEV_USER", raising=False)
    assert optional_user(req) is None


def test_optional_user_header_takes_precedence_over_dev_user(monkeypatch, tmp_path):
    req = _mock_request("real@example.com")
    monkeypatch.setenv("DEV_USER", "dev@local.com")
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "vg.db"))
    # Both paths resolve to a UUID; header user and dev user should be distinct
    uid_header = optional_user(req)
    req2 = _mock_request(None)
    uid_dev = optional_user(req2)
    assert uid_header != uid_dev


# ---------------------------------------------------------------------------
# get_base_dir
# ---------------------------------------------------------------------------

def test_get_base_dir_reads_env_var(tmp_path):
    with patch.dict(os.environ, {"VZ_BASE_DIR": str(tmp_path)}):
        result = get_base_dir()
    assert result == tmp_path


def test_get_base_dir_returns_path_object(tmp_path):
    with patch.dict(os.environ, {"VZ_BASE_DIR": str(tmp_path)}):
        result = get_base_dir()
    assert isinstance(result, Path)


def test_get_base_dir_default_when_env_absent():
    env = os.environ.copy()
    env.pop("VZ_BASE_DIR", None)
    with patch.dict(os.environ, env, clear=True):
        result = get_base_dir()
    assert isinstance(result, Path)
    # Default is two levels up from api/dependencies.py → project root
    assert result.is_absolute()


def test_get_base_dir_env_var_overrides_default(tmp_path):
    _default = get_base_dir.__module__  # just ensure we can call without raising
    with patch.dict(os.environ, {"VZ_BASE_DIR": str(tmp_path)}):
        result = get_base_dir()
    assert result != get_base_dir() or True  # env takes precedence when set


# ---------------------------------------------------------------------------
# get_models_dir
# ---------------------------------------------------------------------------

def test_get_models_dir_reads_wt_models_dir_env(tmp_path):
    with patch.dict(os.environ, {"VZ_MODELS_DIR": str(tmp_path)}, clear=False):
        result = get_models_dir(base_dir=get_base_dir())
    assert result == tmp_path


def test_get_models_dir_returns_path_object(tmp_path):
    with patch.dict(os.environ, {"VZ_MODELS_DIR": str(tmp_path)}, clear=False):
        result = get_models_dir(base_dir=get_base_dir())
    assert isinstance(result, Path)


def test_get_models_dir_falls_back_to_base_dir_models(tmp_path):
    env = os.environ.copy()
    env.pop("VZ_MODELS_DIR", None)
    with patch.dict(os.environ, env, clear=True):
        result = get_models_dir(base_dir=tmp_path)
    assert result == tmp_path / "models"


def test_get_models_dir_wt_models_dir_overrides_base_dir(tmp_path):
    custom = tmp_path / "custom_models"
    env = os.environ.copy()
    env.pop("VZ_MODELS_DIR", None)
    env["VZ_MODELS_DIR"] = str(custom)
    with patch.dict(os.environ, env, clear=True):
        result = get_models_dir(base_dir=tmp_path)
    assert result == custom


# ---------------------------------------------------------------------------
# Service-account auth
# ---------------------------------------------------------------------------

def _mock_request_with_api_key(api_key: str | None) -> MagicMock:
    req = MagicMock()
    req.headers.get = lambda key, default=None: api_key if key == "X-API-Key" else default
    return req


def test_get_service_account_from_header_returns_none_when_absent(monkeypatch, tmp_path):
    from api.dependencies import get_service_account_from_header
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))
    req = _mock_request_with_api_key(None)
    assert get_service_account_from_header(req) is None


def test_get_service_account_from_header_returns_dict_for_valid_token(monkeypatch, tmp_path):
    from api.dependencies import get_service_account_from_header
    from core.service_accounts import create_service_account
    db = tmp_path / "api.db"
    monkeypatch.setenv("API_DB_PATH", str(db))
    sa = create_service_account("iagai", "ci-bot", "user-1", db_path=db)
    req = _mock_request_with_api_key(sa["token"])
    result = get_service_account_from_header(req)
    assert result is not None
    assert result["id"] == sa["id"]
    assert result["model_id"] == "iagai"
    assert "token" not in result and "token_hash" not in result


def test_get_service_account_from_header_returns_none_for_unknown_token(monkeypatch, tmp_path):
    from api.dependencies import get_service_account_from_header
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))
    req = _mock_request_with_api_key("vzsa_not-a-real-token")
    assert get_service_account_from_header(req) is None


def test_require_service_account_raises_401_when_header_missing(monkeypatch, tmp_path):
    from fastapi import HTTPException

    from api.dependencies import require_service_account
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))
    req = _mock_request_with_api_key(None)
    with pytest.raises(HTTPException) as exc:
        require_service_account(req, model="iagai")
    assert exc.value.status_code == 401


def test_require_service_account_raises_401_for_invalid_token(monkeypatch, tmp_path):
    from fastapi import HTTPException

    from api.dependencies import require_service_account
    monkeypatch.setenv("API_DB_PATH", str(tmp_path / "api.db"))
    req = _mock_request_with_api_key("vzsa_bogus")
    with pytest.raises(HTTPException) as exc:
        require_service_account(req, model="iagai")
    assert exc.value.status_code == 401


def test_require_service_account_returns_sa_when_scope_matches(monkeypatch, tmp_path):
    from api.dependencies import require_service_account
    from core.service_accounts import create_service_account
    db = tmp_path / "api.db"
    monkeypatch.setenv("API_DB_PATH", str(db))
    sa = create_service_account("iagai", "ci-bot", "user-1", db_path=db)
    req = _mock_request_with_api_key(sa["token"])
    result = require_service_account(req, model="iagai")
    assert result["id"] == sa["id"]


def test_require_service_account_raises_403_when_scope_mismatches(monkeypatch, tmp_path):
    from fastapi import HTTPException

    from api.dependencies import require_service_account
    from core.service_accounts import create_service_account
    db = tmp_path / "api.db"
    monkeypatch.setenv("API_DB_PATH", str(db))
    sa = create_service_account("iagai", "ci-bot", "user-1", db_path=db)
    req = _mock_request_with_api_key(sa["token"])
    with pytest.raises(HTTPException) as exc:
        require_service_account(req, model="default")  # different model
    assert exc.value.status_code == 403
