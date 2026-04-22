# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/services/tool_config_service.py."""

from pathlib import Path

import pytest
import yaml

from api.services.tool_config_service import (
    delete_tool_config,
    get_tool_config,
    list_tool_configs,
    patch_tool_config,
    put_tool_config,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_config(model_dir: Path, data: dict) -> None:
    with open(model_dir / "config.yaml", "w") as f:
        yaml.dump(data, f)


def _read_config(model_dir: Path) -> dict:
    with open(model_dir / "config.yaml") as f:
        return yaml.safe_load(f) or {}


# ---------------------------------------------------------------------------
# list_tool_configs
# ---------------------------------------------------------------------------

def test_list_empty_config(tmp_path):
    _write_config(tmp_path, {"tools": {}})
    assert list_tool_configs(tmp_path) == []


def test_list_no_config_file(tmp_path):
    assert list_tool_configs(tmp_path) == []


def test_list_returns_all_tools(tmp_path):
    _write_config(tmp_path, {
        "tools": {
            "git": {"enabled": True, "org": "MyOrg", "host": "github.com", "token": "env:GH_TOKEN"},
            "jira": {
                "enabled": False, "server": "https://jira.example.com",
                "email": "me@example.com", "api_token": "file:jira",
            },
        }
    })
    result = list_tool_configs(tmp_path)
    names = {r["name"] for r in result}
    assert names == {"git", "jira"}


def test_list_includes_credential_references(tmp_path):
    _write_config(tmp_path, {
        "tools": {"git": {"enabled": True, "org": "Org", "host": "github.com", "token": "env:GH_TOKEN"}}
    })
    result = list_tool_configs(tmp_path)
    assert result[0]["token"] == "env:GH_TOKEN"


# ---------------------------------------------------------------------------
# get_tool_config
# ---------------------------------------------------------------------------

def test_get_existing_tool(tmp_path):
    _write_config(tmp_path, {
        "tools": {"git": {"enabled": True, "org": "Org", "host": "github.com", "token": "env:GH_TOKEN"}}
    })
    result = get_tool_config(tmp_path, "git")
    assert result["name"] == "git"
    assert result["org"] == "Org"
    assert result["token"] == "env:GH_TOKEN"


def test_get_missing_tool_raises(tmp_path):
    _write_config(tmp_path, {"tools": {}})
    with pytest.raises(KeyError, match="not found"):
        get_tool_config(tmp_path, "git")


# ---------------------------------------------------------------------------
# put_tool_config
# ---------------------------------------------------------------------------

def test_put_creates_new_tool(tmp_path):
    _write_config(tmp_path, {"tools": {}})
    result = put_tool_config(tmp_path, "git", {
        "enabled": True, "org": "NewOrg", "host": "github.com", "token": "env:GH_TOKEN"
    })
    assert result["name"] == "git"
    assert result["org"] == "NewOrg"
    stored = _read_config(tmp_path)
    assert stored["tools"]["git"]["org"] == "NewOrg"


def test_put_replaces_existing_tool(tmp_path):
    _write_config(tmp_path, {
        "tools": {"git": {"enabled": True, "org": "OldOrg", "host": "github.com", "token": "env:OLD"}}
    })
    put_tool_config(tmp_path, "git", {
        "enabled": True, "org": "NewOrg", "host": "github.com", "token": "env:NEW"
    })
    stored = _read_config(tmp_path)
    assert stored["tools"]["git"]["org"] == "NewOrg"
    assert stored["tools"]["git"]["token"] == "env:NEW"


def test_put_preserves_other_tools(tmp_path):
    _write_config(tmp_path, {
        "tools": {"jira": {"enabled": False, "server": "https://jira.example.com"}}
    })
    put_tool_config(tmp_path, "git", {
        "enabled": True, "org": "Org", "host": "github.com", "token": "env:GH_TOKEN"
    })
    stored = _read_config(tmp_path)
    assert "jira" in stored["tools"]
    assert "git" in stored["tools"]


def test_put_rejects_literal_credential(tmp_path):
    _write_config(tmp_path, {"tools": {}})
    with pytest.raises(ValueError, match="env:.*file:"):
        put_tool_config(tmp_path, "git", {
            "enabled": True, "org": "Org", "host": "github.com", "token": "mysecrettoken"
        })


def test_put_accepts_env_credential(tmp_path):
    _write_config(tmp_path, {"tools": {}})
    result = put_tool_config(tmp_path, "git", {
        "enabled": True, "org": "Org", "host": "github.com", "token": "env:GH_TOKEN"
    })
    assert result["token"] == "env:GH_TOKEN"


def test_put_accepts_file_credential(tmp_path):
    _write_config(tmp_path, {"tools": {}})
    result = put_tool_config(tmp_path, "jira", {
        "enabled": True,
        "server": "https://jira.example.com",
        "email": "me@example.com",
        "api_token": "file:jira_token",
    })
    assert result["api_token"] == "file:jira_token"


def test_put_custom_tool_requires_module_and_class(tmp_path):
    _write_config(tmp_path, {"tools": {}})
    with pytest.raises(ValueError, match="class"):
        put_tool_config(tmp_path, "my_tool", {"enabled": True, "module": "tools.custom.my_tool"})


def test_put_custom_tool_valid(tmp_path):
    _write_config(tmp_path, {"tools": {}})
    result = put_tool_config(tmp_path, "my_tool", {
        "enabled": True,
        "module": "tools.custom.my_tool",
        "class": "MyTool",
        "token": "env:MY_TOKEN",
    })
    assert result["module"] == "tools.custom.my_tool"


# ---------------------------------------------------------------------------
# patch_tool_config
# ---------------------------------------------------------------------------

def test_patch_updates_single_field(tmp_path):
    _write_config(tmp_path, {
        "tools": {"git": {"enabled": True, "org": "OldOrg", "host": "github.com", "token": "env:GH_TOKEN"}}
    })
    result = patch_tool_config(tmp_path, "git", {"org": "NewOrg"})
    assert result["org"] == "NewOrg"
    assert result["token"] == "env:GH_TOKEN"  # preserved


def test_patch_enable_disable(tmp_path):
    _write_config(tmp_path, {
        "tools": {"jira": {
            "enabled": True, "server": "https://jira.example.com", "email": "x@x.com", "api_token": "env:JIRA",
        }}
    })
    result = patch_tool_config(tmp_path, "jira", {"enabled": False})
    assert result["enabled"] is False


def test_patch_missing_tool_raises(tmp_path):
    _write_config(tmp_path, {"tools": {}})
    with pytest.raises(KeyError, match="not found"):
        patch_tool_config(tmp_path, "git", {"enabled": False})


def test_patch_rejects_literal_credential(tmp_path):
    _write_config(tmp_path, {
        "tools": {"git": {"enabled": True, "org": "Org", "host": "github.com", "token": "env:GH_TOKEN"}}
    })
    with pytest.raises(ValueError, match="env:.*file:"):
        patch_tool_config(tmp_path, "git", {"token": "newliteraltoken"})


def test_patch_persists_to_disk(tmp_path):
    _write_config(tmp_path, {
        "tools": {"git": {"enabled": True, "org": "OldOrg", "host": "github.com", "token": "env:GH_TOKEN"}}
    })
    patch_tool_config(tmp_path, "git", {"org": "UpdatedOrg"})
    stored = _read_config(tmp_path)
    assert stored["tools"]["git"]["org"] == "UpdatedOrg"


# ---------------------------------------------------------------------------
# delete_tool_config
# ---------------------------------------------------------------------------

def test_delete_removes_tool(tmp_path):
    _write_config(tmp_path, {
        "tools": {
            "git": {"enabled": True, "org": "Org", "host": "github.com", "token": "env:GH_TOKEN"},
            "file": {"enabled": True},
        }
    })
    delete_tool_config(tmp_path, "git")
    stored = _read_config(tmp_path)
    assert "git" not in stored["tools"]
    assert "file" in stored["tools"]


def test_delete_missing_tool_raises(tmp_path):
    _write_config(tmp_path, {"tools": {}})
    with pytest.raises(KeyError, match="not found"):
        delete_tool_config(tmp_path, "git")


def test_delete_last_tool_leaves_empty_tools_block(tmp_path):
    _write_config(tmp_path, {"tools": {"file": {"enabled": True}}})
    delete_tool_config(tmp_path, "file")
    stored = _read_config(tmp_path)
    assert stored["tools"] == {}
