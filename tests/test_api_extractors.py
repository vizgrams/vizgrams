# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/services/extractor_service.py."""


import pytest

from api.services.extractor_service import (
    get_extractor,
    list_extractors,
    validate_extractor,
)

# ---------------------------------------------------------------------------
# Minimal extractor YAML
# ---------------------------------------------------------------------------

_EXTRACTOR_YAML = """\
tasks:
  - name: fetch_items
    tool: jira
    command: boards
    params: {}
    output:
      table: raw_items
      write_mode: UPSERT
      primary_keys: [id]
      columns:
        - name: id
          json_path: $.id
        - name: label
          json_path: $.name
"""

_EXTRACTOR_TWO_TASKS_YAML = """\
tasks:
  - name: fetch_boards
    tool: jira
    command: boards
    params: {}
    output:
      table: raw_boards
      write_mode: UPSERT
      primary_keys: [id]
      columns:
        - name: id
          json_path: $.id

  - name: fetch_users
    tool: jira
    command: users
    params: {}
    output:
      table: raw_users
      write_mode: UPSERT
      primary_keys: [account_id]
      columns:
        - name: account_id
          json_path: $.accountId
"""


@pytest.fixture
def model_dir(tmp_path):
    (tmp_path / "data").mkdir()
    return tmp_path


@pytest.fixture
def model_dir_with_extractor(model_dir):
    from core.metadata_db import record_version
    record_version(model_dir, "extractor", "jira", _EXTRACTOR_YAML)
    return model_dir


# ---------------------------------------------------------------------------
# list_extractors
# ---------------------------------------------------------------------------

def test_list_extractors_empty_when_no_extractors(tmp_path):
    (tmp_path / "data").mkdir()
    result = list_extractors(tmp_path)
    assert result == []


def test_list_extractors_returns_tool_names(model_dir_with_extractor):
    result = list_extractors(model_dir_with_extractor)
    tools = [e["tool"] for e in result]
    assert "jira" in tools


def test_list_extractors_includes_task_summary(model_dir_with_extractor):
    result = list_extractors(model_dir_with_extractor)
    item = result[0]
    assert "tasks" in item
    assert len(item["tasks"]) == 1
    assert item["tasks"][0]["name"] == "fetch_items"


def test_list_extractors_multiple(model_dir):
    from core.metadata_db import record_version
    record_version(model_dir, "extractor", "alpha", _EXTRACTOR_YAML)
    record_version(model_dir, "extractor", "beta", _EXTRACTOR_YAML)
    result = list_extractors(model_dir)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# get_extractor
# ---------------------------------------------------------------------------

def test_get_extractor_by_tool_name(model_dir_with_extractor):
    result = get_extractor(model_dir_with_extractor, "jira")
    assert result["tool"] == "jira"
    assert len(result["tasks"]) == 1


def test_get_extractor_task_detail_has_command(model_dir_with_extractor):
    result = get_extractor(model_dir_with_extractor, "jira")
    task = result["tasks"][0]
    assert task["command"] == "boards"
    assert "params" in task


def test_get_extractor_raises_key_error_when_not_found(model_dir):
    with pytest.raises(KeyError):
        get_extractor(model_dir, "nonexistent")


def test_get_extractor_multiple_tasks(model_dir):
    from core.metadata_db import record_version
    record_version(model_dir, "extractor", "multi", _EXTRACTOR_TWO_TASKS_YAML)
    result = get_extractor(model_dir, "jira")
    assert len(result["tasks"]) == 2
    task_names = [t["name"] for t in result["tasks"]]
    assert "fetch_boards" in task_names
    assert "fetch_users" in task_names


# ---------------------------------------------------------------------------
# validate_extractor
# ---------------------------------------------------------------------------

def test_validate_extractor_valid(model_dir_with_extractor):
    result = validate_extractor(model_dir_with_extractor, "jira")
    assert "valid" in result
    assert "errors" in result


def test_validate_extractor_raises_key_error_when_not_found(model_dir):
    with pytest.raises(KeyError):
        validate_extractor(model_dir, "nonexistent_tool")
