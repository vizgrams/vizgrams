# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the file-based extractor tool."""

import json

import pytest
import yaml

from tools.file.tool import FileTool


@pytest.fixture
def file_tool():
    return FileTool()


def test_list_commands(file_tool):
    assert file_tool.list_commands() == ["load"]


def test_unknown_command(file_tool):
    with pytest.raises(ValueError, match="Unknown file command"):
        list(file_tool.run("nonexistent"))


def test_load_yaml(file_tool, tmp_path):
    data = {"items": [{"id": 1}, {"id": 2}]}
    f = tmp_path / "test.yaml"
    f.write_text(yaml.dump(data))

    records = list(file_tool.run("load", {"path": str(f), "format": "yaml"}))
    assert len(records) == 1
    assert records[0] == data


def test_load_json(file_tool, tmp_path):
    data = {"items": [{"id": 1}, {"id": 2}]}
    f = tmp_path / "test.json"
    f.write_text(json.dumps(data))

    records = list(file_tool.run("load", {"path": str(f), "format": "json"}))
    assert len(records) == 1
    assert records[0] == data


def test_missing_path_param(file_tool):
    with pytest.raises(ValueError, match="Missing required param: 'path'"):
        list(file_tool.run("load", {"format": "yaml"}))


def test_missing_format_param(file_tool):
    with pytest.raises(ValueError, match="Missing required param: 'format'"):
        list(file_tool.run("load", {"path": "/some/file.yaml"}))


def test_unsupported_format(file_tool):
    with pytest.raises(ValueError, match="Unsupported format"):
        list(file_tool.run("load", {"path": "/some/file.xlsx", "format": "xlsx"}))


def test_file_not_found(file_tool):
    with pytest.raises(FileNotFoundError, match="File not found"):
        list(file_tool.run("load", {"path": "/nonexistent/file.yaml", "format": "yaml"}))


def test_load_csv(file_tool, tmp_path):
    f = tmp_path / "test.csv"
    f.write_text("id,name,score\n1,Alice,95\n2,Bob,87\n")

    records = list(file_tool.run("load", {"path": str(f), "format": "csv"}))
    assert len(records) == 1
    assert records[0] == {
        "rows": [
            {"id": "1", "name": "Alice", "score": "95"},
            {"id": "2", "name": "Bob", "score": "87"},
        ]
    }


def test_load_csv_empty(file_tool, tmp_path):
    f = tmp_path / "empty.csv"
    f.write_text("id,name,score\n")

    records = list(file_tool.run("load", {"path": str(f), "format": "csv"}))
    assert len(records) == 1
    assert records[0] == {"rows": []}


def test_relative_path(tmp_path):
    """Relative paths are resolved from model_dir."""
    data = {"key": "value"}
    test_file = tmp_path / "data.yaml"
    test_file.write_text(yaml.dump(data))

    tool = FileTool(model_dir=tmp_path)
    records = list(tool.run("load", {"path": "data.yaml", "format": "yaml"}))
    assert len(records) == 1
    assert records[0] == data
