# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/services/mapper_service.py."""

from unittest.mock import MagicMock, patch

import pytest

from api.services.mapper_service import (
    execute_mapper,
    get_mapper,
    list_mappers,
    validate_mapper,
)

# ---------------------------------------------------------------------------
# Minimal mapper YAML
# ---------------------------------------------------------------------------

_WIDGET_MAPPER_YAML = """\
mapper: widget
description: "Build Widget records from raw data"

sources:
  - alias: w
    table: raw_widgets
    columns: [widget_key, name]

targets:
  - entity: Widget
    rows:
      - from: w
        columns:
          - name: widget_key
            expr: w.widget_key
          - name: name
            expr: w.name
"""


@pytest.fixture
def model_dir(tmp_path):
    (tmp_path / "data").mkdir()
    return tmp_path


@pytest.fixture
def model_dir_with_mapper(model_dir):
    from tests.conftest import seed_artifact
    seed_artifact(model_dir, "mapper", "widget", _WIDGET_MAPPER_YAML)
    return model_dir


# ---------------------------------------------------------------------------
# list_mappers
# ---------------------------------------------------------------------------

def test_list_mappers_empty_when_no_dir(tmp_path):
    result = list_mappers(tmp_path)
    assert result == []


def test_list_mappers_returns_mapper_names(model_dir_with_mapper):
    result = list_mappers(model_dir_with_mapper)
    names = [m["name"] for m in result]
    assert "widget" in names


def test_list_mappers_multiple(model_dir):
    from tests.conftest import seed_artifact
    seed_artifact(model_dir, "mapper", "widget", _WIDGET_MAPPER_YAML)
    seed_artifact(model_dir, "mapper", "gadget",
                  _WIDGET_MAPPER_YAML.replace("widget", "gadget").replace("Widget", "Gadget"))
    result = list_mappers(model_dir)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# get_mapper (by entity name)
# ---------------------------------------------------------------------------

def test_get_mapper_raises_key_error_for_unknown_entity(model_dir_with_mapper):
    with pytest.raises(KeyError):
        get_mapper(model_dir_with_mapper, "NonExistent")


def test_get_mapper_returns_mapper_for_entity(model_dir_with_mapper):
    result = get_mapper(model_dir_with_mapper, "Widget")
    assert result["name"] == "widget"


# ---------------------------------------------------------------------------
# validate_mapper
# ---------------------------------------------------------------------------

def test_validate_mapper_raises_key_error_when_entity_not_found(model_dir):
    with pytest.raises(KeyError):
        validate_mapper(model_dir, "Widget")


def test_validate_mapper_returns_result_for_valid_mapper(model_dir_with_mapper):
    result = validate_mapper(model_dir_with_mapper, "Widget")
    assert "valid" in result
    assert "errors" in result


# ---------------------------------------------------------------------------
# execute_mapper — regression: correct argument order to run_mapper
# ---------------------------------------------------------------------------

def test_execute_mapper_passes_correct_args_to_run_mapper(model_dir_with_mapper):
    """
    Regression: original bug called run_mapper(mc, db, entities) which caused
    "'SQLiteBackend' object is not iterable". Fixed to run_mapper(mc, entities, backend).
    """
    fake_job_service = MagicMock()
    fake_job = MagicMock()
    fake_job.job_id = "test-job-id"
    fake_job_service.create.return_value = fake_job

    captured_fn = []

    def capture_submit(fn, *args, **kwargs):
        captured_fn.append(fn)

    fake_job_service.submit.side_effect = capture_submit

    with patch("core.registry.append_job_audit"):
        execute_mapper(model_dir_with_mapper, "Widget", fake_job_service)

    assert len(captured_fn) == 1

    mock_entity = MagicMock()
    mock_entity.name = "Widget"
    mock_backend = MagicMock()
    with (
        patch("semantic.yaml_adapter.YAMLAdapter.load_entities", return_value=[mock_entity]) as _mock_adapter,
        patch("core.db.get_backend", return_value=mock_backend) as _mock_get_backend,
        patch("engine.mapper.run_mapper") as mock_run_mapper,
        patch("core.registry.load_registry", return_value={}),
    ):
        mock_run_mapper.return_value = MagicMock(rows_written=5)

        captured_fn[0]()

        # Verify run_mapper was called with (mc, entities, backend) — NOT (mc, db, entities)
        assert mock_run_mapper.called
        call_args = mock_run_mapper.call_args
        positional = call_args[0]
        assert len(positional) == 3
        # Second arg must be entities (list), NOT a SQLiteBackend
        assert positional[1] == [mock_entity]
        # Third arg must be the backend object
        assert positional[2] == mock_backend
