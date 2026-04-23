# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/services/entity_service.py."""


import pytest
import yaml

from api.services.entity_service import (
    _to_snake,
    create_entity,
    get_entity,
    list_entities,
    upsert_entity,
)

# ---------------------------------------------------------------------------
# Minimal entity YAML fixture
# ---------------------------------------------------------------------------

_WIDGET_YAML = """\
entity: Widget
description: "A test widget"

identity:
  widget_key:
    type: STRING
    semantic: PRIMARY_KEY

attributes:
  name:
    type: STRING
    semantic: IDENTIFIER
  score:
    type: FLOAT
    semantic: MEASURE
"""


@pytest.fixture
def model_dir(tmp_path):
    """A minimal model directory with data/ subdir (ontology now in DB)."""
    (tmp_path / "data").mkdir()
    (tmp_path / "config.yaml").write_text("database:\n  backend: sqlite\n")
    return tmp_path


@pytest.fixture
def model_dir_with_widget(model_dir):
    from tests.conftest import seed_artifact
    seed_artifact(model_dir, "entity", "widget", _WIDGET_YAML)
    return model_dir


# ---------------------------------------------------------------------------
# _to_snake
# ---------------------------------------------------------------------------

def test_to_snake_pascal_case():
    assert _to_snake("PullRequest") == "pull_request"


def test_to_snake_already_lower():
    assert _to_snake("widget") == "widget"


def test_to_snake_mixed():
    assert _to_snake("ProductVersion") == "product_version"


# ---------------------------------------------------------------------------
# list_entities
# ---------------------------------------------------------------------------

def test_list_entities_empty_when_no_ontology_dir(tmp_path):
    result = list_entities(tmp_path)
    assert result == []


def test_list_entities_returns_entity_names(model_dir_with_widget):
    result = list_entities(model_dir_with_widget)
    names = [e["name"] for e in result]
    assert "Widget" in names


def test_list_entities_includes_counts(model_dir_with_widget):
    result = list_entities(model_dir_with_widget)
    widget = next(e for e in result if e["name"] == "Widget")
    assert widget["attribute_count"] >= 1
    assert "table_name" in widget


# ---------------------------------------------------------------------------
# get_entity
# ---------------------------------------------------------------------------

def test_get_entity_returns_attributes(model_dir_with_widget):
    result = get_entity(model_dir_with_widget, "Widget")
    assert result["name"] == "Widget"
    attr_names = [a["name"] for a in result["attributes"]]
    assert "name" in attr_names
    assert "score" in attr_names


def test_get_entity_attributes_use_col_type_not_type(model_dir_with_widget):
    """Regression: AttributeDef uses .col_type, not .type."""
    result = get_entity(model_dir_with_widget, "Widget")
    for attr in result["attributes"]:
        assert "type" in attr
        # Should be a string like "STRING" or "FLOAT", not raise AttributeError
        assert isinstance(attr["type"], str)


def test_get_entity_raises_key_error_when_not_found(model_dir):
    with pytest.raises(KeyError):
        get_entity(model_dir, "NonExistent")


def test_get_entity_includes_database_stats(model_dir_with_widget):
    result = get_entity(model_dir_with_widget, "Widget")
    assert "database" in result
    assert "present" in result["database"]


# ---------------------------------------------------------------------------
# create_entity
# ---------------------------------------------------------------------------

def test_create_entity_writes_yaml(model_dir):
    from core.metadata_db import get_current_content
    data = {
        "name": "Gadget",
        "description": "A gadget",
        "identity": {"gadget_key": {"type": "STRING", "semantic": "PRIMARY_KEY"}},
        "attributes": {"label": {"type": "STRING", "semantic": "IDENTIFIER"}},
    }
    create_entity(model_dir, data)
    assert get_current_content(model_dir, "entity", "Gadget") is not None


def test_create_entity_written_yaml_is_parseable(model_dir):
    from core.metadata_db import get_current_content
    data = {
        "name": "Gadget",
        "identity": {"gadget_key": {"type": "STRING", "semantic": "PRIMARY_KEY"}},
        "attributes": {},
    }
    create_entity(model_dir, data)
    content = get_current_content(model_dir, "entity", "Gadget")
    parsed = yaml.safe_load(content)
    assert parsed["entity"] == "Gadget"


def test_create_entity_raises_file_exists_error_on_duplicate(model_dir):
    data = {
        "name": "Gadget",
        "identity": {"gadget_key": {"type": "STRING", "semantic": "PRIMARY_KEY"}},
        "attributes": {},
    }
    create_entity(model_dir, data)
    with pytest.raises(FileExistsError):
        create_entity(model_dir, data)


# ---------------------------------------------------------------------------
# upsert_entity
# ---------------------------------------------------------------------------

def test_upsert_entity_creates_new(model_dir):
    from core.metadata_db import get_current_content
    data = {
        "name": "Thing",
        "identity": {"thing_key": {"type": "STRING", "semantic": "PRIMARY_KEY"}},
        "attributes": {},
    }
    result, created = upsert_entity(model_dir, "Thing", data)
    assert created is True
    assert get_current_content(model_dir, "entity", "Thing") is not None


def test_upsert_entity_overwrites_existing(model_dir_with_widget):
    data = {
        "name": "Widget",
        "identity": {"widget_key": {"type": "STRING", "semantic": "PRIMARY_KEY"}},
        "attributes": {"label": {"type": "STRING", "semantic": "IDENTIFIER"}},
    }
    result, created = upsert_entity(model_dir_with_widget, "Widget", data)
    assert created is False
    attr_names = [a["name"] for a in result["attributes"]]
    assert "label" in attr_names
