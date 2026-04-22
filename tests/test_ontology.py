# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for semantic.ontology — parser, validator, and ontology YAML files."""

from pathlib import Path

import pytest
import yaml

from core.validation import validate_schema
from semantic.ontology import (
    _check_entity_rules,
    load_all_entities,
    load_entity_by_name,
    parse_entity_yaml,
    validate_all_entities,
    validate_ontology_yaml,
)
from semantic.types import ColumnType

ONTOLOGY_DIR = Path(__file__).resolve().parent.parent / "models" / "example" / "ontology"


def _write_yaml(tmp_path, data, filename="test.yaml"):
    p = tmp_path / filename
    p.write_text(yaml.dump(data, sort_keys=False))
    return p


def _minimal_entity(**overrides):
    """Return a minimal valid entity dict with identity + history."""
    obj = {
        "entity": "TestEntity",
        "identity": {
            "test_key": {"type": "STRING", "semantic": "PRIMARY_KEY"},
        },
        "attributes": {
            "name": {"type": "STRING", "semantic": "IDENTIFIER"},
        },
        "history": {
            "type": "SCD2",
            "valid_from": {"type": "STRING", "semantic": "SCD_FROM"},
            "valid_to": {"type": "STRING", "semantic": "SCD_TO"},
        },
    }
    obj.update(overrides)
    return obj


def _minimal_entity_no_history(**overrides):
    """Return a minimal valid entity dict without history."""
    obj = {
        "entity": "TestEntity",
        "identity": {
            "test_key": {"type": "STRING", "semantic": "PRIMARY_KEY"},
        },
        "attributes": {
            "name": {"type": "STRING", "semantic": "IDENTIFIER"},
        },
    }
    obj.update(overrides)
    return obj


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


class TestParsing:
    def test_parse_entity(self, tmp_path):
        data = _minimal_entity()
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)

        assert entity.name == "TestEntity"
        assert len(entity.identity) == 1
        assert len(entity.attributes) == 1
        assert entity.history is not None
        assert len(entity.history.columns) == 2
        pk = entity.primary_key
        assert pk is not None
        assert pk.name == "test_key"
        assert pk.col_type == ColumnType.STRING

    def test_parse_with_events(self, tmp_path):
        data = _minimal_entity()
        data["events"] = {
            "lifecycle": {
                "description": "State transitions",
                "attributes": {
                    "state": {"type": "STRING", "semantic": "STATE"},
                    "inserted_at": {"type": "STRING", "semantic": "INSERTED_AT"},
                },
            }
        }
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)

        assert len(entity.events) == 1
        assert entity.events[0].name == "lifecycle"
        assert len(entity.events[0].attributes) == 2

    def test_parse_with_relations(self, tmp_path):
        data = _minimal_entity()
        data["identity"]["parent_key"] = {
            "type": "STRING",
            "semantic": "RELATION",
            "references": "ParentEntity",
        }
        data["relations"] = {
            "parent": {
                "target": "ParentEntity",
                "via": "parent_key",
                "cardinality": "MANY_TO_ONE",
            }
        }
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)

        assert len(entity.relation_columns) == 1
        assert entity.relation_columns[0].references == "ParentEntity"
        assert len(entity.relations) == 1
        assert entity.relations[0].target == "ParentEntity"

    def test_table_name_simple(self, tmp_path):
        data = _minimal_entity(entity="Product")
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)
        assert entity.table_name == "product"

    def test_table_name_multi_word(self, tmp_path):
        data = _minimal_entity(entity="ProductVersion")
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)
        assert entity.table_name == "product_version"

    def test_table_name_triple_word(self, tmp_path):
        data = _minimal_entity(entity="ProductVersionLifecycleEvent")
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)
        assert entity.table_name == "product_version_lifecycle_event"

    def test_primary_key_property(self, tmp_path):
        data = _minimal_entity()
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)
        pk = entity.primary_key
        assert pk is not None
        assert pk.name == "test_key"

    def test_all_base_columns(self, tmp_path):
        data = _minimal_entity()
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)
        col_names = [c.name for c in entity.all_base_columns]
        assert "test_key" in col_names
        assert "name" in col_names
        assert "valid_from" in col_names
        assert "valid_to" in col_names

    def test_tracked_columns(self, tmp_path):
        data = _minimal_entity()
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)
        tracked_names = [c.name for c in entity.tracked_columns]
        assert "name" in tracked_names
        assert "test_key" not in tracked_names

    def test_event_table_name(self, tmp_path):
        data = _minimal_entity(entity="ProductVersion")
        data["events"] = {
            "lifecycle": {
                "attributes": {
                    "inserted_at": {"type": "STRING", "semantic": "INSERTED_AT"},
                },
            }
        }
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)
        assert entity.event_table_name(entity.events[0]) == "product_version_lifecycle_event"

    def test_event_columns(self, tmp_path):
        data = _minimal_entity()
        data["events"] = {
            "lifecycle": {
                "attributes": {
                    "state": {"type": "STRING", "semantic": "STATE"},
                    "inserted_at": {"type": "STRING", "semantic": "INSERTED_AT"},
                },
            }
        }
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)
        ev_cols = entity.event_columns(entity.events[0])
        col_names = [c.name for c in ev_cols]
        assert "test_key" in col_names  # parent PK as FK
        assert "state" in col_names
        assert "inserted_at" in col_names

    def test_description(self, tmp_path):
        data = _minimal_entity(description="A test entity")
        path = _write_yaml(tmp_path, data)
        entity = parse_entity_yaml(path)
        assert entity.description == "A test entity"


# ---------------------------------------------------------------------------
# Load all / Load by name
# ---------------------------------------------------------------------------


class TestLoadAll:
    def setup_method(self):
        from core.metadata_db import seed_from_directory
        seed_from_directory(ONTOLOGY_DIR.parent)

    def test_load_all_from_ontology_dir(self):
        entities = load_all_entities(ONTOLOGY_DIR)
        assert len(entities) >= 6
        names = {e.name for e in entities}
        assert "Product" in names
        assert "Domain" in names
        assert "ProductVersion" in names

    def test_load_entity_by_name(self):
        entity = load_entity_by_name("Product", ONTOLOGY_DIR)
        assert entity is not None
        assert entity.name == "Product"

    def test_load_entity_by_name_not_found(self):
        entity = load_entity_by_name("NonExistent", ONTOLOGY_DIR)
        assert entity is None

    def test_load_all_from_empty_dir(self, tmp_path):
        entities = load_all_entities(tmp_path)
        assert entities == []


# ---------------------------------------------------------------------------
# Phase 1: Schema validation
# ---------------------------------------------------------------------------


class TestSchemaValidation:
    def test_valid_entity(self):
        assert validate_schema(_minimal_entity(), "ontology") == []

    def test_valid_entity_no_history(self):
        assert validate_schema(_minimal_entity_no_history(), "ontology") == []

    def test_missing_entity(self):
        data = _minimal_entity()
        del data["entity"]
        errs = validate_schema(data, "ontology")
        assert len(errs) >= 1
        assert any("entity" in e.message for e in errs)

    def test_missing_identity(self):
        data = _minimal_entity()
        del data["identity"]
        errs = validate_schema(data, "ontology")
        assert len(errs) >= 1

    def test_empty_identity(self):
        data = _minimal_entity()
        data["identity"] = {}
        errs = validate_schema(data, "ontology")
        assert len(errs) >= 1

    def test_invalid_entity_name_pattern(self):
        data = _minimal_entity(entity="lowercase_bad")
        errs = validate_schema(data, "ontology")
        assert len(errs) >= 1

    def test_invalid_column_type(self):
        data = _minimal_entity()
        data["identity"]["test_key"]["type"] = "VARCHAR"
        errs = validate_schema(data, "ontology")
        assert len(errs) >= 1

    def test_invalid_semantic_hint(self):
        data = _minimal_entity()
        data["identity"]["test_key"]["semantic"] = "UNKNOWN_HINT"
        errs = validate_schema(data, "ontology")
        assert len(errs) >= 1

    def test_additional_property_at_root(self):
        data = _minimal_entity()
        data["extra"] = "bad"
        errs = validate_schema(data, "ontology")
        assert len(errs) >= 1

    def test_additional_property_on_attribute(self):
        data = _minimal_entity()
        data["identity"]["test_key"]["extra"] = "bad"
        errs = validate_schema(data, "ontology")
        assert len(errs) >= 1

    def test_invalid_references_pattern(self):
        data = _minimal_entity()
        data["identity"]["ref_id"] = {
            "type": "INTEGER",
            "semantic": "RELATION",
            "references": "lower_case",
        }
        errs = validate_schema(data, "ontology")
        assert len(errs) >= 1

    def test_valid_with_events(self):
        data = _minimal_entity()
        data["events"] = {
            "lifecycle": {
                "attributes": {
                    "state": {"type": "STRING", "semantic": "STATE"},
                    "inserted_at": {"type": "STRING", "semantic": "INSERTED_AT"},
                },
            }
        }
        assert validate_schema(data, "ontology") == []

    def test_valid_with_relations(self):
        data = _minimal_entity()
        data["relations"] = {
            "parent": {
                "target": "ParentEntity",
                "cardinality": "MANY_TO_ONE",
            }
        }
        assert validate_schema(data, "ontology") == []


# ---------------------------------------------------------------------------
# Phase 2: Semantic validation
# ---------------------------------------------------------------------------


class TestSemanticValidation:
    def test_valid_passes(self):
        assert _check_entity_rules(_minimal_entity()) == []

    def test_entity_requires_pk(self):
        data = _minimal_entity()
        data["identity"] = {
            "name": {"type": "STRING", "semantic": "IDENTIFIER"},
        }
        errs = _check_entity_rules(data)
        assert any(e.rule == "entity_requires_pk" for e in errs)

    def test_relation_requires_references(self):
        data = _minimal_entity()
        data["identity"]["bad_rel"] = {
            "type": "INTEGER",
            "semantic": "RELATION",
        }
        errs = _check_entity_rules(data)
        assert any(e.rule == "relation_requires_references" for e in errs)

    def test_references_requires_relation(self):
        data = _minimal_entity()
        data["identity"]["bad_ref"] = {
            "type": "STRING",
            "semantic": "ATTRIBUTE",
            "references": "SomeEntity",
        }
        errs = _check_entity_rules(data)
        assert any(e.rule == "references_requires_relation" for e in errs)

    def test_valid_relation_passes(self):
        data = _minimal_entity()
        data["identity"]["parent_key"] = {
            "type": "STRING",
            "semantic": "RELATION",
            "references": "Parent",
        }
        errs = _check_entity_rules(data)
        assert not any(e.rule in ("relation_requires_references", "references_requires_relation") for e in errs)

    def test_history_requires_scd_columns(self):
        data = _minimal_entity()
        data["history"] = {
            "type": "SCD2",
            "valid_from": {"type": "STRING", "semantic": "SCD_FROM"},
        }
        errs = _check_entity_rules(data)
        assert any(e.rule == "history_requires_scd_columns" for e in errs)

    def test_event_requires_inserted_at(self):
        data = _minimal_entity()
        data["events"] = {
            "lifecycle": {
                "attributes": {
                    "state": {"type": "STRING", "semantic": "STATE"},
                },
            }
        }
        errs = _check_entity_rules(data)
        assert any(e.rule == "event_requires_inserted_at" for e in errs)

    def test_relation_via_must_exist(self):
        data = _minimal_entity()
        data["relations"] = {
            "parent": {
                "target": "Parent",
                "via": "nonexistent_key",
                "cardinality": "MANY_TO_ONE",
            }
        }
        errs = _check_entity_rules(data)
        assert any(e.rule == "relation_via_must_exist" for e in errs)

    def test_relation_source_must_exist(self):
        data = _minimal_entity()
        data["relations"] = {
            "events": {
                "target": "Parent",
                "source": "nonexistent_event",
                "cardinality": "MANY_TO_ONE",
            }
        }
        errs = _check_entity_rules(data)
        assert any(e.rule == "relation_source_must_exist" for e in errs)


# ---------------------------------------------------------------------------
# Cross-entity validation
# ---------------------------------------------------------------------------


class TestCrossEntityValidation:
    def test_valid_entities(self, tmp_path):
        from semantic.types import AttributeDef, ColumnType, EntityDef, SemanticHint
        parent = EntityDef(
            name="Parent",
            identity=[AttributeDef("parent_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        )
        child = EntityDef(
            name="Child",
            identity=[
                AttributeDef("child_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY),
                AttributeDef("parent_key", ColumnType.STRING, SemanticHint.RELATION, references="Parent"),
            ],
        )
        errs = validate_all_entities([parent, child])
        assert errs == []

    def test_reference_entity_missing(self, tmp_path):
        from semantic.types import AttributeDef, ColumnType, EntityDef, SemanticHint
        child = EntityDef(
            name="Child",
            identity=[
                AttributeDef("child_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY),
                AttributeDef("parent_key", ColumnType.STRING, SemanticHint.RELATION, references="NonExistent"),
            ],
        )
        errs = validate_all_entities([child])
        assert any(e.rule == "reference_entity_exists" for e in errs)


# ---------------------------------------------------------------------------
# Integration: full pipeline + real ontology files
# ---------------------------------------------------------------------------


class TestIntegration:
    def test_schema_errors_skip_semantic_phase(self, tmp_path):
        """When phase 1 fails, phase 2 should not run."""
        bad = {"entity": "bad_name", "identity": {}}
        p = _write_yaml(tmp_path, bad)
        errs = validate_ontology_yaml(p)
        assert all(e.rule == "schema" for e in errs)
        assert len(errs) >= 1

    def test_valid_file_passes(self, tmp_path):
        p = _write_yaml(tmp_path, _minimal_entity())
        assert validate_ontology_yaml(p) == []

    def test_combined_schema_and_semantic(self, tmp_path):
        """A structurally valid file with semantic issues reports semantic errors."""
        data = {
            "entity": "BadEntity",
            "identity": {
                "name": {"type": "STRING", "semantic": "IDENTIFIER"},
            },
        }
        p = _write_yaml(tmp_path, data)
        errs = validate_ontology_yaml(p)
        assert any(e.rule == "entity_requires_pk" for e in errs)

    @pytest.mark.parametrize(
        "yaml_file",
        sorted(ONTOLOGY_DIR.glob("*.yaml")),
        ids=lambda p: p.name,
    )
    def test_existing_ontology_yamls_pass(self, yaml_file):
        """All checked-in ontology YAMLs must pass validation."""
        errs = validate_ontology_yaml(yaml_file)
        assert errs == [], f"{yaml_file.name}: {[f'[{e.rule}] {e.path}: {e.message}' for e in errs]}"
