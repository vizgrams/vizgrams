# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for core.validation — schema + semantic validation of extractor YAMLs."""

from pathlib import Path

import pytest
import yaml

from core.validation import (
    _check_cross_field_rules,
    _format_path,
    load_schema,
    validate_extractor_yaml,
    validate_schema,
)

EXTRACTORS_DIR = Path(__file__).resolve().parent.parent / "models" / "example" / "extractors"


def _minimal_config(**overrides):
    """Return a minimal valid extractor config dict, with optional overrides."""
    task = {
        "name": "test_task",
        "tool": "test_tool",
        "command": "test_cmd",
        "output": {
            "table": "test_table",
            "write_mode": "UPSERT",
            "primary_keys": ["id"],
            "columns": [
                {"name": "id", "json_path": "$.id"},
            ],
        },
    }
    task.update(overrides)
    return {"tasks": [task]}


def _write_yaml(tmp_path, data, filename="test.yaml"):
    p = tmp_path / filename
    p.write_text(yaml.dump(data))
    return p


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


class TestFormatPath:
    def test_empty(self):
        assert _format_path([]) == "(root)"

    def test_simple(self):
        assert _format_path(["tasks", 0, "name"]) == "tasks[0].name"

    def test_nested_array(self):
        assert _format_path(["tasks", 1, "output", "columns", 2, "name"]) == (
            "tasks[1].output.columns[2].name"
        )


class TestLoadSchema:
    def test_loads_extractor_schema(self):
        schema = load_schema("extractor")
        assert schema["title"] == "Extractor Config"

    def test_missing_schema_raises(self):
        with pytest.raises(FileNotFoundError, match="Schema not found"):
            load_schema("nonexistent_schema_xyz")


# ---------------------------------------------------------------------------
# Phase 1: Structural (JSON Schema)
# ---------------------------------------------------------------------------


class TestSchemaValidation:
    def test_valid_minimal(self):
        assert validate_schema(_minimal_config(), "extractor") == []

    def test_missing_tasks_key(self):
        errs = validate_schema({}, "extractor")
        assert len(errs) >= 1
        assert any("tasks" in e.message and e.rule == "schema" for e in errs)

    def test_empty_tasks_list(self):
        errs = validate_schema({"tasks": []}, "extractor")
        assert len(errs) == 1
        assert "non-empty" in errs[0].message or "minItems" in errs[0].message

    def test_missing_required_task_fields(self):
        # Task missing tool, command (output enforced semantically, not in schema required)
        errs = validate_schema({"tasks": [{"name": "bad"}]}, "extractor")
        missing = {e.message for e in errs}
        assert any("tool" in m for m in missing)
        assert any("command" in m for m in missing)

    def test_invalid_write_mode_enum(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["write_mode"] = "SNAPSHOT"
        errs = validate_schema(cfg, "extractor")
        assert len(errs) == 1
        assert "SNAPSHOT" in errs[0].message

    def test_invalid_column_type_enum(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["columns"][0]["type"] = "VARCHAR"
        errs = validate_schema(cfg, "extractor")
        assert len(errs) == 1
        assert "VARCHAR" in errs[0].message

    def test_additional_property_on_task(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["unknown_field"] = "oops"
        errs = validate_schema(cfg, "extractor")
        assert len(errs) >= 1
        assert any("additional" in e.message.lower() or "unknown_field" in e.message for e in errs)

    def test_additional_property_on_output(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["extra"] = True
        errs = validate_schema(cfg, "extractor")
        assert len(errs) >= 1

    def test_additional_property_on_column(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["columns"][0]["desc"] = "bad"
        errs = validate_schema(cfg, "extractor")
        assert len(errs) >= 1

    def test_invalid_name_format(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["name"] = "BadName"
        errs = validate_schema(cfg, "extractor")
        assert len(errs) >= 1
        assert any(e.rule == "schema" for e in errs)

    def test_valid_since_format(self):
        cfg = _minimal_config(since="2025-10-01")
        assert validate_schema(cfg, "extractor") == []

    def test_invalid_since_format(self):
        cfg = _minimal_config(since="October 2025")
        errs = validate_schema(cfg, "extractor")
        assert len(errs) >= 1

    def test_params_with_various_types(self):
        cfg = _minimal_config(params={"key": "*", "org": "TestOrg", "count": 5, "flag": True})
        assert validate_schema(cfg, "extractor") == []

    def test_valid_incremental(self):
        cfg = _minimal_config(incremental=True)
        assert validate_schema(cfg, "extractor") == []

    def test_valid_context(self):
        cfg = _minimal_config(context={"board_id": "board_id"})
        assert validate_schema(cfg, "extractor") == []

    def test_valid_row_source(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["row_source"] = {
            "mode": "EXPLODE",
            "json_path": "$.items",
            "inherit": {"parent_id": "$.id"},
        }
        assert validate_schema(cfg, "extractor") == []

    def test_invalid_row_source_mode(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["row_source"] = {"mode": "FANOUT"}
        errs = validate_schema(cfg, "extractor")
        assert len(errs) >= 1
        assert any("FANOUT" in e.message for e in errs)

    def test_row_source_rejects_additional_properties(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["row_source"] = {"mode": "SINGLE", "extra": True}
        errs = validate_schema(cfg, "extractor")
        assert len(errs) >= 1

    def test_valid_write_modes(self):
        for mode in ["APPEND", "UPSERT", "REPLACE"]:
            cfg = _minimal_config()
            cfg["tasks"][0]["output"]["write_mode"] = mode
            if mode != "UPSERT":
                cfg["tasks"][0]["output"]["primary_keys"] = []
            assert validate_schema(cfg, "extractor") == [], f"Failed for {mode}"

    def test_valid_outputs_plural(self):
        cfg = _minimal_config()
        output = cfg["tasks"][0].pop("output")
        cfg["tasks"][0]["outputs"] = [output]
        assert validate_schema(cfg, "extractor") == []

    def test_valid_outputs_multiple(self):
        cfg = _minimal_config()
        output = cfg["tasks"][0].pop("output")
        second = {
            "table": "another",
            "write_mode": "APPEND",
            "columns": [{"name": "val", "json_path": "$.val"}],
        }
        cfg["tasks"][0]["outputs"] = [output, second]
        assert validate_schema(cfg, "extractor") == []


# ---------------------------------------------------------------------------
# Phase 2: Semantic (cross-field)
# ---------------------------------------------------------------------------


class TestSemanticValidation:
    def test_valid_config_passes(self):
        assert _check_cross_field_rules(_minimal_config()) == []

    def test_primary_key_nonexistent_column(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["primary_keys"] = ["nonexistent"]
        errs = _check_cross_field_rules(cfg)
        assert len(errs) == 1
        assert errs[0].rule == "primary_key_ref"
        assert "nonexistent" in errs[0].message

    def test_primary_key_from_context_valid(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["context"] = {"org": "org"}
        cfg["tasks"][0]["output"]["primary_keys"] = ["org", "id"]
        errs = _check_cross_field_rules(cfg)
        assert errs == []

    def test_primary_key_from_inherit_valid(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["write_mode"] = "APPEND"
        cfg["tasks"][0]["output"]["primary_keys"] = ["parent_id", "id"]
        cfg["tasks"][0]["output"]["row_source"] = {
            "mode": "EXPLODE",
            "json_path": "$.items",
            "inherit": {"parent_id": "$.id"},
        }
        errs = _check_cross_field_rules(cfg)
        assert not any(e.rule == "primary_key_ref" for e in errs)

    def test_duplicate_column_names(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["columns"] = [
            {"name": "id", "json_path": "$.id"},
            {"name": "id", "json_path": "$.id2"},
        ]
        errs = _check_cross_field_rules(cfg)
        assert len(errs) == 1
        assert errs[0].rule == "unique_column_name"

    def test_duplicate_task_names(self):
        cfg = _minimal_config()
        cfg["tasks"].append(cfg["tasks"][0].copy())
        errs = _check_cross_field_rules(cfg)
        assert len(errs) == 1
        assert errs[0].rule == "unique_task_name"

    def test_json_path_missing_dollar_dot(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["columns"] = [
            {"name": "id", "json_path": "id"},
        ]
        errs = _check_cross_field_rules(cfg)
        assert len(errs) == 1
        assert errs[0].rule == "json_path_format"

    def test_upsert_without_primary_keys_error(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["write_mode"] = "UPSERT"
        cfg["tasks"][0]["output"]["primary_keys"] = []
        errs = _check_cross_field_rules(cfg)
        assert len(errs) == 1
        assert errs[0].rule == "upsert_primary_keys"

    def test_append_without_primary_keys_ok(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["write_mode"] = "APPEND"
        cfg["tasks"][0]["output"]["primary_keys"] = []
        errs = _check_cross_field_rules(cfg)
        assert errs == []

    def test_replace_without_primary_keys_ok(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["write_mode"] = "REPLACE"
        cfg["tasks"][0]["output"]["primary_keys"] = []
        errs = _check_cross_field_rules(cfg)
        assert errs == []

    def test_explode_requires_json_path(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["row_source"] = {"mode": "EXPLODE"}
        errs = _check_cross_field_rules(cfg)
        assert len(errs) == 1
        assert errs[0].rule == "explode_requires_json_path"

    def test_explode_with_json_path_ok(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["row_source"] = {
            "mode": "EXPLODE",
            "json_path": "$.items",
        }
        errs = _check_cross_field_rules(cfg)
        assert not any(e.rule == "explode_requires_json_path" for e in errs)

    def test_inherit_json_path_format(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["row_source"] = {
            "mode": "EXPLODE",
            "json_path": "$.items",
            "inherit": {"bad_col": "no_dollar_dot"},
        }
        errs = _check_cross_field_rules(cfg)
        assert any(e.rule == "inherit_json_path_format" for e in errs)

    def test_inherit_column_conflict(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["row_source"] = {
            "mode": "EXPLODE",
            "json_path": "$.items",
            "inherit": {"id": "$.parent_id"},  # 'id' conflicts with column name
        }
        errs = _check_cross_field_rules(cfg)
        assert any(e.rule == "inherit_column_conflict" for e in errs)

    def test_inherit_no_conflict_ok(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["row_source"] = {
            "mode": "EXPLODE",
            "json_path": "$.items",
            "inherit": {"parent_id": "$.id"},
        }
        errs = _check_cross_field_rules(cfg)
        assert not any(e.rule == "inherit_column_conflict" for e in errs)

    def test_output_or_outputs_both_present(self):
        cfg = _minimal_config()
        cfg["tasks"][0]["outputs"] = [cfg["tasks"][0]["output"]]
        errs = _check_cross_field_rules(cfg)
        assert any(e.rule == "output_or_outputs" for e in errs)

    def test_output_or_outputs_neither_present(self):
        cfg = _minimal_config()
        del cfg["tasks"][0]["output"]
        errs = _check_cross_field_rules(cfg)
        assert any(e.rule == "output_or_outputs" for e in errs)

    def test_outputs_plural_valid(self):
        cfg = _minimal_config()
        output = cfg["tasks"][0].pop("output")
        cfg["tasks"][0]["outputs"] = [output]
        errs = _check_cross_field_rules(cfg)
        assert errs == []

    def test_outputs_plural_checks_each_output(self):
        """Semantic checks should apply to each output in the list."""
        cfg = _minimal_config()
        output = cfg["tasks"][0].pop("output")
        bad_output = {
            "table": "bad_table",
            "write_mode": "UPSERT",
            "primary_keys": ["missing_col"],
            "columns": [{"name": "id", "json_path": "$.id"}],
        }
        cfg["tasks"][0]["outputs"] = [output, bad_output]
        errs = _check_cross_field_rules(cfg)
        assert any(e.rule == "primary_key_ref" and "outputs[1]" in e.path for e in errs)


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------


class TestIntegration:
    def test_schema_errors_skip_semantic_phase(self, tmp_path):
        """When phase 1 fails, phase 2 should not run."""
        bad = {"tasks": [{"name": "bad"}]}
        p = _write_yaml(tmp_path, bad)
        errs = validate_extractor_yaml(p)
        # All errors should be 'schema' rule (no semantic rules)
        assert all(e.rule == "schema" for e in errs)
        assert len(errs) >= 1

    @pytest.mark.parametrize(
        "yaml_file",
        sorted(EXTRACTORS_DIR.glob("extractor_*.yaml")),
        ids=lambda p: p.name,
    )
    def test_existing_extractor_yamls_pass(self, yaml_file):
        """All checked-in extractor YAMLs must pass validation."""
        errs = validate_extractor_yaml(yaml_file)
        assert errs == [], f"{yaml_file.name}: {[str(e) for e in errs]}"

    def test_valid_file_passes(self, tmp_path):
        p = _write_yaml(tmp_path, _minimal_config())
        assert validate_extractor_yaml(p) == []

    def test_combined_schema_and_semantic(self, tmp_path):
        """A structurally valid file with semantic issues reports semantic errors."""
        cfg = _minimal_config()
        cfg["tasks"][0]["output"]["primary_keys"] = ["missing_col"]
        p = _write_yaml(tmp_path, cfg)
        errs = validate_extractor_yaml(p)
        assert len(errs) == 1
        assert errs[0].rule == "primary_key_ref"
