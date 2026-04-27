# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the model registry DB helpers in core/vizgrams_db.py
and the DB-backed load_registry / save_registry in core/registry.py."""

import json

import pytest
import yaml

from core.vizgrams_db import (
    delete_model_from_db,
    get_model_access_rules,
    load_registry_from_db,
    seed_model_registry,
    set_model_access_rules,
    upsert_model_in_db,
)


@pytest.fixture
def db(tmp_path):
    return tmp_path / "test_registry.db"


# ---------------------------------------------------------------------------
# load_registry_from_db — empty state
# ---------------------------------------------------------------------------

class TestLoadRegistryFromDb:
    def test_empty_db_returns_empty_dict(self, db):
        assert load_registry_from_db(db_path=db) == {}

    def test_returns_upserted_model(self, db):
        upsert_model_in_db("alpha", {
            "display_name": "Alpha",
            "description": "A test model",
            "owner": "test",
            "status": "active",
            "tags": ["eng"],
        }, db_path=db)
        reg = load_registry_from_db(db_path=db)
        assert "alpha" in reg
        assert reg["alpha"]["display_name"] == "Alpha"
        assert reg["alpha"]["tags"] == ["eng"]
        assert reg["alpha"]["status"] == "active"

    def test_returns_multiple_models(self, db):
        for name in ("alpha", "beta", "gamma"):
            upsert_model_in_db(name, {"display_name": name.title(), "status": "experimental"}, db_path=db)
        reg = load_registry_from_db(db_path=db)
        assert set(reg.keys()) == {"alpha", "beta", "gamma"}


# ---------------------------------------------------------------------------
# upsert_model_in_db — create and update
# ---------------------------------------------------------------------------

class TestUpsertModelInDb:
    def test_insert_then_update(self, db):
        upsert_model_in_db("m1", {"display_name": "Original", "status": "experimental"}, db_path=db)
        upsert_model_in_db("m1", {"display_name": "Updated", "status": "active"}, db_path=db)
        reg = load_registry_from_db(db_path=db)
        assert reg["m1"]["display_name"] == "Updated"
        assert reg["m1"]["status"] == "active"

    def test_upsert_does_not_overwrite_access_rules(self, db):
        upsert_model_in_db("m1", {"display_name": "M1"}, db_path=db)
        set_model_access_rules("m1", [{"email": "*", "role": "VIEWER"}], db_path=db)
        # Upsert again — should leave access_rules intact
        upsert_model_in_db("m1", {"display_name": "M1 Updated"}, db_path=db)
        rules = get_model_access_rules("m1", db_path=db)
        assert rules == [{"email": "*", "role": "VIEWER"}]

    def test_defaults_used_when_fields_absent(self, db):
        upsert_model_in_db("m1", {"display_name": "Minimal"}, db_path=db)
        reg = load_registry_from_db(db_path=db)
        assert reg["m1"]["description"] == ""
        assert reg["m1"]["owner"] == ""
        assert reg["m1"]["status"] == "experimental"
        assert reg["m1"]["tags"] == []


# ---------------------------------------------------------------------------
# delete_model_from_db
# ---------------------------------------------------------------------------

class TestDeleteModelFromDb:
    def test_delete_removes_model(self, db):
        upsert_model_in_db("alpha", {"display_name": "Alpha"}, db_path=db)
        delete_model_from_db("alpha", db_path=db)
        assert "alpha" not in load_registry_from_db(db_path=db)

    def test_delete_nonexistent_is_silent(self, db):
        delete_model_from_db("does_not_exist", db_path=db)  # no error

    def test_delete_only_removes_target(self, db):
        upsert_model_in_db("alpha", {"display_name": "Alpha"}, db_path=db)
        upsert_model_in_db("beta", {"display_name": "Beta"}, db_path=db)
        delete_model_from_db("alpha", db_path=db)
        reg = load_registry_from_db(db_path=db)
        assert "beta" in reg
        assert "alpha" not in reg


# ---------------------------------------------------------------------------
# get_model_access_rules / set_model_access_rules
# ---------------------------------------------------------------------------

class TestAccessRules:
    def test_returns_none_when_not_set(self, db):
        upsert_model_in_db("m1", {"display_name": "M1"}, db_path=db)
        assert get_model_access_rules("m1", db_path=db) is None

    def test_returns_none_for_unknown_model(self, db):
        assert get_model_access_rules("unknown", db_path=db) is None

    def test_set_and_get_rules(self, db):
        upsert_model_in_db("m1", {"display_name": "M1"}, db_path=db)
        rules = [
            {"email": "admin@example.com", "role": "ADMIN"},
            {"email": "*@example.com", "role": "VIEWER"},
        ]
        set_model_access_rules("m1", rules, db_path=db)
        assert get_model_access_rules("m1", db_path=db) == rules

    def test_set_empty_list(self, db):
        upsert_model_in_db("m1", {"display_name": "M1"}, db_path=db)
        set_model_access_rules("m1", [], db_path=db)
        result = get_model_access_rules("m1", db_path=db)
        assert result == []

    def test_clear_rules_with_none(self, db):
        upsert_model_in_db("m1", {"display_name": "M1"}, db_path=db)
        set_model_access_rules("m1", [{"email": "*", "role": "VIEWER"}], db_path=db)
        set_model_access_rules("m1", None, db_path=db)
        assert get_model_access_rules("m1", db_path=db) is None

    def test_overwrite_rules(self, db):
        upsert_model_in_db("m1", {"display_name": "M1"}, db_path=db)
        set_model_access_rules("m1", [{"email": "*", "role": "VIEWER"}], db_path=db)
        set_model_access_rules("m1", [{"email": "*", "role": "ADMIN"}], db_path=db)
        assert get_model_access_rules("m1", db_path=db) == [{"email": "*", "role": "ADMIN"}]


# ---------------------------------------------------------------------------
# seed_model_registry
# ---------------------------------------------------------------------------

class TestSeedModelRegistry:
    def _write_registry(self, models_dir, models: dict) -> None:
        (models_dir / "registry.yaml").write_text(
            yaml.dump({"models": models}, default_flow_style=False)
        )

    def _write_config(self, models_dir, model_id: str, access=None) -> None:
        model_dir = models_dir / model_id
        model_dir.mkdir(parents=True, exist_ok=True)
        config = {}
        if access is not None:
            config["access"] = access
        (model_dir / "config.yaml").write_text(yaml.dump(config))

    def test_seeds_models_from_registry_yaml(self, tmp_path, db):
        self._write_registry(tmp_path, {
            "alpha": {"display_name": "Alpha", "status": "active", "tags": ["eng"]},
            "beta":  {"display_name": "Beta",  "status": "experimental", "tags": []},
        })
        count = seed_model_registry(tmp_path, db_path=db)
        assert count == 2
        reg = load_registry_from_db(db_path=db)
        assert "alpha" in reg
        assert "beta" in reg
        assert reg["alpha"]["display_name"] == "Alpha"
        assert reg["alpha"]["tags"] == ["eng"]

    def test_idempotent_second_seed_skips_existing(self, tmp_path, db):
        self._write_registry(tmp_path, {
            "alpha": {"display_name": "Alpha", "status": "active"},
        })
        first = seed_model_registry(tmp_path, db_path=db)
        second = seed_model_registry(tmp_path, db_path=db)
        assert first == 1
        assert second == 0

    def test_seeds_access_rules_from_config_yaml(self, tmp_path, db):
        self._write_registry(tmp_path, {
            "alpha": {"display_name": "Alpha", "status": "active"},
        })
        self._write_config(tmp_path, "alpha", access=[
            {"email": "*@example.com", "role": "VIEWER"},
        ])
        seed_model_registry(tmp_path, db_path=db)
        rules = get_model_access_rules("alpha", db_path=db)
        assert rules == [{"email": "*@example.com", "role": "VIEWER"}]

    def test_no_access_in_config_leaves_rules_null(self, tmp_path, db):
        self._write_registry(tmp_path, {
            "alpha": {"display_name": "Alpha", "status": "active"},
        })
        self._write_config(tmp_path, "alpha")  # config.yaml with no access block
        seed_model_registry(tmp_path, db_path=db)
        assert get_model_access_rules("alpha", db_path=db) is None

    def test_no_registry_yaml_returns_zero(self, tmp_path, db):
        count = seed_model_registry(tmp_path, db_path=db)
        assert count == 0

    def test_empty_registry_returns_zero(self, tmp_path, db):
        self._write_registry(tmp_path, {})
        count = seed_model_registry(tmp_path, db_path=db)
        assert count == 0


# ---------------------------------------------------------------------------
# load_registry / save_registry DB integration (core/registry.py)
# ---------------------------------------------------------------------------

class TestRegistryDbIntegration:
    """Verify that load_registry reads from DB and save_registry writes to DB."""

    def test_load_registry_uses_db_when_populated(self, tmp_path, monkeypatch, db):
        monkeypatch.setattr(
            "core.vizgrams_db.load_registry_from_db",
            lambda db_path=None: {"mymodel": {"display_name": "My Model", "status": "active", "tags": []}},
        )
        from core.registry import load_registry
        reg = load_registry(tmp_path)
        assert "mymodel" in reg

    def test_load_registry_falls_back_to_yaml_when_db_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "core.vizgrams_db.load_registry_from_db",
            lambda db_path=None: {},
        )
        yaml_path = tmp_path / "registry.yaml"
        yaml_path.write_text(yaml.dump({"models": {"yamlmodel": {"display_name": "YAML Model", "status": "active", "tags": []}}}))
        from core.registry import load_registry
        reg = load_registry(tmp_path)
        assert "yamlmodel" in reg

    def test_save_registry_calls_upsert(self, tmp_path, monkeypatch):
        upserted = {}

        def fake_upsert(model_id, fields, db_path=None):
            upserted[model_id] = fields

        monkeypatch.setattr("core.vizgrams_db.upsert_model_in_db", fake_upsert)
        from core.registry import save_registry
        save_registry(tmp_path, {
            "alpha": {"display_name": "Alpha", "status": "active", "tags": []},
        })
        assert "alpha" in upserted
