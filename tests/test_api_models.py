# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for api/services/model_service.py."""

from pathlib import Path

import pytest
import yaml

from api.services.model_service import (
    archive_model,
    create_model,
    delete_model,
    get_access_rules,
    get_model,
    list_models,
    set_access_rules,
    set_active,
    update_model,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolate_registry_db(monkeypatch):
    """Prevent model-service tests from touching the real api.db models table.

    load_registry falls back to registry.yaml when load_registry_from_db returns
    an empty dict, which is exactly what these tests exercise.
    """
    monkeypatch.setattr("core.vizgrams_db.load_registry_from_db", lambda db_path=None: {})
    monkeypatch.setattr("core.vizgrams_db.upsert_model_in_db", lambda *a, **kw: None)
    monkeypatch.setattr("core.vizgrams_db.delete_model_from_db", lambda *a, **kw: None)
    monkeypatch.setattr("core.vizgrams_db.get_model_access_rules", lambda *a, **kw: None)
    monkeypatch.setattr("core.vizgrams_db.set_model_access_rules", lambda *a, **kw: None)


@pytest.fixture
def base_dir(tmp_path):
    """A base_dir with an empty models/ directory."""
    (tmp_path / "models").mkdir()
    return tmp_path


@pytest.fixture
def models_dir(base_dir):
    """The models directory under base_dir."""
    return base_dir / "models"


def _write_registry(models_dir: Path, models: dict) -> None:
    (models_dir / "registry.yaml").write_text(
        yaml.dump({"models": models}, default_flow_style=False)
    )


def _make_model_dir(models_dir: Path, name: str) -> Path:
    """Create a scaffolded model directory (without going through create_model)."""
    model_dir = models_dir / name
    for sub in ("extractors", "ontology", "mappers", "features", "queries", "input_data", "data"):
        (model_dir / sub).mkdir(parents=True, exist_ok=True)
    return model_dir


# ---------------------------------------------------------------------------
# list_models
# ---------------------------------------------------------------------------

def test_list_models_empty(models_dir, base_dir):
    _write_registry(models_dir, {})
    assert list_models(models_dir, base_dir) == []


def test_list_models_returns_all(models_dir, base_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": []},
        "beta":  {"display_name": "Beta",  "status": "experimental", "tags": []},
    })
    _make_model_dir(models_dir, "alpha")
    _make_model_dir(models_dir, "beta")
    result = list_models(models_dir, base_dir)
    names = {m["name"] for m in result}
    assert names == {"alpha", "beta"}


def test_list_models_filters_by_status(models_dir, base_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": []},
        "beta":  {"display_name": "Beta",  "status": "archived", "tags": []},
    })
    result = list_models(models_dir, base_dir, status="active")
    assert len(result) == 1
    assert result[0]["name"] == "alpha"


def test_list_models_filters_by_tag(models_dir, base_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": ["eng", "core"]},
        "beta":  {"display_name": "Beta",  "status": "active", "tags": ["eng"]},
    })
    result = list_models(models_dir, base_dir, tags=["core"])
    assert len(result) == 1
    assert result[0]["name"] == "alpha"


def test_list_models_marks_active(models_dir, base_dir, monkeypatch):
    # is_active now comes from the DB; override the fixture's no-op stub to
    # return the model with is_active=True so list_models picks it up.
    monkeypatch.setattr(
        "core.vizgrams_db.load_registry_from_db",
        lambda db_path=None: {
            "alpha": {"display_name": "Alpha", "status": "active", "tags": [], "is_active": True},
        },
    )
    result = list_models(models_dir, base_dir)
    assert result[0]["is_active"] is True


def test_list_models_no_registry_returns_empty(models_dir, base_dir):
    # registry.yaml absent
    result = list_models(models_dir, base_dir)
    assert result == []


# ---------------------------------------------------------------------------
# get_model
# ---------------------------------------------------------------------------

def test_get_model_returns_expected_fields(models_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": [], "description": "test"},
    })
    _make_model_dir(models_dir, "alpha")
    result = get_model(models_dir, "alpha")
    assert result["name"] == "alpha"
    assert "config" in result
    assert "database" in result
    assert "audit" in result


def test_get_model_raises_key_error_when_not_found(models_dir):
    _write_registry(models_dir, {})
    with pytest.raises(KeyError):
        get_model(models_dir, "nonexistent")


# ---------------------------------------------------------------------------
# create_model
# ---------------------------------------------------------------------------

def test_create_model_scaffolds_directories(models_dir, base_dir):
    data = {
        "name": "newmodel",
        "display_name": "New Model",
        "description": "A test model",
        "owner": "test",
    }
    _result = create_model(models_dir, base_dir, data)
    model_dir = models_dir / "newmodel"
    assert model_dir.is_dir()
    for sub in ("extractors", "input_data", "data"):
        assert (model_dir / sub).is_dir(), f"Missing subdir: {sub}"


def test_create_model_adds_to_registry(models_dir, base_dir):
    data = {
        "name": "newmodel",
        "display_name": "New Model",
        "description": "desc",
        "owner": "test",
    }
    create_model(models_dir, base_dir, data)
    registry_path = models_dir / "registry.yaml"
    assert registry_path.is_file()
    with open(registry_path) as f:
        reg = yaml.safe_load(f)
    assert "newmodel" in reg["models"]


def test_create_model_writes_config_yaml(models_dir, base_dir):
    data = {"name": "newmodel", "display_name": "NM", "description": "", "owner": "test"}
    create_model(models_dir, base_dir, data)
    config = models_dir / "newmodel" / "config.yaml"
    assert config.is_file()


def test_create_model_raises_on_duplicate(models_dir, base_dir):
    data = {"name": "newmodel", "display_name": "NM", "description": "", "owner": "test"}
    create_model(models_dir, base_dir, data)
    with pytest.raises((ValueError, FileExistsError)):
        create_model(models_dir, base_dir, data)


def test_create_model_set_active(models_dir, base_dir):
    data = {
        "name": "newmodel",
        "display_name": "NM",
        "description": "",
        "owner": "test",
        "set_active": True,
    }
    create_model(models_dir, base_dir, data)
    ctx = (base_dir / ".vz_context").read_text().strip()
    assert ctx == "newmodel"


# ---------------------------------------------------------------------------
# update_model
# ---------------------------------------------------------------------------

def test_update_model_updates_description(models_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": [], "description": "old"},
    })
    _make_model_dir(models_dir, "alpha")
    update_model(models_dir, "alpha", {"description": "new description"})
    reg_path = models_dir / "registry.yaml"
    with open(reg_path) as f:
        reg = yaml.safe_load(f)
    assert reg["models"]["alpha"]["description"] == "new description"


def test_update_model_raises_key_error_when_not_found(models_dir):
    _write_registry(models_dir, {})
    with pytest.raises(KeyError):
        update_model(models_dir, "nonexistent", {"description": "x"})


# ---------------------------------------------------------------------------
# archive_model
# ---------------------------------------------------------------------------

def test_archive_model_sets_status(models_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": []},
    })
    _make_model_dir(models_dir, "alpha")
    archive_model(models_dir, "alpha")
    with open(models_dir / "registry.yaml") as f:
        reg = yaml.safe_load(f)
    assert reg["models"]["alpha"]["status"] == "archived"


def test_archive_model_raises_if_already_archived(models_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "archived", "tags": []},
    })
    _make_model_dir(models_dir, "alpha")
    with pytest.raises(ValueError, match="already archived"):
        archive_model(models_dir, "alpha")


def test_archive_model_raises_key_error_when_not_found(models_dir):
    _write_registry(models_dir, {})
    with pytest.raises(KeyError):
        archive_model(models_dir, "nonexistent")


# ---------------------------------------------------------------------------
# set_active
# ---------------------------------------------------------------------------

def test_set_active_writes_wt_context(models_dir, base_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": []},
    })
    set_active(models_dir, base_dir, "alpha")
    assert (base_dir / ".vz_context").read_text().strip() == "alpha"


def test_set_active_raises_key_error_when_not_found(models_dir, base_dir):
    _write_registry(models_dir, {})
    with pytest.raises(KeyError):
        set_active(models_dir, base_dir, "nonexistent")


# ---------------------------------------------------------------------------
# delete_model
# ---------------------------------------------------------------------------

def test_delete_model_removes_from_registry(models_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": []},
    })
    _make_model_dir(models_dir, "alpha")
    delete_model(models_dir, "alpha")
    with open(models_dir / "registry.yaml") as f:
        reg = yaml.safe_load(f)
    assert "alpha" not in reg["models"]


def test_delete_model_raises_key_error_when_not_found(models_dir):
    _write_registry(models_dir, {})
    with pytest.raises(KeyError):
        delete_model(models_dir, "nonexistent")


def test_delete_model_removes_files_when_flag_set(models_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": []},
    })
    _make_model_dir(models_dir, "alpha")
    delete_model(models_dir, "alpha", delete_files=True)
    assert not (models_dir / "alpha").exists()


# ---------------------------------------------------------------------------
# get_access_rules / set_access_rules
# ---------------------------------------------------------------------------

def test_get_access_rules_returns_none_when_not_set(monkeypatch):
    # isolate_registry_db already patches get_model_access_rules to return None
    result = get_access_rules("alpha")
    assert result is None


def test_set_access_rules_raises_key_error_when_model_not_found(models_dir):
    _write_registry(models_dir, {})
    with pytest.raises(KeyError):
        set_access_rules(models_dir, "nonexistent", [])


def test_set_access_rules_returns_provided_rules(models_dir, monkeypatch):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": []},
    })
    rules = [{"email": "*@example.com", "role": "VIEWER"}]
    result = set_access_rules(models_dir, "alpha", rules)
    assert result == rules


def test_set_access_rules_accepts_none(models_dir):
    _write_registry(models_dir, {
        "alpha": {"display_name": "Alpha", "status": "active", "tags": []},
    })
    result = set_access_rules(models_dir, "alpha", None)
    assert result is None


# ---------------------------------------------------------------------------
# AuditEntry schema — detail accepts str or dict
# ---------------------------------------------------------------------------

def test_audit_entry_detail_accepts_string():
    from api.schemas.model import AuditEntry
    e = AuditEntry(timestamp="2026-01-01T00:00:00Z", event="test", actor="user", detail="some text")
    assert e.detail == "some text"


def test_audit_entry_detail_accepts_dict():
    from api.schemas.model import AuditEntry
    d = {"consistent_at": "2026-01-01T00:00:00Z", "clickhouse_databases": ["iagai"]}
    e = AuditEntry(timestamp="2026-01-01T00:00:00Z", event="backup", actor="system", detail=d)
    assert e.detail == d
