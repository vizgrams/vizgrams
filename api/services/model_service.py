# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Model service: CRUD operations on the model registry and directory scaffold."""

import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import yaml

from core.registry import (
    append_audit,
    current_actor,
    load_registry,
    read_audit,
    save_registry,
)

# Subdirectories created by model create
_SCAFFOLD_DIRS = ["extractors", "input_data", "data"]

_CREDENTIAL_KEYS = {"api_token", "token", "password", "secret", "api_key"}


def list_models(
    models_dir: Path,
    base_dir: Path,
    status: str | None = None,
    tags: list[str] | None = None,
) -> list[dict]:
    registry = load_registry(models_dir)
    result = []
    for name, meta in registry.items():
        if status and meta.get("status") != status:
            continue
        if tags:
            model_tags = set(meta.get("tags") or [])
            if not set(tags).issubset(model_tags):
                continue
        result.append({"is_active": False, **meta, "name": name})
    return result


def get_model(models_dir: Path, model_name: str, full_audit: bool = False) -> dict:
    registry = load_registry(models_dir)
    if model_name not in registry:
        raise KeyError(f"Model '{model_name}' not found in registry.")
    meta = registry[model_name]
    model_dir = models_dir / model_name

    audit_entries = read_audit(model_dir)
    audit = audit_entries if full_audit else audit_entries[-10:]

    from core.vizgrams_db import get_model_access_rules
    access_rules = get_model_access_rules(model_name)

    return {
        "name": model_name,
        "is_active": meta.get("is_active", False),
        **meta,
        "config": _get_config_summary(model_dir),
        "database": _get_db_stats(model_dir),
        "audit": audit,
        "access_rules": access_rules,
    }


def create_model(models_dir: Path, base_dir: Path, data: dict) -> dict:
    """Scaffold a new model directory, add registry entry, write audit event."""
    name = data["name"]
    registry = load_registry(models_dir)
    if name in registry:
        status = registry[name].get("status", "unknown")
        raise ValueError(
            f"Model '{name}' already exists (status: {status}). "
            "Archive or delete it before recreating."
        )

    model_dir = models_dir / name
    if model_dir.exists():
        # Orphaned directory — model was removed from registry without deleting files.
        # Remove it so the create can proceed cleanly.
        shutil.rmtree(model_dir)

    # Create directories
    for subdir in _SCAFFOLD_DIRS:
        (model_dir / subdir).mkdir(parents=True, exist_ok=True)

    # Write minimal config.yaml
    config_path = model_dir / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump({"tools": {}}, f, default_flow_style=False)

    # Add to registry
    registry[name] = {
        "display_name": data["display_name"],
        "description": data["description"],
        "owner": data["owner"],
        "created_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": data.get("status", "experimental"),
        "tags": data.get("tags", []),
    }
    save_registry(models_dir, registry)

    # Write first audit event
    append_audit(model_dir, "model_created", "Created via API", actor=current_actor())

    # Optionally set as active context
    if data.get("set_active"):
        (base_dir / ".vz_context").write_text(name + "\n")

    return get_model(models_dir, name)


def update_model(models_dir: Path, model_name: str, data: dict) -> dict:
    """Update mutable registry fields for a model."""
    registry = load_registry(models_dir)
    if model_name not in registry:
        raise KeyError(f"Model '{model_name}' not found in registry.")

    mutable = {"display_name", "description", "owner", "tags"}
    for key, val in data.items():
        if key in mutable and val is not None:
            registry[model_name][key] = val
    save_registry(models_dir, registry)
    return get_model(models_dir, model_name)


def archive_model(models_dir: Path, model_name: str, reason: str | None = None) -> dict:
    """Set model status to archived and write audit event."""
    registry = load_registry(models_dir)
    if model_name not in registry:
        raise KeyError(f"Model '{model_name}' not found in registry.")
    if registry[model_name].get("status") == "archived":
        raise ValueError(f"Model '{model_name}' is already archived.")

    registry[model_name]["status"] = "archived"
    save_registry(models_dir, registry)

    detail = reason or "Archived via API"
    model_dir = models_dir / model_name
    append_audit(model_dir, "model_archived", detail, actor=current_actor())
    return get_model(models_dir, model_name)


def set_active(models_dir: Path, base_dir: Path, model_name: str) -> str:
    """Set the active model in the DB (clears all others) and update .vz_context."""
    registry = load_registry(models_dir)
    if model_name not in registry:
        raise KeyError(f"Model '{model_name}' not found in registry.")
    from core.vizgrams_db import set_model_active
    set_model_active(model_name)
    # Keep .vz_context in sync for CLI tools
    (base_dir / ".vz_context").write_text(model_name + "\n")
    return model_name


def delete_model(models_dir: Path, model_name: str, delete_files: bool = False) -> None:
    """Remove model from registry; optionally delete its directory."""
    registry = load_registry(models_dir)
    if model_name not in registry:
        raise KeyError(f"Model '{model_name}' not found in registry.")
    del registry[model_name]
    save_registry(models_dir, registry)

    from core.vizgrams_db import delete_model_from_db
    delete_model_from_db(model_name)

    if delete_files:
        model_dir = models_dir / model_name
        if model_dir.exists():
            shutil.rmtree(model_dir)


def get_access_rules(model_name: str) -> list[dict] | None:
    """Return DB access rules for a model, or None if using config.yaml fallback."""
    from core.vizgrams_db import get_model_access_rules
    return get_model_access_rules(model_name)


def set_access_rules(models_dir: Path, model_name: str, rules: list[dict] | None) -> list[dict] | None:
    """Set (or clear) DB access rules for a model. Returns the new rules."""
    registry = load_registry(models_dir)
    if model_name not in registry:
        raise KeyError(f"Model '{model_name}' not found in registry.")
    from core.vizgrams_db import set_model_access_rules
    set_model_access_rules(model_name, rules)
    return rules


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_config_summary(model_dir: Path) -> dict | None:
    config_path = model_dir / "config.yaml"
    if not config_path.is_file():
        return None
    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}
    tools_block = raw.get("tools", {})
    return {
        "tools_enabled": [
            name for name, cfg in tools_block.items()
            if isinstance(cfg, dict) and cfg.get("enabled", False)
        ],
        "managed": raw.get("managed", {}),
    }


def _get_db_stats(model_dir: Path) -> dict:
    db_path = model_dir / "data" / "data.db"
    try:
        display_path = str(db_path.relative_to(model_dir.parent.parent))
    except ValueError:
        display_path = str(db_path)
    stats: dict = {
        "path": display_path,
        "present": db_path.exists(),
        "raw_tables": 0,
        "raw_row_count": 0,
        "semantic_tables": 0,
        "semantic_row_count": 0,
        "last_extract_at": None,
        "last_map_at": None,
    }
    if not db_path.exists():
        return stats

    try:
        with sqlite3.connect(str(db_path)) as conn:
            tables = [
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            raw = [t for t in tables if t.startswith("raw_")]
            sem = [t for t in tables if t.startswith("sem_")]
            stats["raw_tables"] = len(raw)
            stats["semantic_tables"] = len(sem)
            for t in raw:
                stats["raw_row_count"] += conn.execute(
                    f"SELECT COUNT(*) FROM [{t}]"
                ).fetchone()[0]
            for t in sem:
                stats["semantic_row_count"] += conn.execute(
                    f"SELECT COUNT(*) FROM [{t}]"
                ).fetchone()[0]
            # Last extract / map timestamps from meta table
            if "_wt_runs" in tables:
                row = conn.execute(
                    "SELECT MAX(completed_at) FROM _wt_runs WHERE status='ok'"
                ).fetchone()
                if row and row[0]:
                    stats["last_extract_at"] = row[0]
    except Exception:
        pass

    # Derive last_map_at from audit log
    from core.registry import read_audit
    entries = read_audit(model_dir)
    map_entries = [e for e in entries if e.get("event") == "map_run"]
    if map_entries:
        stats["last_map_at"] = map_entries[-1]["timestamp"]

    return stats


def _scrub_credentials(cfg: dict) -> dict:
    """Return a copy of cfg with credential values removed."""
    return {k: v for k, v in cfg.items() if k.lower() not in _CREDENTIAL_KEYS}
