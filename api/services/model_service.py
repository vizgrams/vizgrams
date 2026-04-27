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
        try:
            (base_dir / ".vz_context").write_text(name + "\n")
        except OSError:
            pass

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
    # Best-effort .vz_context for CLI compat (container fs may be read-only)
    try:
        (base_dir / ".vz_context").write_text(model_name + "\n")
    except OSError:
        pass
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
# Model config — tools + database (VG-143, VG-144)
# ---------------------------------------------------------------------------


def get_model_config(models_dir: Path, model_name: str) -> dict:
    """Return the model config with credential references masked (not resolved).

    Returns raw config from the DB (or config.yaml fallback) so that the
    masked values are round-trippable — e.g. ``env:CLICKHOUSE_PASSWORD``
    becomes ``env:***``, not ``***`` (which would fail PUT validation).
    """
    registry = load_registry(models_dir)
    if model_name not in registry:
        raise KeyError(f"Model '{model_name}' not found in registry.")
    from core.vizgrams_db import load_database_config_from_db, load_model_config_from_db
    # Read raw (unresolved) config — DB first, then config.yaml fallback
    tools = load_model_config_from_db(model_name)
    db_cfg = load_database_config_from_db(model_name)
    if tools is None or db_cfg is None:
        from core.model_config import load_config_yaml
        model_dir = models_dir / model_name
        yaml_data = load_config_yaml(model_dir) if (model_dir / "config.yaml").exists() else {}
        if tools is None:
            tools = yaml_data.get("tools", {})
        if db_cfg is None:
            db_cfg = yaml_data.get("database", {})
    import os
    deployment_backend = os.environ.get("VZ_DATABASE_BACKEND")
    return {
        "tools": _mask_credentials(tools),
        "database": _mask_credential_values(db_cfg),
        "database_managed": deployment_backend is not None,
    }


def update_model_config(
    models_dir: Path, model_name: str, data: dict
) -> dict:
    """Update tools and/or database config in the DB. Returns masked config."""
    registry = load_registry(models_dir)
    if model_name not in registry:
        raise KeyError(f"Model '{model_name}' not found in registry.")

    from core.vizgrams_db import set_model_database_config, set_model_tools_config

    if "tools" in data and data["tools"] is not None:
        _validate_no_literal_credentials(data["tools"])
        set_model_tools_config(model_name, data["tools"])

    if "database" in data and data["database"] is not None:
        _validate_no_literal_credentials_flat(data["database"])
        set_model_database_config(model_name, data["database"])

    model_dir = models_dir / model_name
    append_audit(model_dir, "config_updated", "Updated via API", actor=current_actor())
    return get_model_config(models_dir, model_name)


def _mask_credentials(tools: dict) -> dict:
    """Mask credential values in a tools config dict (nested by tool name)."""
    return {
        tool_name: _mask_credential_values(tool_cfg)
        if isinstance(tool_cfg, dict) else tool_cfg
        for tool_name, tool_cfg in tools.items()
    }


def _mask_credential_values(cfg: dict) -> dict:
    """Mask credential field values in a flat config dict."""
    result = {}
    for k, v in cfg.items():
        if k in _CREDENTIAL_KEYS and isinstance(v, str):
            if v.startswith("env:"):
                result[k] = "env:***"
            elif v.startswith("file:"):
                result[k] = "file:***"
            else:
                result[k] = "***"
        else:
            result[k] = v
    return result


def _validate_no_literal_credentials(tools: dict) -> None:
    """Raise ValueError if any tool has a credential with a literal value."""
    for tool_name, tool_cfg in tools.items():
        if not isinstance(tool_cfg, dict):
            continue
        for key, val in tool_cfg.items():
            if key in _CREDENTIAL_KEYS and isinstance(val, str) and not val.startswith(("env:", "file:")):
                raise ValueError(
                    f"Tool '{tool_name}': credential '{key}' must use "
                    f"'env:VAR_NAME' or 'file:secret_name', not a literal value."
                )


def _validate_no_literal_credentials_flat(cfg: dict) -> None:
    """Raise ValueError if a flat config dict has literal credential values."""
    for key, val in cfg.items():
        if key in _CREDENTIAL_KEYS and isinstance(val, str) and not val.startswith(("env:", "file:")):
            raise ValueError(
                f"Credential '{key}' must use 'env:VAR_NAME' or "
                f"'file:secret_name', not a literal value."
            )


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
