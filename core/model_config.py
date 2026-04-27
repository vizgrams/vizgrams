# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Per-model tool configuration: loader and credential resolver."""

import os
from pathlib import Path

import yaml


def load_yaml(path: str | Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def load_config_yaml(model_dir: Path) -> dict:
    """Load config.yaml for a model, merged with config.<WT_ENV>.yaml if present.

    When ``WT_ENV`` is set (e.g. ``dev``), the file ``config.dev.yaml`` in the
    model directory is loaded and merged on top of the base ``config.yaml``::

      - The ``access:`` list from the env file is **prepended** to the base list,
        so env-specific entries take priority (first-match wins in RBAC).
      - All other top-level keys from the env file overwrite the base value.

    This lets you keep production access rules in ``config.yaml`` and add Dex
    test accounts or relaxed permissions in ``config.dev.yaml`` without editing
    the production file.
    """
    model_dir = Path(model_dir)
    base_path = model_dir / "config.yaml"
    data: dict = load_yaml(base_path) if base_path.exists() else {}

    env = os.environ.get("WT_ENV")
    if env:
        env_path = model_dir / f"config.{env}.yaml"
        if env_path.exists():
            env_data = load_yaml(env_path)
            # Prepend env access entries so they match first
            if "access" in env_data:
                base_access = data.get("access") or []
                data = {**data, "access": env_data["access"] + base_access}
            for k, v in env_data.items():
                if k != "access":
                    data[k] = v

    return data

# Credential field names whose values are resolved at instantiation time
_CREDENTIAL_KEYS = {"api_token", "token", "password", "secret"}


def resolve_credential(value) -> str | None:
    """Resolve a credential reference to its actual value.

    Supports three forms:
      env:VAR_NAME  — read from environment variable at runtime
      file:name     — bare name resolved against VZ_SECRETS_DIR (preferred)
      file:path     — absolute or ~-relative path (legacy; still supported)
      literal       — returned as-is

    Bare name resolution (``file:name``, no path separators or ``~``):
      The name is resolved relative to ``VZ_SECRETS_DIR`` env var when set
      (e.g. ``/run/secrets`` in containers), or ``~/.secrets/`` otherwise.
      This makes config files environment-agnostic: the same YAML works in
      local dev, Docker, and Kubernetes — only the mount point changes.

    Non-string values (e.g. None) are returned unchanged.
    Returns None if an env: variable is not set (unset vars may be intentional
    for future-use fields; required fields should be validated by the tool).
    Raises FileNotFoundError if a file: path does not exist.
    """
    if not isinstance(value, str):
        return value
    if value.startswith("env:"):
        return os.environ.get(value[4:])
    if value.startswith("file:"):
        raw = value[5:]
        secrets_dir = os.environ.get("VZ_SECRETS_DIR")
        # Bare name: no path separator and no leading ~ — resolve against secrets dir
        if "/" not in raw and "\\" not in raw and not raw.startswith("~"):
            base = Path(secrets_dir) if secrets_dir else Path("~/.secrets").expanduser()
            path = base / raw
        # Legacy ~/ form: honour VZ_SECRETS_DIR override, fall back to expanduser
        elif secrets_dir and raw.startswith("~/.secrets/"):
            path = Path(secrets_dir) / raw[len("~/.secrets/"):]
        else:
            path = Path(raw).expanduser()
        return path.read_text().strip()
    return value


def resolve_tool_config(raw: dict) -> dict:
    """Resolve credential fields in a tool config dict.

    Returns a new dict with all _CREDENTIAL_KEYS values resolved via
    resolve_credential. Other fields are passed through unchanged.
    Does not mutate the input dict.
    """
    return {
        k: resolve_credential(v) if k in _CREDENTIAL_KEYS else v
        for k, v in raw.items()
    }


def load_model_config(model_dir: Path) -> dict | None:
    """Return the raw tools config for a model (credentials NOT resolved).

    Reads from the DB first; falls back to config.yaml on disk if the DB
    column is NULL (pre-migration or not yet seeded).

    Call resolve_tool_config when instantiating a specific tool.
    """
    from core.vizgrams_db import load_model_config_from_db
    db_config = load_model_config_from_db(Path(model_dir).name)
    if db_config is not None:
        return db_config
    # Fallback: file-based
    path = Path(model_dir) / "config.yaml"
    if not path.exists():
        return None
    data = load_config_yaml(model_dir)
    return data.get("tools", {})


_DATABASE_DEFAULTS: dict = {
    "backend": "sqlite",
}

_CLICKHOUSE_DEFAULTS: dict = {
    "host": "env:CLICKHOUSE_HOST",
    "port": 8123,
    "username": "default",
    "password": "env:CLICKHOUSE_PASSWORD",
}


def load_database_config(model_dir: Path) -> dict:
    """Return the database config for a model, with defaults and credentials resolved.

    Resolution order:
      1. ``VZ_DATABASE_BACKEND`` env var — deployment-level override.
         When set, all models use this backend with connection details from
         env vars (CLICKHOUSE_HOST, etc.). Per-model database config is ignored.
      2. Per-model DB column (``database_config`` in models table).
      3. Per-model ``config.yaml`` on disk (legacy fallback).
      4. Defaults (SQLite).

    The returned dict always contains at least ``backend``.  Additional keys
    depend on the backend:
      clickhouse → ``host``, ``port``, ``database``, ``username``, ``password``,
                   ``raw_database``, ``sem_database``
      sqlite     → ``path`` (relative to model_dir)
      duckdb     → ``path`` (relative to model_dir)
    """
    model_dir = Path(model_dir)

    # Deployment-level override — all models share the same backend
    deployment_backend = os.environ.get("VZ_DATABASE_BACKEND")
    if deployment_backend:
        return _apply_database_defaults({"backend": deployment_backend}, model_dir)

    from core.vizgrams_db import load_database_config_from_db
    db_block = load_database_config_from_db(model_dir.name)

    # Fallback: file-based
    if db_block is None:
        path = model_dir / "config.yaml"
        if path.exists():
            db_block = load_config_yaml(model_dir).get("database") or {}
        else:
            db_block = {}

    return _apply_database_defaults(db_block, model_dir)


def _apply_database_defaults(db_block: dict, model_dir: Path) -> dict:
    """Merge defaults, resolve credentials, and derive ClickHouse database names."""
    merged = {**_DATABASE_DEFAULTS, **db_block}
    # Inject ClickHouse connection defaults only when backend is clickhouse.
    if merged.get("backend") == "clickhouse":
        merged = {**_CLICKHOUSE_DEFAULTS, **merged}
    resolved = {
        k: resolve_credential(v) if isinstance(v, str) else v
        for k, v in merged.items()
    }

    # Apply env-var fallbacks: CLICKHOUSE_HOST / CLICKHOUSE_PASSWORD may be
    # unset in local dev (Docker-only env vars) — fall back to safe defaults.
    if resolved.get("backend") == "clickhouse":
        if not resolved.get("host"):
            resolved["host"] = "localhost"
        if resolved.get("password") is None:
            resolved["password"] = ""
        # Derive database name from model directory when not explicitly set.
        if not resolved.get("database"):
            resolved["database"] = model_dir.name
        base = resolved["database"]
        if "raw_database" not in resolved:
            resolved["raw_database"] = f"{base}_raw"
        if "sem_database" not in resolved:
            resolved["sem_database"] = base

    return resolved


