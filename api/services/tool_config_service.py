# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tool config service: read and write the tools block of config.yaml."""

from pathlib import Path

import yaml

_CREDENTIAL_KEYS = frozenset({"api_token", "token", "password", "secret", "api_key"})

# Required connection fields per built-in tool (others are optional)
_REQUIRED_FIELDS: dict[str, list[str]] = {
    "git": ["org", "host", "token"],
    "jira": ["server", "email", "api_token"],
    "git_codeowners": [],  # inherits from git
    "file": [],
}


# ---------------------------------------------------------------------------
# config.yaml I/O (tools block only)
# ---------------------------------------------------------------------------

def _read_config(model_dir: Path) -> dict:
    path = model_dir / "config.yaml"
    if not path.is_file():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _write_config(model_dir: Path, full_config: dict) -> None:
    path = model_dir / "config.yaml"
    with open(path, "w") as f:
        yaml.dump(full_config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_tool_config(tool_name: str, config: dict) -> list[str]:
    """Return a list of validation error strings, empty if valid."""
    errors: list[str] = []

    # Credential fields must use env: or file: — no literals in source control
    for key, value in config.items():
        if key in _CREDENTIAL_KEYS and isinstance(value, str) and not (
            value.startswith("env:") or value.startswith("file:")
        ):
            errors.append(
                f"'{key}' must use env:<VAR> or file:<path> format "
                f"(literal credentials must not be stored in config.yaml)"
            )

    # Custom tools must have both module and class
    if "module" in config or "class" in config:
        if "module" not in config:
            errors.append("Custom tools must specify 'module' alongside 'class'")
        if "class" not in config:
            errors.append("Custom tools must specify 'class' alongside 'module'")

    # Required fields for known built-in tools (skip if not fully configured yet)
    if tool_name in _REQUIRED_FIELDS and config.get("enabled", False):
        for field in _REQUIRED_FIELDS[tool_name]:
            if not config.get(field):
                errors.append(f"'{field}' is required when tool '{tool_name}' is enabled")

    return errors


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def list_tool_configs(model_dir: Path) -> list[dict]:
    """Return all tool configurations (credential references shown, values not resolved)."""
    tools = _read_config(model_dir).get("tools", {})
    return [
        {"name": name, **cfg}
        for name, cfg in tools.items()
        if isinstance(cfg, dict)
    ]


def get_tool_config(model_dir: Path, tool_name: str) -> dict:
    """Return a single tool's configuration. Raises KeyError if not found."""
    tools = _read_config(model_dir).get("tools", {})
    if tool_name not in tools:
        raise KeyError(f"Tool '{tool_name}' not found in config.yaml")
    return {"name": tool_name, **tools[tool_name]}


def put_tool_config(model_dir: Path, tool_name: str, config: dict) -> dict:
    """Replace a tool's full configuration block.

    Validates credential format and required fields, then writes to config.yaml.
    Returns the stored configuration.
    """
    errors = _validate_tool_config(tool_name, config)
    if errors:
        raise ValueError(errors)

    full = _read_config(model_dir)
    full.setdefault("tools", {})[tool_name] = config
    _write_config(model_dir, full)
    return {"name": tool_name, **config}


def patch_tool_config(model_dir: Path, tool_name: str, updates: dict) -> dict:
    """Merge updates into an existing tool's configuration block.

    The tool must already exist. Validates the merged result before writing.
    Returns the updated configuration.
    """
    full = _read_config(model_dir)
    tools = full.setdefault("tools", {})
    if tool_name not in tools:
        raise KeyError(f"Tool '{tool_name}' not found in config.yaml")

    merged = {**tools[tool_name], **updates}
    errors = _validate_tool_config(tool_name, merged)
    if errors:
        raise ValueError(errors)

    tools[tool_name] = merged
    _write_config(model_dir, full)
    return {"name": tool_name, **merged}


def delete_tool_config(model_dir: Path, tool_name: str) -> None:
    """Remove a tool's configuration block entirely."""
    full = _read_config(model_dir)
    tools = full.get("tools", {})
    if tool_name not in tools:
        raise KeyError(f"Tool '{tool_name}' not found in config.yaml")
    del tools[tool_name]
    _write_config(model_dir, full)
