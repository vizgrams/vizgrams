# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tool service: load tool configuration and instantiate tools.

Lives in core/ so it is available in both the API and batch containers.
"""

import importlib
import logging
import os
import sys
from pathlib import Path

import yaml

from core.model_config import resolve_tool_config
from tools.base import BaseTool
from tools.file.tool import FileTool
from tools.garmin.tool import GarminTool
from tools.git.tool import GitHubTool
from tools.git_codeowners.tool import CodeownersTool
from tools.jira.tool import JiraTool

logger = logging.getLogger(__name__)

_CREDENTIAL_KEYS = frozenset({"api_token", "token", "password", "secret", "api_key"})

BUILTIN_REGISTRY: dict[str, type] = {
    "jira": JiraTool,
    "git": GitHubTool,
    "git_codeowners": CodeownersTool,
    "file": FileTool,
    "garmin": GarminTool,
}

SYSTEM_REGISTRY: dict[str, type] = {}


# ---------------------------------------------------------------------------
# System tool discovery
# ---------------------------------------------------------------------------


def discover_system_tools(tools_dirs: list[Path]) -> dict[str, type]:
    """Scan directories for tool.py files containing BaseTool subclasses.

    Each immediate subdirectory with a ``tool.py`` is imported; the first
    concrete ``BaseTool`` subclass found is registered under the directory name.

    Directories are scanned in order.  Names that collide with
    ``BUILTIN_REGISTRY`` or an earlier directory are skipped (warning logged).

    Returns the discovered {name: class} mapping.
    """
    discovered: dict[str, type] = {}
    for tools_dir in tools_dirs:
        if not tools_dir.is_dir():
            logger.debug("System tools directory does not exist: %s", tools_dir)
            continue
        for entry in sorted(tools_dir.iterdir()):
            if not entry.is_dir():
                continue
            tool_py = entry / "tool.py"
            if not tool_py.is_file():
                continue
            name = entry.name
            if name in BUILTIN_REGISTRY:
                logger.warning("System tool '%s' skipped — collides with built-in", name)
                continue
            if name in discovered:
                logger.warning("System tool '%s' skipped — already registered from earlier directory", name)
                continue
            try:
                cls = _import_tool_class(name, tool_py)
                if cls is not None:
                    discovered[name] = cls
            except Exception:
                logger.exception("Failed to load system tool '%s' from %s", name, tool_py)
    return discovered


def _import_tool_class(name: str, tool_py: Path) -> type | None:
    """Import tool.py and return the first BaseTool subclass found."""
    spec_name = f"_system_tools.{name}"
    spec = importlib.util.spec_from_file_location(spec_name, tool_py)
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec_name] = mod
    spec.loader.exec_module(mod)
    for attr_name in dir(mod):
        obj = getattr(mod, attr_name)
        if (
            isinstance(obj, type)
            and issubclass(obj, BaseTool)
            and obj is not BaseTool
        ):
            return obj
    logger.warning("No BaseTool subclass found in %s", tool_py)
    return None


def init_system_tools() -> int:
    """Discover and register system tools from VZ_TOOLS_DIR.

    VZ_TOOLS_DIR is a colon-separated list of directories, like PATH.
    Call once at app startup. Returns the number of tools discovered.
    """
    env = os.environ.get("VZ_TOOLS_DIR", "")
    if not env:
        return 0
    dirs = [Path(d) for d in env.split(":") if d]
    discovered = discover_system_tools(dirs)
    SYSTEM_REGISTRY.update(discovered)
    if discovered:
        logger.info(
            "Discovered %d system tool(s): %s",
            len(discovered),
            ", ".join(sorted(discovered)),
        )
    return len(discovered)


def list_available_tools() -> list[dict]:
    """Return all registered tools (builtin + system) with their PARAMS metadata."""
    result = []
    for name, cls in BUILTIN_REGISTRY.items():
        result.append({
            "name": name,
            "source": "builtin",
            "params": getattr(cls, "PARAMS", {}),
        })
    for name, cls in SYSTEM_REGISTRY.items():
        result.append({
            "name": name,
            "source": "system",
            "params": getattr(cls, "PARAMS", {}),
        })
    return result


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def _load_model_config(model_dir: Path) -> dict:
    config_path = model_dir / "config.yaml"
    if not config_path.is_file():
        return {}
    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}
    return raw.get("tools", {})


def _load_custom_class(tool_name: str, tool_def: dict, model_dir: Path) -> type:
    module_path = tool_def["module"]
    class_name = tool_def["class"]
    file_path = model_dir / Path(module_path.replace(".", "/") + ".py")
    if not file_path.is_file():
        raise FileNotFoundError(f"Model tool module not found: {file_path}")
    spec_name = f"_model_tools.{model_dir.name}.{module_path}"
    spec = importlib.util.spec_from_file_location(spec_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec_name] = mod
    spec.loader.exec_module(mod)
    return getattr(mod, class_name)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def list_tools(model_dir: Path) -> list[dict]:
    """Return safe (credential-free) tool config list."""
    model_config = _load_model_config(model_dir)
    result = []
    for name, cfg in model_config.items():
        if not isinstance(cfg, dict):
            continue
        result.append({
            "name": name,
            "enabled": cfg.get("enabled", False),
            "config": _scrub(cfg),
        })
    return result


def get_tool_info(model_dir: Path, tool_name: str) -> dict:
    """Return tool config + available commands."""
    model_config = _load_model_config(model_dir)
    if tool_name not in model_config:
        raise KeyError(f"Tool '{tool_name}' not listed in config.yaml.")
    cfg = model_config[tool_name]

    commands: list[str] = []
    cls = _resolve_class(tool_name, cfg, model_dir)
    if cls is not None:
        try:
            instance = cls.__new__(cls)
            if hasattr(instance, "list_commands"):
                commands = instance.list_commands()
        except Exception:
            pass

    return {
        "name": tool_name,
        "enabled": cfg.get("enabled", False),
        "config": _scrub(cfg),
        "commands": commands,
    }


def get_tool_instance(tool_name: str, model_dir: Path):
    """Instantiate and return a tool with resolved credentials.

    When config.yaml is present, the tool must be listed and enabled.
    """
    model_config = _load_model_config(model_dir)

    if model_config:
        if tool_name not in model_config:
            raise ValueError(
                f"Tool '{tool_name}' is not listed in config.yaml for this model."
            )
        if not model_config[tool_name].get("enabled", False):
            raise ValueError(
                f"Tool '{tool_name}' is disabled in config.yaml for this model."
            )

    cfg = model_config.get(tool_name, {})
    resolved = resolve_tool_config(cfg)
    cls = _resolve_class(tool_name, cfg, model_dir)
    if cls is None:
        raise ValueError(f"Unknown tool: '{tool_name}'")
    return cls(config=resolved, model_dir=model_dir)


def _resolve_class(tool_name: str, cfg: dict, model_dir: Path) -> type | None:
    if tool_name in BUILTIN_REGISTRY:
        return BUILTIN_REGISTRY[tool_name]
    if tool_name in SYSTEM_REGISTRY:
        return SYSTEM_REGISTRY[tool_name]
    if "module" in cfg and "class" in cfg:
        return _load_custom_class(tool_name, cfg, model_dir)
    return None


def _scrub(cfg: dict) -> dict:
    return {
        k: v for k, v in cfg.items()
        if k.lower() not in _CREDENTIAL_KEYS and k not in ("module", "class")
    }
