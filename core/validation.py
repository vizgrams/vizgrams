# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""YAML config validation: JSON Schema structural checks + semantic cross-field rules."""

from dataclasses import dataclass
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"


@dataclass
class ValidationError:
    path: str
    message: str
    rule: str


def _format_path(path_parts) -> str:
    """Convert jsonschema path deque to a human-readable dotted string."""
    parts = []
    for p in path_parts:
        if isinstance(p, int):
            # Attach array index to previous part: tasks[0]
            if parts:
                parts[-1] = f"{parts[-1]}[{p}]"
            else:
                parts.append(f"[{p}]")
        else:
            parts.append(str(p))
    return ".".join(parts) if parts else "(root)"


def load_schema(schema_name: str) -> dict:
    """Load a JSON Schema from schemas/{schema_name}.yaml."""
    path = SCHEMAS_DIR / f"{schema_name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Schema not found: {path}")
    with open(path) as f:
        return yaml.safe_load(f)


def validate_schema(data: dict, schema_name: str) -> list[ValidationError]:
    """Phase 1: structural validation via JSON Schema."""
    schema = load_schema(schema_name)
    validator = Draft202012Validator(schema)
    errors = []
    for err in sorted(validator.iter_errors(data), key=lambda e: list(e.absolute_path)):
        errors.append(ValidationError(
            path=_format_path(err.absolute_path),
            message=err.message,
            rule="schema",
        ))
    return errors


def _resolve_outputs(task: dict) -> list[tuple[str, dict]]:
    """Normalize output/outputs into a list of (path_prefix, output_dict) pairs."""
    if "outputs" in task:
        return [(f"outputs[{j}]", out) for j, out in enumerate(task["outputs"])]
    if "output" in task:
        return [("output", task["output"])]
    return []


def _check_cross_field_rules(data: dict) -> list[ValidationError]:
    """Phase 2: semantic cross-field checks for extractor configs."""
    errors = []
    seen_task_names = []

    for i, task in enumerate(data.get("tasks", [])):
        task_name = task.get("name", f"(unnamed-{i})")
        task_prefix = f"tasks[{i}]"

        # unique_task_name
        if task_name in seen_task_names:
            errors.append(ValidationError(
                path=f"{task_prefix}.name",
                message=f"duplicate task name {task_name!r}",
                rule="unique_task_name",
            ))
        seen_task_names.append(task_name)

        # output_or_outputs — exactly one must be present
        has_output = "output" in task
        has_outputs = "outputs" in task
        if has_output and has_outputs:
            errors.append(ValidationError(
                path=task_prefix,
                message="task must have 'output' or 'outputs', not both",
                rule="output_or_outputs",
            ))
        elif not has_output and not has_outputs:
            errors.append(ValidationError(
                path=task_prefix,
                message="task must have 'output' or 'outputs'",
                rule="output_or_outputs",
            ))

        # Per-output checks
        for out_prefix, output in _resolve_outputs(task):
            prefix = f"{task_prefix}.{out_prefix}"
            columns = output.get("columns", [])
            col_names = [c.get("name") for c in columns]

            # unique_column_name
            seen_cols = set()
            for j, col in enumerate(columns):
                cn = col.get("name")
                if cn in seen_cols:
                    errors.append(ValidationError(
                        path=f"{prefix}.columns[{j}].name",
                        message=f"duplicate column name {cn!r}",
                        rule="unique_column_name",
                    ))
                seen_cols.add(cn)

            # json_path_format
            for j, col in enumerate(columns):
                jp = col.get("json_path", "")
                if not jp.startswith("$."):
                    errors.append(ValidationError(
                        path=f"{prefix}.columns[{j}].json_path",
                        message=f"json_path {jp!r} must start with '$.'",
                        rule="json_path_format",
                    ))

            # primary_key_ref
            context_values = set((task.get("context") or {}).values())
            row_source = output.get("row_source") or {}
            inherit_keys = set((row_source.get("inherit") or {}).keys())
            primary_keys = output.get("primary_keys", [])
            valid_names = set(col_names) | context_values | inherit_keys
            for pk in primary_keys:
                if pk not in valid_names:
                    errors.append(ValidationError(
                        path=f"{prefix}.primary_keys",
                        message=f"primary key {pk!r} does not match any column name, context value, or inherit key",
                        rule="primary_key_ref",
                    ))

            # upsert_primary_keys
            write_mode = output.get("write_mode")
            if write_mode == "UPSERT" and not primary_keys:
                errors.append(ValidationError(
                    path=f"{prefix}.primary_keys",
                    message="UPSERT tasks should have at least one primary key",
                    rule="upsert_primary_keys",
                ))

            # row_source checks
            rs_mode = row_source.get("mode")

            if rs_mode == "EXPLODE" and not row_source.get("json_path"):
                errors.append(ValidationError(
                    path=f"{prefix}.row_source",
                    message="EXPLODE mode requires json_path",
                    rule="explode_requires_json_path",
                ))

            for col_name, jp in (row_source.get("inherit") or {}).items():
                if not jp.startswith("$."):
                    errors.append(ValidationError(
                        path=f"{prefix}.row_source.inherit.{col_name}",
                        message=f"inherit json_path {jp!r} must start with '$.'",
                        rule="inherit_json_path_format",
                    ))

            for col_name in inherit_keys:
                if col_name in set(col_names):
                    errors.append(ValidationError(
                        path=f"{prefix}.row_source.inherit.{col_name}",
                        message=f"inherit key {col_name!r} conflicts with a column name",
                        rule="inherit_column_conflict",
                    ))

    return errors


# --- model config validation ---

# Known built-in tool names (must match TOOL_REGISTRY keys in run.py)
_BUILTIN_TOOLS = {"jira", "git", "git_codeowners", "file"}

# Fields that must be present when the tool is enabled
_TOOL_REQUIRED_FIELDS: dict[str, list[str]] = {
    "jira": ["server", "email", "api_token"],
    "git": ["org"],
}

# Fields whose values should use env: or file: indirection
_CREDENTIAL_KEYS = {"api_token", "token", "password", "secret"}


def _check_model_config_rules(data: dict) -> list[ValidationError]:
    errors = []
    tools = data.get("tools", {})

    for tool_name, tool_cfg in tools.items():
        if not isinstance(tool_cfg, dict):
            errors.append(ValidationError(
                path=f"tools.{tool_name}",
                message="Tool config must be a mapping",
                rule="model_config.tool_must_be_mapping",
            ))
            continue

        has_module = "module" in tool_cfg
        has_class = "class" in tool_cfg

        # Custom tools: module and class must appear together
        if has_module != has_class:
            errors.append(ValidationError(
                path=f"tools.{tool_name}",
                message="Custom tools must specify both 'module' and 'class'",
                rule="model_config.custom_tool_requires_class",
            ))

        # Unknown tool: not a built-in and not a custom tool definition
        if tool_name not in _BUILTIN_TOOLS and not (has_module and has_class):
            errors.append(ValidationError(
                path=f"tools.{tool_name}",
                message=(
                    f"Unknown tool {tool_name!r}. "
                    f"Built-ins: {sorted(_BUILTIN_TOOLS)}. "
                    f"Custom tools must specify 'module' and 'class'."
                ),
                rule="model_config.unknown_tool",
            ))

        # Required fields for enabled built-in tools
        if tool_cfg.get("enabled", False):
            for field in _TOOL_REQUIRED_FIELDS.get(tool_name, []):
                if field not in tool_cfg:
                    errors.append(ValidationError(
                        path=f"tools.{tool_name}.{field}",
                        message=f"Required field '{field}' is missing for tool '{tool_name}'",
                        rule="model_config.missing_required_field",
                    ))

        # Credential fields should use env: or file: indirection
        for key, val in tool_cfg.items():
            if key in _CREDENTIAL_KEYS and isinstance(val, str) and not (
                val.startswith("env:") or val.startswith("file:")
            ):
                errors.append(ValidationError(
                    path=f"tools.{tool_name}.{key}",
                    message=(
                        f"Credential field '{key}' contains a literal value. "
                        f"Use 'env:VAR' or 'file:path' to avoid committing secrets."
                    ),
                    rule="model_config.literal_credential",
                ))

    return errors


def validate_model_config(path: str | Path) -> list[ValidationError]:
    """Validate a models/<model>/config.yaml file."""
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data.get("tools"), dict):
        return [ValidationError(
            path="tools",
            message="config.yaml must contain a 'tools' mapping",
            rule="model_config.missing_tools_key",
        )]

    return _check_model_config_rules(data)


def validate_extractor_yaml(path: str | Path) -> list[ValidationError]:
    """Validate an extractor YAML config file (both phases)."""
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)

    # Phase 1: structural
    errors = validate_schema(data, "extractor")
    if errors:
        return errors

    # Phase 2: semantic (only if structure is valid)
    errors.extend(_check_cross_field_rules(data))
    return errors


_VALID_STATUSES = {"active", "experimental", "archived"}
_REGISTRY_REQUIRED = ("display_name", "description", "owner", "created_at", "status")


def validate_registry(models_dir: Path) -> list[ValidationError]:
    """Validate registry.yaml in models_dir: required fields, status values, directory existence,
    and that every <models_dir>/<name>/ with a config.yaml has a registry entry."""
    from datetime import datetime

    from core.registry import load_registry

    errors: list[ValidationError] = []
    registry = load_registry(models_dir)

    for name, entry in registry.items():
        if not (models_dir / name).is_dir():
            errors.append(ValidationError(
                rule="missing_directory", path=name,
                message=f"Registry entry '{name}' has no models/{name}/ directory",
            ))
        for field in _REGISTRY_REQUIRED:
            if not entry.get(field):
                errors.append(ValidationError(
                    rule="missing_required_field", path=f"{name}.{field}",
                    message=f"Required field '{field}' is missing or empty",
                ))
        status = entry.get("status")
        if status and status not in _VALID_STATUSES:
            errors.append(ValidationError(
                rule="invalid_status", path=f"{name}.status",
                message=f"status must be one of {sorted(_VALID_STATUSES)}, got {status!r}",
            ))
        created_at = entry.get("created_at")
        if created_at:
            try:
                datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
            except ValueError:
                errors.append(ValidationError(
                    rule="invalid_timestamp", path=f"{name}.created_at",
                    message="created_at must be a valid ISO 8601 timestamp",
                ))

    if models_dir.is_dir():
        for model_path in sorted(models_dir.iterdir()):
            if (
                model_path.is_dir()
                and model_path.name not in ("__pycache__",)
                and (model_path / "config.yaml").is_file()
                and model_path.name not in registry
            ):
                errors.append(ValidationError(
                    rule="unregistered_model", path=model_path.name,
                    message=f"models/{model_path.name}/ has config.yaml but no registry entry",
                ))

    return errors
