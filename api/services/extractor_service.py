# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Extractor service: list, inspect, validate and execute extractors."""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

from core import metadata_db
from core.validation import validate_extractor_yaml
from engine.extractor import find_extractor, parse_yaml_config_from_content


def list_extractors(model_dir: Path) -> list[dict]:
    result = []
    for name in metadata_db.list_artifact_names(model_dir, "extractor"):
        content = metadata_db.get_current_content(model_dir, "extractor", name)
        if not content:
            continue
        try:
            tasks = parse_yaml_config_from_content(content)
        except Exception:
            tasks = []
        tool = tasks[0].tool if tasks else name
        result.append({
            "tool": tool,
            "tasks": [_task_summary(t) for t in tasks],
            "raw_yaml": content,
        })
    return result


def get_extractor(model_dir: Path, tool_name: str) -> dict:
    content = find_extractor(model_dir, tool_name)  # raises KeyError if not found
    tasks = parse_yaml_config_from_content(content)
    return {
        "tool": tool_name,
        "tasks": [_task_detail(t) for t in tasks],
        "raw_yaml": content,
    }


def validate_extractor(model_dir: Path, tool_name: str) -> dict:
    import os
    import tempfile

    content = find_extractor(model_dir, tool_name)  # raises KeyError if not found
    tmp_fd, tmp_path_str = tempfile.mkstemp(suffix=".yaml")
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(tmp_fd, "w") as f:
            f.write(content)
        errors = validate_extractor_yaml(tmp_path)
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass
    return {
        "valid": len(errors) == 0,
        "errors": [{"path": e.path, "message": e.message} for e in errors],
    }


class ExtractorConflictError(Exception):
    """Raised when an extraction job is already running for this model."""


def execute_extractor(
    model_dir: Path,
    tool_name: str,
    task_name: str | None,
    full_refresh: bool,
    job_service,
    since: str | None = None,
) -> dict:
    """Submit an extraction job to the batch service and return a job dict."""
    from api.batch_client import BatchServiceError, submit_job

    find_extractor(model_dir, tool_name)  # raises KeyError if not found

    try:
        return submit_job(
            model=model_dir.name,
            tool=tool_name,
            task=task_name,
            full_refresh=full_refresh,
            since=since,
            triggered_by="api",
        )
    except BatchServiceError as exc:
        if exc.status_code == 409:
            raise ExtractorConflictError(str(exc)) from exc
        raise


class ExtractorValidationError(Exception):
    """Raised when extractor YAML fails schema validation."""
    def __init__(self, errors: list[dict]):
        self.errors = errors
        super().__init__(f"{len(errors)} validation error(s)")


def create_or_replace_extractor(model_dir: Path, tool_name: str, content: str) -> dict:
    """Validate YAML content and write extractor to the metadata DB."""
    import os
    import tempfile

    tmp_fd, tmp_path_str = tempfile.mkstemp(suffix=".yaml")
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(tmp_fd, "w") as f:
            f.write(content)
        errors = validate_extractor_yaml(tmp_path)
        if errors:
            raise ExtractorValidationError(
                [{"path": e.path, "message": e.message} for e in errors]
            )
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass

    metadata_db.record_version(model_dir, "extractor", tool_name, content)
    return get_extractor(model_dir, tool_name)


def validate_all(model_dir: Path) -> list[dict]:
    """Validate all extractor artifacts in the DB; returns list of {tool, valid, errors}."""
    import os
    import tempfile

    results = []
    for name in metadata_db.list_artifact_names(model_dir, "extractor"):
        content = metadata_db.get_current_content(model_dir, "extractor", name)
        if not content:
            continue
        try:
            tasks = parse_yaml_config_from_content(content)
            tool = tasks[0].tool if tasks else name
        except Exception:
            tool = name

        tmp_fd, tmp_path_str = tempfile.mkstemp(suffix=".yaml")
        tmp_path = Path(tmp_path_str)
        try:
            with os.fdopen(tmp_fd, "w") as f:
                f.write(content)
            errors = validate_extractor_yaml(tmp_path)
        finally:
            try:
                tmp_path.unlink()
            except OSError:
                pass

        results.append({
            "tool": tool,
            "valid": len(errors) == 0,
            "errors": [{"path": e.path, "message": e.message, "rule": getattr(e, "rule", "")} for e in errors],
        })
    return results


# ---------------------------------------------------------------------------


def _task_summary(task) -> dict:
    outputs = task.outputs if hasattr(task, "outputs") else []
    table = outputs[0].table if outputs else getattr(task, "table", None)
    return {
        "name": task.name,
        "command": task.command,
        "table": table,
    }


def _task_detail(task) -> dict:
    outputs = task.outputs if hasattr(task, "outputs") else []
    table = outputs[0].table if outputs else getattr(task, "table", None)
    return {
        "name": task.name,
        "command": task.command,
        "table": table,
        "params": task.params if hasattr(task, "params") else {},
        "incremental": getattr(task, "incremental", False),
    }
