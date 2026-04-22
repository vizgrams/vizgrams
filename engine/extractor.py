# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Core extraction engine: reads YAML configs, runs tools, writes to DB."""

import json
import logging
from pathlib import Path

import yaml

from core.db import DBBackend, _now_utc
from core.types import ColumnDef, OutputConfig, RowSource, TaskConfig, WriteMode
from engine.schema import ensure_table, extract_json_path
from tools.base import BaseTool

logger = logging.getLogger(__name__)


def _parse_output(raw: dict) -> OutputConfig:
    """Parse a raw output dict into an OutputConfig."""
    columns = [
        ColumnDef(
            name=c["name"],
            json_path=c["json_path"],
            type=c.get("type"),
        )
        for c in raw.get("columns", [])
    ]

    rs_raw = raw.get("row_source")
    row_source = None
    if rs_raw:
        row_source = RowSource(
            mode=rs_raw.get("mode", "SINGLE"),
            json_path=rs_raw.get("json_path"),
            inherit=rs_raw.get("inherit"),
        )

    return OutputConfig(
        table=raw["table"],
        write_mode=WriteMode[raw["write_mode"]],
        primary_keys=raw.get("primary_keys", []),
        columns=columns,
        row_source=row_source,
    )


def _parse_tasks_from_dict(data: dict) -> list[TaskConfig]:
    """Parse task list from an already-loaded extractor data dict."""
    tasks = []
    for t in data.get("tasks", []):
        if "outputs" in t:
            outputs = [_parse_output(o) for o in t["outputs"]]
        elif "output" in t:
            outputs = [_parse_output(t["output"])]
        else:
            outputs = []

        tasks.append(TaskConfig(
            name=t["name"],
            tool=t["tool"],
            command=t["command"],
            params=t.get("params", {}),
            context=t.get("context", {}),
            outputs=outputs,
            since=t.get("since"),
            incremental=t.get("incremental", False),
        ))
    return tasks


def parse_yaml_config(path: str | Path, validate: bool = False) -> list[TaskConfig]:
    """Load an extractor YAML file and return a list of TaskConfig.

    When validate=True, runs schema + semantic validation before parsing
    and raises ValueError with all errors if any are found.
    """
    if validate:
        from core.validation import validate_extractor_yaml
        errors = validate_extractor_yaml(path)
        if errors:
            msg = f"{path}: {len(errors)} validation error(s):\n"
            msg += "\n".join(f"  [{e.rule}] {e.path}: {e.message}" for e in errors)
            raise ValueError(msg)

    with open(path) as f:
        data = yaml.safe_load(f)
    return _parse_tasks_from_dict(data)


def parse_yaml_config_from_content(content: str) -> list[TaskConfig]:
    """Parse extractor YAML content string into a list of TaskConfig."""
    return _parse_tasks_from_dict(yaml.safe_load(content) or {})


def find_extractor(model_dir: Path, tool_name: str) -> str:
    """Return the YAML content of the extractor whose tasks use *tool_name*.

    Reads from the metadata DB. Raises KeyError if no matching extractor is found.
    """
    from core import metadata_db
    for name in metadata_db.list_artifact_names(model_dir, "extractor"):
        content = metadata_db.get_current_content(model_dir, "extractor", name)
        if not content:
            continue
        try:
            tasks = parse_yaml_config_from_content(content)
        except Exception:
            continue
        if tasks and tasks[0].tool == tool_name:
            return content
    raise KeyError(f"Extractor for tool '{tool_name}' not found.")


def resolve_params(task: TaskConfig, tool: BaseTool) -> list[dict]:
    """Expand wildcard params into a list of concrete param dicts.

    If no wildcards, returns [task.params].
    If a param value is "*", calls tool.resolve_wildcard() and returns
    one param dict per resolved value.
    """
    wildcard_params = {k: v for k, v in task.params.items() if v == "*"}

    if not wildcard_params:
        return [dict(task.params)]

    # Support single wildcard param (expand to list of param dicts)
    if len(wildcard_params) != 1:
        raise ValueError(f"Task {task.name}: only one wildcard param supported at a time")

    param_name, param_value = next(iter(wildcard_params.items()))
    resolved_values = tool.resolve_wildcard(param_name, param_value)

    param_sets = []
    for val in resolved_values:
        params = dict(task.params)
        params[param_name] = val
        param_sets.append(params)

    return param_sets


def _coerce_to_text(value):
    """Coerce a complex value (dict/list) to a plain text string.

    Handles common API patterns like Jira select fields that return
    {"value": "...", "id": "..."} or {"name": "..."} objects, and
    multi-select fields that return lists of such objects.
    """
    if isinstance(value, dict):
        if "value" in value:
            return value["value"]
        if "name" in value:
            return value["name"]
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(_coerce_to_text(item)))
            else:
                parts.append(str(item))
        return ", ".join(parts)
    return value


def _map_record(output: OutputConfig, record: dict, context: dict | None = None) -> dict:
    """Extract columns from a raw record using json_path mappings."""
    row = {}

    # Add context columns (e.g., board_id from wildcard iteration)
    if context:
        row.update({ctx_col_name: ctx_value for ctx_col_name, ctx_value in context.items()})

    # Map defined columns
    for col in output.columns:
        val = extract_json_path(record, col.json_path)
        # When column is explicitly STRING, coerce complex values to readable strings.
        # extract_json_path returns dicts/lists as JSON strings, so parse first.
        if col.type == "STRING" and isinstance(val, str) and val and val[0] in ("{", "["):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, (dict, list)):
                    val = _coerce_to_text(parsed)
            except (json.JSONDecodeError, ValueError):
                pass
        row[col.name] = val

    return row


def _write_row(output: OutputConfig, db: DBBackend, table_name: str, row: dict) -> None:
    """Write a single row using the output's write_mode."""
    if output.write_mode == WriteMode.APPEND:
        db.append(table_name, row)
    else:
        # UPSERT and REPLACE both use upsert for individual rows
        # (REPLACE truncates the table beforehand)
        db.upsert(table_name, row)


def _explode_records(
    output: OutputConfig, records: list[dict], context: dict,
) -> list[tuple[dict, dict]]:
    """Fan out records into (element, merged_context) pairs for EXPLODE mode."""
    exploded = []
    for record in records:
        array = extract_json_path(record, output.row_source.json_path, serialize=False)
        if not isinstance(array, list):
            continue

        # Pre-compute inherited values from parent record
        inherited = {}
        if output.row_source.inherit:
            for col_name, parent_path in output.row_source.inherit.items():
                inherited[col_name] = extract_json_path(record, parent_path)

        merged_context = {**context, **inherited}
        for element in array:
            exploded.append((element, merged_context))

    return exploded


def _compute_since(
    task: TaskConfig, db: DBBackend | None, since_override: str | None = None,
    param_key: str | None = None,
) -> str | None:
    """Resolve effective since date from CLI override, YAML, and incremental DB lookup.

    Priority: CLI override > max(YAML since, DB last_run) when incremental.

    When param_key is provided and incremental, looks up the per-param_set
    last run first, falling back to the task-level last run.
    """
    if since_override:
        return since_override

    yaml_since = task.since
    db_since = None

    if task.incremental and db is not None:
        if param_key is not None:
            # Try per-param_set first, fall back to task-level
            db_since = db.get_last_run(task.name, param_key=param_key)
            if db_since is None:
                db_since = db.get_last_run(task.name)
        else:
            db_since = db.get_last_run(task.name)

    # Pick the more recent of yaml_since and db_since
    candidates = [s for s in (yaml_since, db_since) if s]
    if not candidates:
        return None
    return max(candidates)


def _process_output(
    output: OutputConfig, records: list[dict],
    context: dict, context_col_names: list[str], db: DBBackend,
) -> None:
    """Process records for a single output — handles both SINGLE and EXPLODE modes."""
    is_explode = output.row_source and output.row_source.mode == "EXPLODE"

    if is_explode:
        exploded = _explode_records(output, records, context)
        if not exploded:
            return
        table_name = ensure_table(output, context_col_names, [exploded[0][0]], db)
        for element, merged_context in exploded:
            row = _map_record(output, element, merged_context)
            _write_row(output, db, table_name, row)
    else:
        table_name = ensure_table(output, context_col_names, records, db)
        for record in records:
            row = _map_record(output, record, context)
            _write_row(output, db, table_name, row)


class JobCancelledError(Exception):
    """Raised inside run_task when the job has been cancelled."""


def run_task(
    task: TaskConfig, tool: BaseTool, db: DBBackend | None,
    debug: bool = False, since_override: str | None = None,
    progress_cb=None, cancel_check=None,
) -> int:
    """Run a single extraction task. Returns total source records fetched."""
    logger.info("Running task: %s", task.name)
    started_at = _now_utc()

    context_col_names = list(task.context.values())

    param_sets = resolve_params(task, tool)
    is_wildcard = len(param_sets) > 1 or any(
        v == "*" for v in task.params.values()
    )
    total = 0
    iteration_errors = 0
    # REPLACE outputs are truncated lazily — just before the first successful
    # write.  This prevents data loss when a fetch fails (network error, rate
    # limit) and the task would otherwise leave the table empty.
    _replace_truncated: set[str] = set()

    if is_wildcard:
        logger.info("  resolved %d param_set(s)", len(param_sets))

    # Compute task-level since (used for non-wildcard or as initial log)
    task_since = _compute_since(task, db, since_override)
    if task_since:
        logger.info("  since=%s", task_since)

    for idx, params in enumerate(param_sets, 1):
        if cancel_check and cancel_check():
            logger.info("  %s: cancelled at iteration %d/%d", task.name, idx, len(param_sets))
            raise JobCancelledError(f"Cancelled at iteration {idx}/{len(param_sets)}")

        # Build param_key for wildcard tasks (e.g. "board_id=123")
        param_key = None
        if is_wildcard:
            wildcard_parts = [
                f"{k}={params[k]}" for k in sorted(task.context)
                if k in params
            ]
            if wildcard_parts:
                param_key = "&".join(wildcard_parts)

        if is_wildcard:
            logger.info("  [%d/%d] %s", idx, len(param_sets), param_key or params)
            if progress_cb:
                progress_cb(f"  [{idx}/{len(param_sets)}] {param_key or ''}")

        # Compute per-param_set since for wildcard incremental tasks
        effective_since = _compute_since(task, db, since_override, param_key=param_key)

        # Inject since into params for the tool
        if effective_since:
            params["since"] = effective_since

        # Build context values for wildcard iterations
        context = {}
        for param_name, col_name in task.context.items():
            context[col_name] = params.get(param_name)

        iteration_started = _now_utc()

        # Collect records from tool (skip this iteration on error)
        try:
            records = list(tool.run(task.command, params))
        except Exception as e:
            logger.warning("  %s (params=%s): skipped — %s", task.name, params, e)
            if progress_cb:
                progress_cb(f"  WARNING: {param_key or task.name}: skipped — {e}")
            iteration_errors += 1
            continue

        if not records:
            logger.info("  %s (params=%s): 0 records", task.name, params)
            # Record successful (empty) iteration for wildcard tasks
            if db is not None and not debug and param_key is not None:
                db.record_run(
                    task.name, iteration_started, _now_utc(),
                    0, "success", param_key=param_key,
                )
            continue

        if debug:
            for r in records:
                print(json.dumps(r, default=str))
            total += len(records)
            continue

        # Deferred truncate: clear REPLACE tables just before first successful write
        if db is not None:
            for output in task.outputs:
                if output.write_mode == WriteMode.REPLACE and output.table not in _replace_truncated:
                    if db.table_exists(output.table):
                        db.truncate(output.table)
                    _replace_truncated.add(output.table)

        # Process each output
        for output in task.outputs:
            _process_output(output, records, context, context_col_names, db)

        total += len(records)
        logger.info("  %s (params=%s): %d records", task.name, params, len(records))

        # Record per-param_set run for wildcard tasks
        if db is not None and param_key is not None:
            db.record_run(
                task.name, iteration_started, _now_utc(),
                len(records), "success", param_key=param_key,
            )

    # Record task-level run in _task_runs (only when writing to DB)
    # Status is "failed" if every iteration errored and nothing was fetched.
    if db is not None and not debug:
        completed_at = _now_utc()
        all_failed = iteration_errors > 0 and total == 0
        status = "failed" if all_failed else "success"
        db.record_run(task.name, started_at, completed_at, total, status)

    return total
