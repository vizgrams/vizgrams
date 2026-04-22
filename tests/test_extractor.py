# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Integration tests: tool + engine + DB working together."""

import json
import textwrap
from collections.abc import Iterator
from tempfile import NamedTemporaryFile

import pytest

from core.db import SQLiteBackend
from core.types import ColumnDef, OutputConfig, RowSource, WriteMode
from engine.extractor import (
    _coerce_to_text,
    _compute_since,
    _explode_records,
    parse_yaml_config,
    resolve_params,
    run_task,
)
from tools.base import BaseTool

# --- Fake tool for testing ---

class FakeTool(BaseTool):
    """A tool that returns canned data for testing."""

    def __init__(self, data: dict[str, list[dict]]):
        self._data = data
        self._wildcard_values: dict[str, list] = {}

    def run(self, command: str, params: dict | None = None) -> Iterator[dict]:
        params = params or {}
        key = command
        # For parameterized commands, include param values in key
        if "board_id" in params:
            key = f"{command}:{params['board_id']}"
        records = self._data.get(key, [])
        yield from records

    def list_commands(self) -> list[str]:
        return list(set(k.split(":")[0] for k in self._data))

    def resolve_wildcard(self, param_name: str, param_value: str) -> list:
        if param_name in self._wildcard_values:
            return self._wildcard_values[param_name]
        raise NotImplementedError(f"No wildcard for {param_name}")


@pytest.fixture
def db():
    backend = SQLiteBackend()
    backend.connect()
    backend.ensure_meta_table()
    yield backend
    backend.close()


def _make_task(name="test_task", tool="fake", command="cmd", **kwargs):
    """Helper to create a TaskConfig with a single OutputConfig."""
    from core.types import TaskConfig
    output_kwargs = {}
    task_kwargs = {}
    output_fields = {"table", "write_mode", "primary_keys", "columns", "row_source"}
    for k, v in kwargs.items():
        if k in output_fields:
            output_kwargs[k] = v
        else:
            task_kwargs[k] = v

    outputs = [OutputConfig(**output_kwargs)] if output_kwargs else []
    return TaskConfig(name=name, tool=tool, command=command, outputs=outputs, **task_kwargs)


# --- parse_yaml_config ---

def test_parse_yaml_config():
    yaml_content = textwrap.dedent("""\
    tasks:
      - name: test_boards
        tool: fake
        command: boards
        params: {}
        output:
          table: boards
          write_mode: UPSERT
          primary_keys: [id]
          columns:
            - name: id
              json_path: $.id
            - name: board_name
              json_path: $.name
    """)
    with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        tasks = parse_yaml_config(f.name)

    assert len(tasks) == 1
    t = tasks[0]
    assert t.name == "test_boards"
    assert t.tool == "fake"
    assert t.command == "boards"
    assert len(t.outputs) == 1
    out = t.outputs[0]
    assert out.write_mode == WriteMode.UPSERT
    assert out.primary_keys == ["id"]
    assert len(out.columns) == 2
    assert out.columns[0].name == "id"
    assert out.columns[0].json_path == "$.id"
    assert out.row_source is None


def test_parse_yaml_with_context_and_type():
    yaml_content = textwrap.dedent("""\
    tasks:
      - name: test_sprints
        tool: fake
        command: sprints
        params:
          board_id: "*"
        context:
          board_id: board_id
        output:
          table: sprints
          write_mode: UPSERT
          primary_keys: [board_id, id]
          columns:
            - name: id
              json_path: $.id
            - name: start_date
              json_path: $.startDate
              type: STRING
    """)
    with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        tasks = parse_yaml_config(f.name)

    t = tasks[0]
    assert t.context == {"board_id": "board_id"}
    assert t.params == {"board_id": "*"}
    assert t.outputs[0].columns[1].type == "STRING"


def test_parse_yaml_with_row_source():
    yaml_content = textwrap.dedent("""\
    tasks:
      - name: test_explode
        tool: fake
        command: issues
        output:
          table: changelog
          write_mode: APPEND
          row_source:
            mode: EXPLODE
            json_path: $.changelog.histories
            inherit:
              issue_key: $.key
          columns:
            - name: history_id
              json_path: $.id
    """)
    with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        tasks = parse_yaml_config(f.name)

    t = tasks[0]
    out = t.outputs[0]
    assert out.row_source is not None
    assert out.row_source.mode == "EXPLODE"
    assert out.row_source.json_path == "$.changelog.histories"
    assert out.row_source.inherit == {"issue_key": "$.key"}
    assert out.write_mode == WriteMode.APPEND


def test_parse_yaml_with_outputs_plural():
    yaml_content = textwrap.dedent("""\
    tasks:
      - name: test_multi
        tool: fake
        command: issues
        outputs:
          - table: issues
            write_mode: UPSERT
            primary_keys: [key]
            columns:
              - name: key
                json_path: $.key
          - table: changelog
            write_mode: APPEND
            row_source:
              mode: EXPLODE
              json_path: $.changelog.histories
              inherit:
                issue_key: $.key
            columns:
              - name: history_id
                json_path: $.id
    """)
    with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        tasks = parse_yaml_config(f.name)

    assert len(tasks) == 1
    t = tasks[0]
    assert len(t.outputs) == 2
    assert t.outputs[0].table == "issues"
    assert t.outputs[0].write_mode == WriteMode.UPSERT
    assert t.outputs[1].table == "changelog"
    assert t.outputs[1].write_mode == WriteMode.APPEND
    assert t.outputs[1].row_source.mode == "EXPLODE"


# --- resolve_params ---

def test_resolve_params_no_wildcard():
    from core.types import TaskConfig
    task = TaskConfig(
        name="t", tool="fake", command="cmd",
        params={"jql": "project = X"},
    )
    result = resolve_params(task, FakeTool({}))
    assert result == [{"jql": "project = X"}]


def test_resolve_params_wildcard():
    from core.types import TaskConfig
    task = TaskConfig(
        name="t", tool="fake", command="sprints",
        params={"board_id": "*"},
    )
    tool = FakeTool({})
    tool._wildcard_values = {"board_id": [1, 2, 3]}
    result = resolve_params(task, tool)
    assert len(result) == 3
    assert result[0] == {"board_id": 1}
    assert result[2] == {"board_id": 3}


# --- run_task (UPSERT) ---

def test_run_task_upsert(db):
    task = _make_task(
        name="test_boards",
        command="boards",
        table="boards",
        write_mode=WriteMode.UPSERT,
        primary_keys=["id"],
        columns=[
            ColumnDef(name="id", json_path="$.id"),
            ColumnDef(name="board_name", json_path="$.name"),
        ],
    )
    tool = FakeTool({
        "boards": [
            {"id": 1, "name": "Board A"},
            {"id": 2, "name": "Board B"},
        ]
    })

    count = run_task(task, tool, db)
    assert count == 2

    rows = db.execute("SELECT * FROM boards ORDER BY id")
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["board_name"] == "Board A"


def test_run_task_upsert_deduplicates(db):
    """Running same UPSERT task twice should upsert (not duplicate)."""
    task = _make_task(
        name="test_boards",
        command="boards",
        table="boards",
        write_mode=WriteMode.UPSERT,
        primary_keys=["id"],
        columns=[
            ColumnDef(name="id", json_path="$.id"),
            ColumnDef(name="board_name", json_path="$.name"),
        ],
    )
    tool = FakeTool({
        "boards": [
            {"id": 1, "name": "Board A"},
        ]
    })

    run_task(task, tool, db)
    # Update the name
    tool._data["boards"] = [{"id": 1, "name": "Board A Updated"}]
    run_task(task, tool, db)

    rows = db.execute("SELECT * FROM boards")
    assert len(rows) == 1
    assert rows[0]["board_name"] == "Board A Updated"


# --- run_task (APPEND) ---

def test_run_task_append(db):
    task = _make_task(
        name="test_events",
        command="events",
        table="events",
        write_mode=WriteMode.APPEND,
        primary_keys=[],
        columns=[
            ColumnDef(name="event", json_path="$.event"),
            ColumnDef(name="detail", json_path="$.detail"),
        ],
    )
    tool = FakeTool({
        "events": [{"event": "deploy", "detail": "v1.0"}]
    })

    count = run_task(task, tool, db)
    assert count == 1

    rows = db.execute("SELECT * FROM events")
    assert len(rows) == 1
    assert rows[0]["event"] == "deploy"
    assert rows[0]["inserted_at"] is not None


def test_run_task_append_duplicates(db):
    """Running same APPEND task twice should append (not upsert)."""
    task = _make_task(
        name="test_events",
        command="events",
        table="events",
        write_mode=WriteMode.APPEND,
        primary_keys=[],
        columns=[
            ColumnDef(name="event", json_path="$.event"),
        ],
    )
    tool = FakeTool({
        "events": [{"event": "deploy"}]
    })

    run_task(task, tool, db)
    run_task(task, tool, db)

    rows = db.execute("SELECT * FROM events")
    assert len(rows) == 2  # Appended, not upserted


# --- run_task (REPLACE) ---

def test_run_task_replace_truncates(db):
    """REPLACE mode should delete existing data before inserting."""
    task = _make_task(
        name="test_teams",
        command="teams",
        table="teams",
        write_mode=WriteMode.REPLACE,
        primary_keys=["id"],
        columns=[
            ColumnDef(name="id", json_path="$.id"),
            ColumnDef(name="team_name", json_path="$.name"),
        ],
    )
    tool = FakeTool({
        "teams": [
            {"id": 1, "name": "Alpha"},
            {"id": 2, "name": "Beta"},
        ]
    })

    run_task(task, tool, db)
    rows = db.execute("SELECT * FROM teams ORDER BY id")
    assert len(rows) == 2

    # Second run with different data — old rows should be gone
    tool._data["teams"] = [{"id": 3, "name": "Gamma"}]
    run_task(task, tool, db)

    rows = db.execute("SELECT * FROM teams ORDER BY id")
    assert len(rows) == 1
    assert rows[0]["id"] == 3
    assert rows[0]["team_name"] == "Gamma"


def test_run_task_replace_no_inserted_at(db):
    """REPLACE tables should NOT have inserted_at column."""
    task = _make_task(
        name="test_teams",
        command="teams",
        table="teams",
        write_mode=WriteMode.REPLACE,
        primary_keys=["id"],
        columns=[ColumnDef(name="id", json_path="$.id")],
    )
    tool = FakeTool({"teams": [{"id": 1}]})
    run_task(task, tool, db)

    cols = db.get_columns("teams")
    assert "inserted_at" not in cols


# --- run_task (EXPLODE) ---

def test_run_task_explode(db):
    """EXPLODE mode should fan out array elements into individual rows."""
    task = _make_task(
        name="test_changelog",
        command="issues",
        table="changelog",
        write_mode=WriteMode.APPEND,
        row_source=RowSource(
            mode="EXPLODE",
            json_path="$.changelog.histories",
            inherit={"issue_key": "$.key"},
        ),
        columns=[
            ColumnDef(name="history_id", json_path="$.id"),
            ColumnDef(name="author", json_path="$.author.displayName", type="STRING"),
        ],
    )
    tool = FakeTool({
        "issues": [{
            "key": "ENG-101",
            "changelog": {
                "histories": [
                    {"id": "1001", "author": {"displayName": "Alice"}},
                    {"id": "1002", "author": {"displayName": "Bob"}},
                ],
            },
        }]
    })

    count = run_task(task, tool, db)
    assert count == 1  # 1 source record fetched

    rows = db.execute("SELECT * FROM changelog ORDER BY history_id")
    assert len(rows) == 2
    assert rows[0]["issue_key"] == "ENG-101"
    assert rows[0]["history_id"] == "1001"
    assert rows[0]["author"] == "Alice"
    assert rows[1]["history_id"] == "1002"
    assert rows[1]["author"] == "Bob"


def test_run_task_explode_empty_array(db):
    """EXPLODE on a record with no matching array should produce 0 rows."""
    task = _make_task(
        name="test_explode_empty",
        command="issues",
        table="changelog_empty",
        write_mode=WriteMode.APPEND,
        row_source=RowSource(mode="EXPLODE", json_path="$.changelog.histories"),
        columns=[ColumnDef(name="history_id", json_path="$.id")],
    )
    tool = FakeTool({
        "issues": [{"key": "ENG-1", "changelog": {"histories": []}}]
    })

    count = run_task(task, tool, db)
    assert count == 1  # 1 source record fetched (even though 0 rows written)


def test_run_task_explode_debug_mode(db, capsys):
    """EXPLODE in debug mode should print raw source records."""
    task = _make_task(
        name="test",
        command="issues",
        table="cl",
        write_mode=WriteMode.APPEND,
        row_source=RowSource(mode="EXPLODE", json_path="$.items"),
        columns=[ColumnDef(name="val", json_path="$.val")],
    )
    tool = FakeTool({
        "issues": [{"items": [{"val": "a"}, {"val": "b"}]}]
    })

    count = run_task(task, tool, db, debug=True)
    assert count == 1  # 1 source record

    captured = capsys.readouterr()
    lines = [line for line in captured.out.strip().split("\n") if line]
    assert len(lines) == 1  # Raw source record printed once


# --- _explode_records ---

def test_explode_records_basic():
    output = OutputConfig(
        table="t", write_mode=WriteMode.APPEND,
        row_source=RowSource(mode="EXPLODE", json_path="$.items", inherit={"parent": "$.id"}),
        columns=[ColumnDef(name="val", json_path="$.val")],
    )
    records = [{"id": "P1", "items": [{"val": "a"}, {"val": "b"}]}]
    result = _explode_records(output, records, {"ctx": "v"})

    assert len(result) == 2
    assert result[0] == ({"val": "a"}, {"ctx": "v", "parent": "P1"})
    assert result[1] == ({"val": "b"}, {"ctx": "v", "parent": "P1"})


def test_explode_records_no_array():
    output = OutputConfig(
        table="t", write_mode=WriteMode.APPEND,
        row_source=RowSource(mode="EXPLODE", json_path="$.missing"),
        columns=[ColumnDef(name="val", json_path="$.val")],
    )
    records = [{"id": 1}]
    result = _explode_records(output, records, {})
    assert result == []


# --- run_task with wildcard + context ---

def test_run_task_wildcard_with_context(db):
    task = _make_task(
        name="test_sprints",
        command="sprints",
        params={"board_id": "*"},
        context={"board_id": "board_id"},
        table="sprints",
        write_mode=WriteMode.UPSERT,
        primary_keys=["board_id", "id"],
        columns=[
            ColumnDef(name="id", json_path="$.id"),
            ColumnDef(name="sprint_name", json_path="$.name"),
        ],
    )
    tool = FakeTool({
        "sprints:10": [
            {"id": 1, "name": "Sprint 1"},
            {"id": 2, "name": "Sprint 2"},
        ],
        "sprints:20": [
            {"id": 3, "name": "Sprint 3"},
        ],
    })
    tool._wildcard_values = {"board_id": [10, 20]}

    count = run_task(task, tool, db)
    assert count == 3

    rows = db.execute("SELECT * FROM sprints ORDER BY id")
    assert len(rows) == 3
    assert rows[0]["board_id"] == "10"  # Context value stored as TEXT
    assert rows[0]["sprint_name"] == "Sprint 1"
    assert rows[2]["board_id"] == "20"


# --- run_task with nested json_path ---

def test_run_task_nested_json_path(db):
    task = _make_task(
        name="test_tickets",
        command="search",
        table="tickets",
        write_mode=WriteMode.UPSERT,
        primary_keys=["key"],
        columns=[
            ColumnDef(name="key", json_path="$.key"),
            ColumnDef(name="summary", json_path="$.fields.summary"),
            ColumnDef(name="status", json_path="$.fields.status.name"),
            ColumnDef(name="changelog_json", json_path="$.changelog", type="JSON"),
        ],
    )
    tool = FakeTool({
        "search": [{
            "key": "ENG-101",
            "fields": {
                "summary": "Fix bug",
                "status": {"name": "Done"},
            },
            "changelog": {"histories": [{"id": 1}]},
        }]
    })

    run_task(task, tool, db)

    rows = db.execute("SELECT * FROM tickets")
    assert len(rows) == 1
    assert rows[0]["key"] == "ENG-101"
    assert rows[0]["summary"] == "Fix bug"
    assert rows[0]["status"] == "Done"
    # changelog stored as JSON string
    assert json.loads(rows[0]["changelog_json"]) == {"histories": [{"id": 1}]}


# --- debug mode ---

def test_run_task_debug_mode(db, capsys):
    task = _make_task(
        name="test",
        command="boards",
        table="boards",
        write_mode=WriteMode.UPSERT,
        primary_keys=["id"],
        columns=[ColumnDef(name="id", json_path="$.id")],
    )
    tool = FakeTool({"boards": [{"id": 1}, {"id": 2}]})

    count = run_task(task, tool, db, debug=True)
    assert count == 2

    captured = capsys.readouterr()
    lines = [line for line in captured.out.strip().split("\n") if line]
    assert len(lines) == 2

    # DB should be untouched
    assert not db.table_exists("boards")


# --- _compute_since ---

def test_compute_since_no_config():
    from core.types import TaskConfig
    task = TaskConfig(name="t", tool="x", command="y")
    assert _compute_since(task, None) is None


def test_compute_since_yaml_only():
    from core.types import TaskConfig
    task = TaskConfig(name="t", tool="x", command="y", since="2024-06-01")
    assert _compute_since(task, None) == "2024-06-01"


def test_compute_since_cli_override_wins():
    from core.types import TaskConfig
    task = TaskConfig(name="t", tool="x", command="y", since="2024-06-01")
    assert _compute_since(task, None, since_override="2025-01-01") == "2025-01-01"


def test_compute_since_incremental_from_db(db):
    from core.types import TaskConfig
    db.ensure_meta_table()
    db.record_run("my_task", "2025-03-01T00:00:00Z", "2025-03-01T00:05:00Z", 50, "success")
    task = TaskConfig(name="my_task", tool="x", command="y", incremental=True)
    assert _compute_since(task, db) == "2025-03-01T00:00:00Z"


def test_compute_since_incremental_db_newer_than_yaml(db):
    from core.types import TaskConfig
    db.ensure_meta_table()
    db.record_run("my_task", "2025-03-01T00:00:00Z", "2025-03-01T00:05:00Z", 50, "success")
    task = TaskConfig(name="my_task", tool="x", command="y", since="2024-06-01", incremental=True)
    # DB is newer, so it wins
    assert _compute_since(task, db) == "2025-03-01T00:00:00Z"


def test_compute_since_yaml_newer_than_db(db):
    from core.types import TaskConfig
    db.ensure_meta_table()
    db.record_run("my_task", "2024-01-01T00:00:00Z", "2024-01-01T00:05:00Z", 50, "success")
    task = TaskConfig(name="my_task", tool="x", command="y", since="2024-06-01", incremental=True)
    # YAML is newer, so it wins
    assert _compute_since(task, db) == "2024-06-01"


def test_compute_since_incremental_no_prior_runs(db):
    from core.types import TaskConfig
    db.ensure_meta_table()
    task = TaskConfig(name="my_task", tool="x", command="y", incremental=True)
    assert _compute_since(task, db) is None


# --- run_task records runs ---

def test_run_task_records_run(db):
    db.ensure_meta_table()
    task = _make_task(
        name="test_boards",
        command="boards",
        table="boards",
        write_mode=WriteMode.UPSERT,
        primary_keys=["id"],
        columns=[ColumnDef(name="id", json_path="$.id")],
    )
    tool = FakeTool({"boards": [{"id": 1}]})

    run_task(task, tool, db)

    rows = db.execute(
        "SELECT * FROM _task_runs WHERE task_name = 'test_boards' AND param_key IS NULL"
    )
    assert len(rows) == 1
    assert rows[0]["status"] == "success"
    assert rows[0]["record_count"] == 1


def test_run_task_passes_since_to_tool(db):
    """Verify since is injected into params passed to the tool."""
    received_params = []

    class CaptureTool(BaseTool):
        def run(self, command, params=None):
            received_params.append(dict(params or {}))
            yield {"id": 1}
        def list_commands(self):
            return ["cmd"]

    db.ensure_meta_table()
    task = _make_task(
        name="test",
        command="cmd",
        table="t",
        write_mode=WriteMode.UPSERT,
        primary_keys=["id"],
        columns=[ColumnDef(name="id", json_path="$.id")],
        since="2024-06-01",
    )

    run_task(task, CaptureTool(), db)

    assert received_params[0]["since"] == "2024-06-01"


# --- parse_yaml_config with since/incremental ---

def test_parse_yaml_with_since_and_incremental():
    yaml_content = textwrap.dedent("""\
    tasks:
      - name: test_task
        tool: fake
        command: cmd
        incremental: true
        since: "2024-06-01"
        params: {}
        output:
          table: t
          write_mode: UPSERT
          primary_keys: [id]
          columns:
            - name: id
              json_path: $.id
    """)
    with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        tasks = parse_yaml_config(f.name)

    t = tasks[0]
    assert t.incremental is True
    assert t.since == "2024-06-01"


# --- _coerce_to_text ---

def test_coerce_to_text_dict_with_value():
    """Jira select field: {"value": "...", "id": "...", "self": "..."}"""
    val = {"self": "https://example.com", "value": "Product A", "id": "123"}
    assert _coerce_to_text(val) == "Product A"


def test_coerce_to_text_dict_with_name():
    """Jira objects like status/priority: {"name": "High", "id": "2"}"""
    val = {"name": "High", "id": "2"}
    assert _coerce_to_text(val) == "High"


def test_coerce_to_text_dict_value_over_name():
    """'value' key takes precedence over 'name'."""
    val = {"value": "the value", "name": "the name"}
    assert _coerce_to_text(val) == "the value"


def test_coerce_to_text_list_of_dicts():
    """Jira multi-select: [{"value": "A"}, {"value": "B"}]"""
    val = [
        {"self": "https://example.com/1", "value": "Alpha", "id": "1"},
        {"self": "https://example.com/2", "value": "Beta", "id": "2"},
    ]
    assert _coerce_to_text(val) == "Alpha, Beta"


def test_coerce_to_text_list_of_strings():
    """Labels are already lists of strings."""
    assert _coerce_to_text(["backend", "auth"]) == "backend, auth"


def test_coerce_to_text_passthrough():
    """Plain values pass through unchanged."""
    assert _coerce_to_text("hello") == "hello"
    assert _coerce_to_text(42) == 42
    assert _coerce_to_text(None) is None


def test_coerce_to_text_empty_list():
    assert _coerce_to_text([]) == ""


def test_map_record_coerces_text_columns(db):
    """TEXT columns with complex values should be coerced to plain strings."""
    task = _make_task(
        name="test",
        command="cmd",
        table="t",
        write_mode=WriteMode.UPSERT,
        primary_keys=["key"],
        columns=[
            ColumnDef(name="key", json_path="$.key"),
            ColumnDef(name="product", json_path="$.fields.product", type="STRING"),
            ColumnDef(name="labels", json_path="$.fields.labels", type="JSON"),
        ],
    )
    record = {
        "key": "X-1",
        "fields": {
            "product": [{"value": "Widget", "id": "1", "self": "https://..."}],
            "labels": ["a", "b"],
        },
    }
    tool = FakeTool({"cmd": [record]})
    db.ensure_meta_table()

    run_task(task, tool, db)

    rows = db.execute("SELECT * FROM t")
    assert rows[0]["product"] == "Widget"
    # JSON column should remain as serialized JSON, not coerced
    assert json.loads(rows[0]["labels"]) == ["a", "b"]


# --- Multi-output ---

def test_run_task_multi_output(db):
    """A single task with multiple outputs should write to multiple tables from one fetch."""
    from core.types import TaskConfig
    task = TaskConfig(
        name="test_multi",
        tool="fake",
        command="issues",
        outputs=[
            OutputConfig(
                table="issues",
                write_mode=WriteMode.UPSERT,
                primary_keys=["key"],
                columns=[
                    ColumnDef(name="key", json_path="$.key"),
                    ColumnDef(name="summary", json_path="$.fields.summary"),
                ],
            ),
            OutputConfig(
                table="changelog",
                write_mode=WriteMode.APPEND,
                row_source=RowSource(
                    mode="EXPLODE",
                    json_path="$.changelog.histories",
                    inherit={"issue_key": "$.key"},
                ),
                columns=[
                    ColumnDef(name="history_id", json_path="$.id"),
                    ColumnDef(name="author", json_path="$.author.displayName", type="STRING"),
                ],
            ),
        ],
    )
    tool = FakeTool({
        "issues": [{
            "key": "ENG-1",
            "fields": {"summary": "Fix bug"},
            "changelog": {
                "histories": [
                    {"id": "h1", "author": {"displayName": "Alice"}},
                    {"id": "h2", "author": {"displayName": "Bob"}},
                ],
            },
        }]
    })

    count = run_task(task, tool, db)
    assert count == 1  # 1 source record

    # Check issues table
    issues = db.execute("SELECT * FROM issues")
    assert len(issues) == 1
    assert issues[0]["key"] == "ENG-1"
    assert issues[0]["summary"] == "Fix bug"

    # Check changelog table
    cl = db.execute("SELECT * FROM changelog ORDER BY history_id")
    assert len(cl) == 2
    assert cl[0]["issue_key"] == "ENG-1"
    assert cl[0]["history_id"] == "h1"
    assert cl[0]["author"] == "Alice"
    assert cl[1]["history_id"] == "h2"


# --- per-param_set progress tracking ---

def test_wildcard_task_records_per_param_set(db):
    """Wildcard tasks should record a run per param_set plus a task-level run."""
    task = _make_task(
        name="test_sprints",
        command="sprints",
        params={"board_id": "*"},
        context={"board_id": "board_id"},
        table="sprints",
        write_mode=WriteMode.UPSERT,
        primary_keys=["board_id", "id"],
        columns=[
            ColumnDef(name="id", json_path="$.id"),
            ColumnDef(name="sprint_name", json_path="$.name"),
        ],
    )
    tool = FakeTool({
        "sprints:10": [{"id": 1, "name": "Sprint 1"}],
        "sprints:20": [{"id": 2, "name": "Sprint 2"}],
    })
    tool._wildcard_values = {"board_id": [10, 20]}

    run_task(task, tool, db)

    # Per-param_set runs
    param_runs = db.execute(
        "SELECT * FROM _task_runs WHERE task_name = 'test_sprints' AND param_key IS NOT NULL "
        "ORDER BY param_key"
    )
    assert len(param_runs) == 2
    assert param_runs[0]["param_key"] == "board_id=10"
    assert param_runs[1]["param_key"] == "board_id=20"

    # Task-level run
    task_runs = db.execute(
        "SELECT * FROM _task_runs WHERE task_name = 'test_sprints' AND param_key IS NULL"
    )
    assert len(task_runs) == 1
    assert task_runs[0]["record_count"] == 2


def test_compute_since_with_param_key(db):
    """Per-param_set since should use param_key-specific last run."""
    from core.types import TaskConfig
    db.ensure_meta_table()
    db.record_run("my_task", "2025-03-01T00:00:00Z", "2025-03-01T00:05:00Z", 10, "success", param_key="board_id=1")
    task = TaskConfig(name="my_task", tool="x", command="y", incremental=True)
    assert _compute_since(task, db, param_key="board_id=1") == "2025-03-01T00:00:00Z"
    # Unknown param_key falls back to task-level (which is None here)
    assert _compute_since(task, db, param_key="board_id=99") is None


def test_compute_since_param_key_falls_back_to_task_level(db):
    """When no param_key run exists, fall back to task-level last run."""
    from core.types import TaskConfig
    db.ensure_meta_table()
    db.record_run("my_task", "2025-01-01T00:00:00Z", "2025-01-01T00:05:00Z", 50, "success")
    task = TaskConfig(name="my_task", tool="x", command="y", incremental=True)
    # No param_key run, but task-level exists
    assert _compute_since(task, db, param_key="board_id=99") == "2025-01-01T00:00:00Z"


# --- Schema evolution: new columns added after table exists ---

def test_new_column_added_to_yaml_is_written_on_next_run(db):
    """When a new column is added to the extractor YAML after the table already exists,
    re-running the task should (a) add the column to the schema and (b) populate it
    via UPSERT for every record returned by the tool.

    Regression: adding additions/deletions to github_pr_commits after initial extraction.
    """
    base_columns = [
        ColumnDef(name="sha", json_path="$.sha"),
        ColumnDef(name="message", json_path="$.message"),
    ]
    extended_columns = [
        ColumnDef(name="sha", json_path="$.sha"),
        ColumnDef(name="message", json_path="$.message"),
        ColumnDef(name="additions", json_path="$.additions"),
        ColumnDef(name="deletions", json_path="$.deletions"),
    ]
    records = [
        {"sha": "abc123", "message": "fix bug", "additions": 10, "deletions": 2},
        {"sha": "def456", "message": "add feature", "additions": 50, "deletions": 5},
    ]

    # First run: table created with base columns only
    task_v1 = _make_task(
        name="test_commits",
        command="commits",
        table="commits",
        write_mode=WriteMode.UPSERT,
        primary_keys=["sha"],
        columns=base_columns,
    )
    tool = FakeTool({"commits": records})
    run_task(task_v1, tool, db)

    cols_after_v1 = db.get_columns("commits")
    assert "additions" not in cols_after_v1
    assert "deletions" not in cols_after_v1

    # Second run: YAML updated with new columns, same records returned by tool
    task_v2 = _make_task(
        name="test_commits",
        command="commits",
        table="commits",
        write_mode=WriteMode.UPSERT,
        primary_keys=["sha"],
        columns=extended_columns,
    )
    run_task(task_v2, tool, db)

    # New columns should exist in schema
    cols_after_v2 = db.get_columns("commits")
    assert "additions" in cols_after_v2
    assert "deletions" in cols_after_v2

    # Existing rows should have the new column values populated (not NULL)
    rows = db.execute("SELECT * FROM commits ORDER BY sha")
    assert len(rows) == 2
    assert rows[0]["sha"] == "abc123"
    assert rows[0]["additions"] == 10
    assert rows[0]["deletions"] == 2
    assert rows[1]["sha"] == "def456"
    assert rows[1]["additions"] == 50
    assert rows[1]["deletions"] == 5


def test_new_column_null_when_not_in_api_response(db):
    """New columns whose json_path resolves to nothing should be NULL, not error."""
    base_columns = [ColumnDef(name="sha", json_path="$.sha")]
    extended_columns = [
        ColumnDef(name="sha", json_path="$.sha"),
        ColumnDef(name="new_field", json_path="$.new_field"),
    ]
    records = [{"sha": "abc123"}]  # new_field not present in API response

    task_v1 = _make_task(
        name="test_commits2",
        command="commits",
        table="commits2",
        write_mode=WriteMode.UPSERT,
        primary_keys=["sha"],
        columns=base_columns,
    )
    run_task(task_v1, FakeTool({"commits": records}), db)

    task_v2 = _make_task(
        name="test_commits2",
        command="commits",
        table="commits2",
        write_mode=WriteMode.UPSERT,
        primary_keys=["sha"],
        columns=extended_columns,
    )
    run_task(task_v2, FakeTool({"commits": records}), db)

    rows = db.execute("SELECT * FROM commits2")
    assert len(rows) == 1
    assert rows[0]["new_field"] is None
