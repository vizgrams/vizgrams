# Writing a Custom Tool

If the built-in tools (git, jira, file) don't cover your data source, you can write a custom tool and register it in your model's `config.yaml`.

→ [[tools-and-extractors|← Back to Tools & Extractors]]

---

## What a tool does

A tool is a Python class that:

1. Accepts configuration (credentials, connection details) at initialisation
2. Exposes named **commands** — each command knows how to query a data source and yield rows as dicts
3. Optionally supports **wildcard expansion** — resolving `"*"` params to a concrete list

---

## Minimal example

```python
# tools/custom/my_tool.py

from typing import Iterator


class MyTool:
    def __init__(self, config: dict):
        """
        config is the tool's block from config.yaml, with credentials resolved.
        e.g. {"enabled": True, "base_url": "https://api.example.com", "token": "abc123"}
        """
        self.base_url = config["base_url"]
        self.token = config["token"]

    def list_commands(self) -> list[str]:
        """Return the names of all supported commands."""
        return ["widgets", "widget_events"]

    def run(self, command: str, params: dict) -> Iterator[dict]:
        """
        Run a command with the given params and yield one dict per row.
        Each dict maps column names to values.
        """
        if command == "widgets":
            yield from self._get_widgets(params)
        elif command == "widget_events":
            yield from self._get_widget_events(params)
        else:
            raise ValueError(f"Unknown command: {command}")

    def _get_widgets(self, params: dict) -> Iterator[dict]:
        import requests
        resp = requests.get(
            f"{self.base_url}/widgets",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        resp.raise_for_status()
        for item in resp.json():
            yield {
                "id": item["id"],
                "name": item["name"],
                "created_at": item["createdAt"],
                "status": item["status"],
            }

    def _get_widget_events(self, params: dict) -> Iterator[dict]:
        widget_id = params.get("widget_id")
        import requests
        resp = requests.get(
            f"{self.base_url}/widgets/{widget_id}/events",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        resp.raise_for_status()
        for event in resp.json():
            yield {
                "widget_id": widget_id,
                "event_type": event["type"],
                "occurred_at": event["timestamp"],
            }
```

---

## Supporting wildcard expansion

If a command supports `"*"` as a param value (e.g. run against all widgets), implement `resolve_wildcard`:

```python
def resolve_wildcard(self, param_name: str, param_value: str) -> list:
    """
    Called when a param has value "*". Return the concrete list to iterate over.
    """
    if param_name == "widget_id":
        import requests
        resp = requests.get(
            f"{self.base_url}/widgets",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        resp.raise_for_status()
        return [str(w["id"]) for w in resp.json()]
    return [param_value]
```

With this in place, an extractor task can use `widget_id: "*"` and the engine will call `_get_widget_events` once per widget.

---

## Registering the tool

Add the tool to your model's `config.yaml`:

```yaml
# models/my_model/config.yaml

tools:
  my_tool:
    enabled: true
    module: tools.custom.my_tool    # Python module path
    class: MyTool                   # Class name
    base_url: "https://api.example.com"
    token: "file:~/.secrets/my_tool_token"
```

The `module` and `class` fields tell the engine where to find your tool. All other fields in the tool block are passed to `__init__` as the `config` dict, with credential values resolved before they reach your code.

---

## Using the tool in an extractor

Once registered, reference the tool by its key (`my_tool`) in extractor YAML exactly as you would a built-in tool:

```yaml
# models/my_model/extractors/extractor_my_tool.yaml

tasks:
  - name: widgets
    tool: my_tool
    command: widgets
    output:
      table: my_tool_widgets
      write_mode: UPSERT
      primary_keys: [id]
      columns:
        - name: id
          json_path: $.id
        - name: name
          json_path: $.name
        - name: created_at
          json_path: $.created_at
        - name: status
          json_path: $.status

  - name: widget_events
    tool: my_tool
    command: widget_events
    params:
      widget_id: "*"               # wildcard — runs once per widget
    context:
      widget_id: widget_id         # capture the expanded value as a column
    output:
      table: my_tool_widget_events
      write_mode: APPEND
      columns:
        - name: widget_id
          json_path: $.widget_id
        - name: event_type
          json_path: $.event_type
        - name: occurred_at
          json_path: $.occurred_at
```

---

## Tips

**Yield dicts, not lists.** Each `yield` from `run()` should be a flat `{column: value}` dict. Nested structures should be serialised to JSON strings if you want to store them (use `type: JSON` in the extractor column definition and return the nested object — the engine will serialise it).

**Keep `run()` lazy.** Use `yield` (generator) rather than building a list and returning it. This keeps memory usage flat for large datasets.

**Handle pagination.** The tool is responsible for following pagination tokens/cursors. The engine just iterates whatever `run()` yields.

**Use incremental params.** If the extractor task sets `incremental: true`, the engine will pass a `since` key in `params` with the last-run ISO timestamp. Your `run()` method can check for `params.get("since")` and apply it as a filter to your API call.

```python
def _get_widgets(self, params: dict) -> Iterator[dict]:
    query_params = {}
    if since := params.get("since"):
        query_params["updated_after"] = since
    # ...
```
