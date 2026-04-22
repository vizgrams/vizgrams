# Reference — YAML Schemas & CLI

Quick reference for YAML field definitions and CLI commands across all model components.

→ [[index|← Back to home]]

---

## Table naming conventions

| Source | Table prefix | Example |
|--------|-------------|---------|
| Extractor output | `raw_` | `raw_github_pull_requests` |
| Entity semantic table | `sem_` | `sem_pull_request` |
| Entity event table | `sem_` + `_event` | `sem_pull_request_lifecycle_event` |
| Feature store | `feature_value` | (all features in one table) |

Entity names are PascalCase in YAML; tables use `snake_case`:
- `PullRequest` → `sem_pull_request`
- `SprintDelivery` → `sem_sprint_delivery`

---

## Extractor YAML

```yaml
tasks:
  - name: <string>                  # unique task name
    tool: <git|jira|git_codeowners|file>
    command: <string>               # tool-specific command name
    incremental: <bool>             # default false
    params:
      <param>: <value|"*">
    context:
      <param>: <column_name>        # column that receives the expanded wildcard value
    output:                         # singular: one output table
      table: <string>               # stored as raw_<table>
      write_mode: <UPSERT|APPEND|REPLACE>
      primary_keys: [<col>, ...]    # required for UPSERT
      columns:
        - name: <string>
          json_path: <$.path>
          type: <STRING|INTEGER|FLOAT|JSON>   # default STRING
      row_source:                   # optional: explode a nested array
        mode: EXPLODE
        json_path: <$.array.path>
        inherit:
          <column>: <$.parent.field>
    outputs:                        # plural: multiple output tables from one API call
      - table: ...
        # same fields as output above
```

---

## Ontology YAML

```yaml
entity: <PascalCase>
description: <string>

identity:
  <column_name>:
    type: <STRING|INTEGER|FLOAT>
    semantic: PRIMARY_KEY

attributes:
  <column_name>:
    type: <STRING|INTEGER|FLOAT>
    semantic: <PRIMARY_KEY|IDENTIFIER|TIMESTAMP|MEASURE|ATTRIBUTE|RELATION|ENTITY|STATE|INSERTED_AT|SCD_FROM|SCD_TO|ORDERING>

events:
  <event_name>:
    description: <string>
    attributes:
      <column_name>:
        type: <STRING|INTEGER|FLOAT>
        semantic: <...>

relations:
  <relation_name>:
    target: <PascalCase|dynamic(field_name)>
    via: <column>                           # or: local_col > target_pk
    cardinality: <MANY_TO_ONE|ONE_TO_MANY>
```

---

## Mapper YAML

```yaml
mapper: <string>
description: <string>
depends_on: [<mapper_name>, ...]

grain: <alias>                    # primary source alias (omit for multi-group)

enums:
  <mapping_name>:
    <CANONICAL>: [raw_value, ...]

sources:
  - alias: <string>
    table: <raw_*|sem_*>
    columns: [<col>, ...]
    filter: <expression>          # or structured filter block
    deduplicate: [<col>, ...]

joins:
  - from: <alias>
    to: <alias>
    type: <left|inner|right|full>
    "on":
      - left: <alias.col>
        right: <alias.col>
        operator: <eq|json_array_contains|scalar_or_array_contains>

targets:
  - entity: <PascalCase>
    event: <event_name>           # optional: write to event table
    columns:
      - name: <string>
        expr: <expression>
    rows:                         # alternative to columns: for multi-group
      - from: <alias>
        joins: [...]
        columns: [...]
```

---

## Feature YAML

```yaml
feature_id: <entity_lower>.<feature_name>
name: <string>
description: <string>
entity_type: <PascalCase>
entity_key: <primary_key_column>
data_type: <STRING|INTEGER|FLOAT>
materialization_mode: <materialized|dynamic>
dependencies: [<feature_id>, ...]
expr: <expression>
```

---

## Query YAML

```yaml
name: <string>            # must match filename stem
root: <PascalCase>

attributes:
  - <alias>: <expression>
    order: <N, asc|desc>  # optional

measures:
  - <alias>:
      expr: <expression>
      format:
        type: <number|percent|duration>
        unit: <hours|days|weeks>        # for duration type
        pattern: <numeral.js pattern>
      window:
        method: <weighted|simple|cumulative|lag|lead>
        unit: <month|week|...>          # must match a format_time grain
        frame: <int>                    # for weighted/simple
        offset: <int>                   # for lag/lead
      thresholds:
        - op: <">"|">="|"<"|"<="|"=="|"!=">
          value: <number>
          status: <red|amber|green>

where:
  - <expression>
```

---

## API reference

All operations are available through the REST API at `http://localhost:8000`. With the server running:

- **`/docs`** — interactive Swagger UI; try any endpoint directly in the browser
- **`/redoc`** — ReDoc reference

All model-scoped routes follow the pattern `/api/v1/model/{model}/<resource>`.
Extractor and mapper execution are asynchronous — they return a job immediately; poll `/api/v1/model/{model}/job/{job_id}` for status and progress.

---

## Semantic hints reference

| Hint | Usage |
|------|-------|
| `PRIMARY_KEY` | Unique row identifier — exactly one per entity |
| `IDENTIFIER` | Human-readable label (e.g. `display_name`, `title`) |
| `TIMESTAMP` | ISO datetime string; enables `format_time` and `now()` filters |
| `MEASURE` | Numeric value suitable for aggregation |
| `ATTRIBUTE` | Plain descriptive field (default) |
| `RELATION` | Foreign key to another entity |
| `ENTITY` | Holds an entity type name (for polymorphic relations) |
| `STATE` | Lifecycle state value |
| `INSERTED_AT` | Auto-timestamp for append event rows (required on all events) |
| `SCD_FROM` | Start of a slowly-changing-dimension validity range |
| `SCD_TO` | End of a slowly-changing-dimension validity range |
| `ORDERING` | Sequence or sort column |

---

## Credential resolution

| Format | Example | Behaviour |
|--------|---------|-----------|
| `file:<path>` | `file:~/.secrets/token` | Reads file, strips whitespace |
| `env:<VAR>` | `env:GITHUB_PAT` | Reads environment variable |
| Literal | `my-secret` | Used as-is |
