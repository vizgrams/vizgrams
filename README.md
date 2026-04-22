# Vizgrams — Software Lifecycle Data Pipeline

Extracts raw data from external systems into a local SQLite database, models it as a semantic layer of typed entities, and exposes a query engine for computing reusable measurements over those entities. All configuration is declarative YAML.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Extractor YAML          (what to extract)              │
│  models/<model>/extractors/extractor_jira.yaml          │
│  models/<model>/extractors/extractor_github.yaml        │
└────────────────┬────────────────────────────────────────┘
                 │ parsed by
                 ▼
┌─────────────────────────────────────────────────────────┐
│  Engine                  (how to extract)               │
│  engine/extractor.py     orchestrator: wildcard          │
│                          expansion, column mapping       │
│  engine/schema.py        type inference, table creation  │
└────────┬───────────────────────────┬────────────────────┘
         │ calls                     │ writes to
         ▼                           ▼
┌──────────────────┐    ┌─────────────────────────────────┐
│  Tools            │    │  SQLite (models/<model>/data/)  │
│  tools/jira/      │    │                                 │
│  tools/git/       │    │  raw_*    (extractor tables)    │
│  tools/file/      │    │  sem_*    (semantic tables)     │
│                   │    │                                 │
│  Connect to APIs  │    │                                 │
│  Yield raw dicts  │    │                                 │
└──────────────────┘    └─────────────────────────────────┘
                                     ▲
                                     │ mapped from raw_* by
┌─────────────────────────────────────────────────────────┐
│  Semantic Layer          (what it means)                │
│  models/<model>/ontology/  entity definitions           │
│  models/<model>/mappers/   mapping rules raw → sem      │
│  models/<model>/features/  computed columns (SQL expr.) │
│  semantic/                 parser, mapper, expression   │
│                            engine, query evaluator      │
└─────────────────────────────────────────────────────────┘
                                     ▲
                                     │ queried by
┌─────────────────────────────────────────────────────────┐
│  Query Layer             (what to measure)              │
│  models/<model>/queries/   query YAML definitions       │
│  engine/query_runner.py    GROUP BY builder, join       │
│                            resolver, window SQL         │
└─────────────────────────────────────────────────────────┘
                                     ▲
                                     │ served by
┌─────────────────────────────────────────────────────────┐
│  REST API                (api/)                         │
│  api/main.py             FastAPI app, all routes        │
│  api/services/           shared business logic          │
│  api/routers/            HTTP handlers per resource     │
└────────────────┬────────────────────────────────────────┘
                 │ served at
                 ▼
              http://localhost:8000/docs   (interactive API docs)
              http://localhost:5173        (ui)
```

## Project structure

```
models/                 Bundled models (example model for quickstart)
  example/              Toy model — works out of the box with no credentials
    extractors/         Extractor YAML configs (extractor_*.yaml)
    ontology/           Entity definitions (*.yaml)
    mappers/            Mapping rules: raw_* → sem_* (*.yaml)
    features/           Computed column definitions (entity.feature.yaml)
    queries/            Query definitions — aggregations and signals (*.yaml)
    input_data/         Static input files loaded by the file tool
    data/               SQLite database (data.db)
core/                   Shared types, config, validation, SQLite backend
engine/                 Extractor orchestrator, schema management, query runner
semantic/               Ontology parser, mapper engine, expression compiler
tools/                  Tool connectors (Jira, GitHub, file, codeowners)
schemas/                JSON Schemas for all YAML config types
tests/                  Test suite (1200+ tests)
docs/adr/               Architecture Decision Records
```

> **Models are decoupled from the application.** The `models/` directory in this repo contains only the built-in `example` model. Your real models live in a separate directory (or private repo) and are pointed to via the `VZ_MODELS_DIR` environment variable.

### Multi-model support

The platform supports multiple isolated models. Each model lives under `<models-dir>/<name>/` with its own extractors, ontology, input data, and database.

**Model directory** — defaults to `models/` inside this repo. Override with the `VZ_MODELS_DIR` env var to point at any directory:

```bash
export VZ_MODELS_DIR=/path/to/your/private/models
```

**Active model resolution** (checked in priority order):
1. `VZ_MODEL` environment variable: `export VZ_MODEL=mymodel`
2. `.vz_context` file in project root — plain model name or `KEY=VALUE` pairs

### Layers

**Tools** (`tools/`) — Lightweight Python classes that connect to external systems and yield raw JSON records. Each tool implements `run(command, params) -> Iterator[dict]` and `list_commands() -> list[str]`. Tools that support wildcard expansion also implement `resolve_wildcard(param_name, param_value) -> list`.

**Engine** (`engine/`) — Reads extractor YAML files, calls tools, maps columns via `json_path`, and writes to SQLite. Handles wildcard expansion (e.g. fetching sprints for all boards), row explosion (e.g. flattening changelog histories), and schema evolution (new columns are added automatically).

**Core** (`core/`) — Shared types (`WriteMode`, `TaskConfig`, `ColumnDef`, `RowSource`), config loading, schema validation, and the SQLite backend.

**Semantic** (`semantic/`) — Parses ontology YAML into typed entity objects, executes mapper YAML to populate `sem_*` tables from `raw_*` tables, runs the expression compiler to materialize feature columns, and parses query YAML definitions.

**Query layer** (`engine/query_runner.py`, `models/<model>/queries/`) — Declarative GROUP BY queries over the semantic layer. Supports relation traversal in slice dimensions, window functions, thresholds for signal evaluation, and ratio metrics.

### Write modes

Each extractor task declares a `write_mode` that controls how records are written to the table:

| Mode | Behaviour | `inserted_at` column | Use case |
|------|-----------|---------------------|----------|
| `UPSERT` | `INSERT OR REPLACE` on primary key. Re-running updates existing rows. | No | Reference data (boards, teams, issues) |
| `APPEND` | `INSERT` — re-running adds new rows. | Yes (auto) | Event/log data (changelog entries, deploys) |
| `REPLACE` | `DELETE` all existing rows, then insert fresh. | No | Full-refresh dimensions without a stable key |

### Row source modes

By default each record from a tool produces one database row (`SINGLE` mode). For array-based data, `EXPLODE` mode fans out an array field into one row per element:

```yaml
row_source:
  mode: EXPLODE
  json_path: $.changelog.histories   # array to iterate
  inherit:
    issue_key: $.key                 # parent field → column
columns:
  - name: history_id
    json_path: $.id                  # resolved against each element
```

- `json_path` — dot-path to the array within each source record
- `inherit` — maps column names to json_paths resolved against the **parent** record
- Column json_paths are resolved against each **element** of the exploded array

## Setup

```bash
poetry install --with dev
```

### Quick start with the example model

The bundled `example` model works immediately with no external credentials:

```bash
./start_api_server.sh          # API at http://localhost:8000
cd ui && npm install && npm run dev          # UI at http://localhost:5173
```

The example model is pre-loaded with a product/sprint ontology and static JSON fixtures.

### Pointing at your own models

Create a `.env` file in the project root:

```
VZ_MODELS_DIR=/path/to/your/private/models
```

`start_api_server.sh` sources this file automatically. For tests, set the variable in your shell — the test suite clears it via `conftest.py` so it never bleeds into unit tests.

### Start the API server

```bash
./start_api_server.sh                         # default: host=0.0.0.0, port=8000, reload on
./start_api_server.sh --port 8001 --no-reload # production-style
```

### Configuration

- `.env` — `VZ_MODELS_DIR` to point at your models directory (gitignored)
- `models/<model>/config.yaml` — Per-model tool config and credentials

## Usage

### Start the API server

```bash
./start_api_server.sh
```

The API runs at `http://localhost:8000`. Interactive docs are at `/docs`.

### Set the active model

```bash
echo "example" > .vz_context
```

Or set `VZ_MODEL` env var. The `.vz_context` file can also hold `KEY=VALUE` pairs:

```
VZ_MODEL=example
VZ_API_URL=http://localhost:8000
```

### API

All operations are available through the REST API. With the server running, visit:

- **`http://localhost:8000/docs`** — interactive Swagger UI (try any endpoint directly)
- **`http://localhost:8000/redoc`** — ReDoc reference

All routes are versioned under `/api/v1/model/{model}/`. Extractor and mapper execution are asynchronous — they return a job ID immediately; poll `/api/v1/model/{model}/job/{job_id}` for status.

### Inspect the database directly

```bash
sqlite3 models/example/data/data.db ".tables"
sqlite3 models/example/data/data.db "SELECT count(*) FROM raw_products"
sqlite3 models/example/data/data.db "SELECT * FROM sem_product LIMIT 5"
```

Or substitute the path to any model in your `VZ_MODELS_DIR`.

## Writing extractor tasks

Add a new entry to an existing `extractor_*.yaml` in `models/<model-name>/extractors/` or create a new file:

```yaml
tasks:
  - name: jira_tickets
    tool: jira
    command: issues
    params:
      project: "ENG"
    output:
      table: jira_tickets
      write_mode: UPSERT
      primary_keys: [key]
      columns:
        - name: key
          json_path: $.key
        - name: summary
          json_path: $.fields.summary
        - name: status
          json_path: $.fields.status.name
```

### Multiple outputs per task

A task can write to multiple tables from a single API call using `outputs` (plural) instead of `output`:

```yaml
  - name: jira_issues
    tool: jira
    command: issues
    params:
      project: "*"
      expand: changelog
    context:
      project: project_key
    outputs:
      - table: jira_issues
        write_mode: UPSERT
        primary_keys: [key]
        columns:
          - name: key
            json_path: $.key
          - name: summary
            json_path: $.fields.summary
      - table: jira_issue_changelog
        write_mode: APPEND
        row_source:
          mode: EXPLODE
          json_path: $.changelog.histories
          inherit:
            issue_key: $.key
        columns:
          - name: history_id
            json_path: $.id
          - name: author
            json_path: $.author.displayName
            type: STRING
```

### YAML fields

| Field | Description |
|-------|-------------|
| `name` | Unique task identifier (snake_case) |
| `tool` | Tool to use: `jira`, `git`, `git_codeowners`, or `file` |
| `command` | Tool command (e.g. `boards`, `issues`, `repos`) |
| `params` | Key-value parameters passed to the tool. Use `"*"` for wildcard expansion |
| `context` | Maps wildcard param names to output column names |
| `since` | ISO date floor for incremental extraction (e.g. `"2025-10-01"`) |
| `incremental` | When `true`, uses last successful run timestamp as a date cutoff |
| `output` | Single output config (mutually exclusive with `outputs`) |
| `outputs` | List of output configs (mutually exclusive with `output`) |
| `output.table` | Table name (auto-prefixed with `raw_`) |
| `output.write_mode` | `UPSERT`, `APPEND`, or `REPLACE` |
| `output.primary_keys` | Columns forming the primary key (required for `UPSERT`) |
| `output.row_source.mode` | `SINGLE` (default) or `EXPLODE` |
| `output.row_source.json_path` | Array to iterate when exploding |
| `output.row_source.inherit` | Map of column names to parent-record json_paths |
| `output.columns` | List of column mappings |
| `columns[].name` | Output column name in the database (snake_case) |
| `columns[].json_path` | Dot-path into the record (`$.field.nested.value`) |
| `columns[].type` | Optional type override: `STRING`, `INTEGER`, `FLOAT`, `JSON` |

## Writing ontology

Ontology files define the semantic entity model. Each file declares one entity with its primary key, attributes, and relations to other entities.

```yaml
entity: PullRequest
description: "A GitHub pull request"

identity:
  pull_request_key:
    type: STRING
    semantic: PRIMARY_KEY

attributes:
  title:
    type: STRING
    semantic: IDENTIFIER
  merged_at:
    type: STRING
    semantic: TIMESTAMP
  story_points:
    type: FLOAT
    semantic: MEASURE
  team_key:
    type: STRING
    semantic: RELATION
    references: Team

relations:
  belongs_to:
    target: Team
    via: team_key
    cardinality: MANY_TO_ONE
  commits:
    target: Commit
    via: [pull_request_key]
    cardinality: ONE_TO_MANY
```

### Semantic hints

| Hint | Description |
|------|-------------|
| `PRIMARY_KEY` | The entity's unique identifier column |
| `IDENTIFIER` | Human-readable name/label for the entity |
| `TIMESTAMP` | A datetime column eligible for time-grain bucketing |
| `MEASURE` | A numeric column that can be aggregated |
| `RELATION` | A foreign key column pointing to another entity |
| `ATTRIBUTE` | A plain descriptive attribute |
| `ENTITY` | Holds an entity type name (used for dynamic/polymorphic relations) |

### Relation cardinalities

| Cardinality | Description | `via` |
|-------------|-------------|-------|
| `MANY_TO_ONE` | Many rows of this entity → one row of target | `via: fk_column` or `via: fk_col > target_pk` |
| `ONE_TO_MANY` | One row of this entity → many rows of target | `via: [target_fk_column]` (column name on the target side) |

### Dynamic (polymorphic) relations

When the target entity type varies per row, use `target: dynamic(field_name)` where `field_name` is a column with `semantic: ENTITY`:

```yaml
entity: Identity
attributes:
  subject_type:
    type: STRING
    semantic: ENTITY      # holds "Person", "Machine", etc.
  subject_key:
    type: STRING
    semantic: ATTRIBUTE

relations:
  subject:
    target: dynamic(subject_type)
    via: subject_key
    cardinality: MANY_TO_ONE
```

Traversing `identity.subject.name` in a query or feature generates a conditional LEFT JOIN per possible target entity and wraps the results in `COALESCE`. Fields absent on a particular entity produce `NULL` for that entity's rows rather than an error.

## Writing mappers

Mapper files live in `models/<model>/mappers/` and define how to populate `sem_*` tables from `raw_*` sources. The mapper engine executes all joins in Python-generated SQL and applies transform expressions column by column.

```yaml
mapper: issue
description: "Map Jira issues to Issue entity"

depends_on:
  - team
  - identity

grain: iss

sources:
  - alias: iss
    table: raw_jira_issues
    columns: [key, project_key, summary, status, assignee_account_id]
    filter:
      project_key in ['ENG', 'PLATFORM']
  - alias: id_assignee
    table: sem_identity
    columns: [identity_key, value, system, type]
    filter:
      and:
        - system == 'jira'
        - type == 'id'
    deduplicate: [value]

joins:
  - from: iss
    to: id_assignee
    type: left
    "on":
      - left: iss.assignee_account_id
        right: id_assignee.value

targets:
  - entity: Issue
    columns:
      - name: issue_key
        expression: iss.key
      - name: assignee_identity_key
        expression: id_assignee.identity_key
```

### Filter operators (source filtering)

Used in `filter:` fields on mapper sources.

#### Comparison operators

| Operator | Example |
|----------|---------|
| `==` | `issue_type == "Product Version"` |
| `!=` | `status != "Closed"` |
| `>`, `<`, `>=`, `<=` | `created > "2026-01-01"` |

#### List membership

| Operator | Example |
|----------|---------|
| `in` | `issue_type in ["Product", "Product Version"]` |
| `not_in` | `status not_in ["Done", "Cancelled"]` |

#### Method-style operators

Called as `column.method(args)`:

| Method | Description | Example |
|--------|-------------|---------|
| `is_null()` | Field is NULL | `priority.is_null()` |
| `not_null()` | Field is not NULL | `priority.not_null()` |
| `startswith(s)` | String prefix match | `summary.startswith("AD-")` |
| `endswith(s)` | String suffix match | `summary.endswith("Review")` |
| `contains(v)` | JSON array contains value | `items.contains("status")` |
| `containsAny(list)` | JSON array contains any value | `items.containsAny(["status", "assignee"])` |
| `json_any(key, val)` | Any array element has field matching value | `items.json_any("fieldId", "status")` |

#### Logical composition (YAML-based)

```yaml
filter:
  and:
    - issue_type == "Product Version"
    - status != "Closed"
    - or:
        - priority >= 2
        - assignee.not_null()
```

### Transform functions (column expressions)

Used in `expression:` fields on mapper target columns:

| Function | Description | Example |
|----------|-------------|---------|
| `TRIM(x)` | Strip whitespace | `TRIM(src.name)` |
| `UPPER_SNAKE(x)` | Convert to UPPER_SNAKE_CASE | `UPPER_SNAKE(src.status)` |
| `TITLE(x)` | Title-case a string | `TITLE(src.name)` |
| `LOWER(x)` | Lowercase a string | `LOWER(src.name)` |
| `CAST(x, type)` | Type cast: `integer`, `float`, `boolean`, `date`, `datetime`, `string` | `CAST(src.id, integer)` |
| `DEFAULT(x, fallback)` | Return fallback if x is null | `DEFAULT(src.name, "unknown")` |
| `COALESCE(a, b, ...)` | Return first non-null argument | `COALESCE(team.team_key, project_team.team_key)` |
| `IF_NOT_NULL(check, value)` | Return value if check is not null, else null | `IF_NOT_NULL(fp.person_key, CONST("Person"))` |
| `ENUM(x, mapping)` | Reverse-lookup in an enum mapping | `ENUM(src.status, lifecycle_state)` |
| `CONCAT(a, b, ...)` | Concatenate strings (null if any arg is null) | `CONCAT(pr.repo_name, "#", pr.number)` |
| `CONST(s)` | Return a literal string constant | `CONST("OPEN")` |
| `JSON_EXTRACT(x, field)` | Extract a field from a JSON object | `JSON_EXTRACT(row.data, "name")` |
| `JSON_FIND(x, mk, mv, ek)` | Find element in JSON array by field match | `JSON_FIND(sc.items, "fieldId", "status", "toString")` |
| `REGEX_EXTRACT(x, pattern)` | Return first regex match or None | `REGEX_EXTRACT(iss.summary, "\\d+\\.\\d+")` |

Single-argument functions (`TRIM`, `UPPER_SNAKE`, `TITLE`, `LOWER`) propagate `None`. `CONCAT` returns null if any argument is null.

### Join operators

Used in `join.on[].operator`:

| Operator | Description |
|----------|-------------|
| `eq` | Equality (default) — `left = right` |
| `json_array_contains` | Left value exists in a JSON array on the right. Supports optional `json_path` to extract a nested array first. |
| `scalar_or_array_contains` | Left value matches either a scalar string column or a JSON array column on the right. |

## Writing features

Feature files live in `models/<model>/features/` and are named `entity.feature_name.yaml`. They define computed columns that are materialized alongside entity rows.

```yaml
feature_id: pull_request.fc_to_pr_open_hours
name: First Commit to PR Open (hours)
description: Hours from first commit to PR opened.
entity_type: PullRequest
entity_key: pull_request_key
data_type: FLOAT
materialization_mode: materialized
expression: 'datetime_diff(min(Commit.committed_at), created_at, unit="hours")'
```

Features are used:
- In query `measures:` via `expr:` — reference features by name as if they were regular columns
- In other feature expressions — bare FieldRefs matching same-entity features are inlined automatically
- In query `group_by:` slices — traverse via relation dot notation

The `expression:` field uses the **Semantic Expression Language** described below.

## Semantic Expression Language

The expression language is a typed mini-language for writing computed values over entity data. It is used in:

- **Feature YAML** — the `expression:` field
- **Query YAML measures** — the `expr:` shorthand (e.g. `expr: avg(build_duration)`)

Expressions are compiled to SQL by the expression engine and can reference entity columns, traverse relations, aggregate over joined rows, apply window functions, and use built-in functions.

### Literals

```
42          # integer
3.14        # float
"hello"     # string (double quotes)
```

### Field references

```
build_duration                  # bare column on the root entity
Product.display_name            # MANY_TO_ONE traversal via relation name
Commit.committed_at             # ONE_TO_MANY traversal (used inside aggregates)
is_authored_by.subject.name     # multi-hop: relation → entity → field
```

Bare names are first checked against same-entity feature definitions (and inlined if found), then against root entity columns.

### Arithmetic operators

| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division (denominator wrapped in `NULLIF(..., 0)`) |

### Comparison and boolean operators

```
story_points > 0
merged_at != null
priority <= 3
a AND b
a OR b
```

> **Note:** The `=` operator is available inside `expression:` / `expr:` strings compiled by the expression engine. Query `where:` clauses use the filter parser (same as mapper source filters) where equality is `==`.

### Aggregate functions

These consume all matching joined rows and produce one value per root entity row:

| Function | Description |
|----------|-------------|
| `count(x)` | Count non-null values |
| `sum(x)` | Sum |
| `avg(x)` | Average |
| `min(x)` | Minimum |
| `max(x)` | Maximum |

When the argument traverses a ONE_TO_MANY relation, the compiler emits a GROUP BY subquery automatically.

```
# Count commits on a PR
count(Commit.commit_sha)

# First commit timestamp on a PR
min(Commit.committed_at)

# Weighted ratio: sum over ONE_TO_MANY rows
sum(is_lt_28d) / count(product_version_key)
```

### CASE WHEN

```
case when Deployment.environment = 'prd' and Deployment.status = 'success'
     then Deployment.created_at
     end
```

The `else` clause is optional; omitting it returns `NULL` for unmatched rows.

### Built-in functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `datetime_diff` | `datetime_diff(a, b, unit="hours")` | Difference between two timestamps. `unit`: `"hours"`, `"days"`. Division of the difference is dialect-aware. |
| `format_date` | `format_date(x, fmt)` | Format a timestamp string. |
| `json_has_key` | `json_has_key(x, key)` | Returns 1 if the JSON object has the given key, 0 otherwise. |
| `argmax` | `argmax(order_field, value_field)` | Returns the `value_field` from the row with the highest `order_field`. |
| `lag` | `lag(x)` | Previous row's value (used with `.over()`). |

### Window functions

Window expressions use the `.over()` postfix syntax:

```
lag(released_at).over(product_key, released_at)
```

The last argument to `.over()` becomes the `ORDER BY`; all preceding arguments become `PARTITION BY`.

```
# Days since the previous release in the same product
datetime_diff(
  lag(released_at).over(product_key, released_at),
  released_at,
  unit="days"
)
```

Window features compile to a two-phase SQL query:
1. An inner subquery aggregates any aggregate-expression columns and groups by entity key + partition/order columns
2. An outer SELECT applies the window function over the inner result set

### Full examples

**Feature: first commit to PR open hours (aggregate over ONE_TO_MANY)**
```
datetime_diff(min(Commit.committed_at), created_at, unit="hours")
```

**Feature: change lead time using CASE WHEN inside aggregate**
```
datetime_diff(
  min(Commit.committed_at),
  min(case when Deployment.environment = 'prd' and Deployment.status = 'success'
           then Deployment.created_at
           end),
  unit="hours"
)
```

**Feature: days since previous version (window function)**
```
datetime_diff(
  lag(released_at).over(product_key, released_at),
  released_at,
  unit="days"
)
```

**Query measure: ratio**
```yaml
measures:
  pct_versions_lt_28d:
    expr: sum(is_lt_28d) / count(product_version_key)
```

**Query measure: traversal in aggregate**
```yaml
measures:
  commit_count:
    expr: count(commit_sha)
group_by:
  - is_authored_by.subject_type
  - is_authored_by.subject.name
```

## Writing queries

Query files live in `models/<model>/queries/`. Each query defines a GROUP BY aggregation over a root entity, with optional filtering, ordering, window functions, and threshold-based signal evaluation.

```yaml
name: pv_build_duration
root: ProductVersion

group_by:
  - format_time(released_at, "YYYY-MM")  # ISO year + month
  - Product.display_name                 # MANY_TO_ONE traversal

measures:
  avg_build_duration:
    expr: avg(build_duration)
  pct_versions_lt_28d:
    expr: sum(is_lt_28d) / count(product_version_key)
  rolling_avg_days_since_prev:
    expr: avg(days_since_prev_version)
    window:
      method: weighted
      unit: month
      frame: 3

where:
  - build_duration is not NULL

order_by:
  - format_time(released_at, "YYYY-MM")
```

### Query YAML fields

| Field | Description |
|-------|-------------|
| `name` | Unique identifier — must match filename stem |
| `root` | Root entity (PascalCase) |
| `group_by` | List of GROUP BY dimensions. Bare name = entity column; `Entity.field` = relation traversal; `format_time(field, pattern)` = ISO timestamp bucketing |
| `measures` | Map of measure name → measure definition |
| `measures.<name>.expr` | Expression string using the Semantic Expression Language |
| `measures.<name>.window` | Optional window definition (see below) |
| `measures.<name>.thresholds` | Optional signal thresholds — see Thresholds |
| `where` | List of filter expressions applied before aggregation |
| `order_by` | List of fields or `format_time(field, pattern)` expressions to sort by |

### format_time — ISO timestamp bucketing

`format_time(field, pattern)` formats a TIMESTAMP column using ISO 8601 tokens. `YYYY` and `WW` always use the ISO year and ISO week number, so dates near year boundaries are bucketed correctly.

| Token | Meaning | SQLite | PostgreSQL |
|-------|---------|--------|-----------|
| `YYYY` | ISO year | `%G` | `IYYY` |
| `WW` | ISO week number (01–53) | `%V` | `IW` |
| `MM` | Month (01–12) | `%m` | `MM` |
| `DD` | Day (01–31) | `%d` | `DD` |
| `HH` | Hour (00–23) | `%H` | `HH24` |

Common patterns:

| Pattern | Example output | Use for |
|---------|---------------|---------|
| `"YYYY-WW"` | `2025-03` | weekly bucketing |
| `"YYYY-MM"` | `2025-03` | monthly bucketing |
| `"YYYY-MM-DD"` | `2025-03-11` | daily bucketing |

### Window functions in queries

Adding a `window:` block to a measure wraps the query in a CTE and applies the SQL window function in the outer SELECT:

```yaml
rolling_avg_build_duration:
  expr: avg(build_duration)
  window:
    method: weighted   # weighted | simple | cumulative | lag | lead
    unit: month        # matches the time-grain dimension in group_by
    frame: 3           # rolling window size (current + N-1 preceding)
```

| Method | Description |
|--------|-------------|
| `weighted` | Population-weighted rolling average — avoids mean-of-means distortion |
| `simple` | Applies the rollup function as a SQL window aggregate |
| `cumulative` | Accumulates from start of partition to current row |
| `lag` | Value from `offset` rows behind the current row |
| `lead` | Value from `offset` rows ahead of the current row |

### Thresholds (signal evaluation)

Measures can carry threshold rules that classify each row as `green`, `amber`, or `red`:

```yaml
measures:
  avg_build_duration:
    expr: avg(build_duration)
    thresholds:
      - level: red
        operator: ">"
        value: 56
      - level: amber
        operator: ">"
        value: 42
```

Thresholds are evaluated after SQL execution; the classified result is added to the query output. Queries with thresholds are effectively "signals" — they answer "is this metric within acceptable bounds?"

## Identity model

The Identity entity is a polymorphic link between a system credential (a Jira account ID, GitHub handle, email, etc.) and the underlying subject — either a `Person` or a `Machine`.

```
Identity
  system:      'jira' | 'github' | 'email'
  type:        'id' | 'handle' | 'email'
  value:       the credential value
  subject_type: 'Person' | 'Machine'
  subject_key:  foreign key into sem_person or sem_machine

  relations:
    subject → dynamic(subject_type)   # polymorphic: resolves to Person or Machine
    person  → Person (direct shortcut)
```

Traversing `identity.subject.name` in a query generates:
- A conditional LEFT JOIN to `sem_person` where `subject_type = 'Person'`
- A conditional LEFT JOIN to `sem_machine` where `subject_type = 'Machine'`
- `COALESCE(sem_person.name, sem_machine.name)` as the result

This allows queries like `commits_per_identity_per_month` to group by both `subject_type` and `subject.name` in a single result set, with unmatched identities appearing as blank rows.

## Testing tools independently

You can import and call any tool directly from a Python shell to inspect its raw output.

### Jira

```python
from tools.jira.tool import JiraTool

jira = JiraTool(config={"url": "...", "email": "...", "api_token": "..."})

# List available commands
jira.list_commands()  # ['boards', 'sprints', 'issues', 'projects', 'users', ...]

# Fetch all boards
for board in jira.run("boards"):
    print(board["id"], board["name"])

# Fetch sprints for a specific board
for sprint in jira.run("sprints", {"board_id": 59}):
    print(sprint["name"], sprint["state"])

# Fetch issues with changelog expansion
for issue in jira.run("issues", {"project": "ENG", "expand": "changelog"}):
    print(issue["key"], len(issue["changelog"]["histories"]), "history entries")

# List all board IDs (used by wildcard expansion)
board_ids = jira.resolve_wildcard("board_id", "*")
```

### GitHub

```python
from tools.git.tool import GitHubTool

gh = GitHubTool(config={"org": "my-org", "token": "..."})

# List available commands
gh.list_commands()  # ['repos', 'teams', 'team_members', 'orgs', ...]

# Fetch repos for the default org
for repo in gh.run("repos"):
    print(repo["name"], repo["language"])

# List members of a team
for member in gh.run("team_members", {"team_slug": "backend-engineers"}):
    print(member["login"])
```

## Running tests

```bash
poetry run pytest tests/ -v
```

Tests use the bundled `example` model and do not require `VZ_MODELS_DIR` or any external credentials. The test suite automatically clears `VZ_MODELS_DIR` via `tests/conftest.py` so `.env` values never bleed in.

## License

Copyright 2024-2026 Oliver Fenton. Licensed under the [Apache License, Version 2.0](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
