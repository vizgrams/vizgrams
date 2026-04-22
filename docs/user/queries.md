# Writing Queries

Queries define the aggregations and metrics you want to produce from your semantic layer. They are declarative GROUP BY definitions — you specify which entity to query, which dimensions to group by, and which aggregate measures to compute.

→ [[index|← Back to home]]

---

## Query YAML

Each query is a separate file in `models/<model>/queries/`, named `<query_name>.yaml`. The filename stem must match the `name` field.

```yaml
# models/my_model/queries/pr_throughput.yaml

name: pr_throughput
root: PullRequest

attributes:
  - month_key: format_time(merged_at, "YYYY-MM")
    order: 1, asc
  - team: Repository.Team.display_name

measures:
  - pr_count:
      expr: count(pull_request_key)
      format:
        type: number
        pattern: "0"
  - avg_hours_to_merge:
      expr: avg(hours_to_merge)
      format:
        type: duration
        unit: hours
        pattern: "0.1"

where:
  - merged_at.not_null()
  - merged_at >= now() - 12w
```

---

## root

The `root` is the entity you're querying. It must be a PascalCase entity name from your ontology.

```yaml
root: PullRequest
```

All attributes and measures resolve relative to this entity unless they traverse a relation.

---

## attributes

Attributes are the GROUP BY dimensions of the query. Each attribute is a `{alias: expression}` pair:

```yaml
attributes:
  - month_key: format_time(merged_at, "YYYY-MM")
  - team: Repository.Team.display_name
  - author: is_authored_by.subject.name
```

### Time bucketing

Use `format_time(field, pattern)` to bucket timestamps into periods. This is the standard way to create time-series dimensions:

| Pattern | Output | Use for |
|---------|--------|---------|
| `"YYYY-MM"` | `"2026-03"` | Monthly trends |
| `"YYYY-WW"` | `"2026-12"` | Weekly trends (ISO week) |
| `"YYYY-wWW"` | `"2026-w12"` | Weekly trends (prefixed format) |

### Ordering

Add `order: N, asc` or `order: N, desc` to sort by a dimension:

```yaml
attributes:
  - month_key: format_time(merged_at, "YYYY-MM")
    order: 1, asc
  - team: Repository.Team.display_name
    order: 2, asc
```

### Traversal in attributes

Traverse MANY_TO_ONE relations using dot notation:

```yaml
attributes:
  - team: Repository.Team.display_name     # PullRequest → Repository → Team → display_name
  - author: is_authored_by.subject.name    # relation name → dynamic entity → field
  - airline: Aircraft.Airline.name         # chained traversal
```

---

## measures

Measures are the aggregated values produced by the query. Each measure is a named block with an `expr` and optional `format`, `window`, and `thresholds`:

```yaml
measures:
  - pr_count:
      expr: count(pull_request_key)
      format:
        type: number
        pattern: "0"

  - avg_cycle_time:
      expr: avg(hours_to_merge)
      format:
        type: duration
        unit: hours
        pattern: "0.1"

  - pct_fast_prs:
      expr: sum(case when hours_to_merge < 24 then 1 else 0 end) / count(pull_request_key)
      format:
        type: percent
        pattern: "0.0%"
```

### Aggregate functions

| Function | Description |
|----------|-------------|
| `count(x)` | Count non-null values |
| `sum(x)` | Sum of values |
| `avg(x)` | Average |
| `min(x)` | Minimum |
| `max(x)` | Maximum |

### Referencing features

Features materialised on the root entity can be used directly by their bare column name:

```yaml
measures:
  - avg_build_duration:
      expr: avg(build_duration)       # "build_duration" is a feature on ProductVersion
```

### Conditional aggregation

Use `case when` inside aggregate functions to count or sum only rows matching a condition:

```yaml
measures:
  - completed_points:
      expr: sum(case when status == "Done" then story_points end)
  - pct_completed:
      expr: sum(case when status == "Done" then story_points end) / sum(story_points)
      format:
        type: percent
        pattern: "0.0%"
```

### Traversal in measures

Traverse ONE_TO_MANY relations inside aggregates:

```yaml
measures:
  - commit_count:
      expr: count(Commit.sha)                    # PullRequest → Commit (ONE_TO_MANY)
  - avg_files_changed:
      expr: avg(Commit.files_changed)
```

---

## format

Controls how a measure value is displayed.

### Number

```yaml
format:
  type: number
  pattern: "0.0"         # decimal places
```

### Percent

```yaml
format:
  type: percent
  pattern: "0.0%"        # value is displayed as e.g. "73.5%"
```

### Duration

```yaml
format:
  type: duration
  unit: hours            # hours | days | weeks
  pattern: "0.1"
```

---

## window

Add a `window` to compute a rolling or cumulative version of a measure. Windows require at least one time-grain attribute (a `format_time(...)` attribute) to anchor the window.

```yaml
measures:
  - rolling_avg_cycle_time:
      expr: avg(hours_to_merge)
      format:
        type: duration
        unit: hours
        pattern: "0.1"
      window:
        method: weighted     # weighted | simple | cumulative | lag | lead
        unit: month          # must match a format_time grain in attributes
        frame: 3             # look back 3 periods
```

### Window methods

| Method | Description |
|--------|-------------|
| `weighted` | Population-weighted rolling average over the last `frame` periods |
| `simple` | Simple rolling window aggregate over the last `frame` periods |
| `cumulative` | Running total from the start of the data to the current period |
| `lag` | Value from `offset` periods behind the current row |
| `lead` | Value from `offset` periods ahead of the current row |

For `weighted` and `simple`, provide `frame` (number of periods) and `unit` (the time grain, matching a `format_time` attribute). For `lag`/`lead`, provide `offset` instead of `frame`.

---

## thresholds

Add traffic-light status to a measure. Each threshold is an `{op, value, status}` rule evaluated top-to-bottom; the first match wins.

```yaml
measures:
  - avg_cycle_time_signal:
      expr: avg(hours_to_merge)
      format:
        type: duration
        unit: hours
        pattern: "0.1"
      thresholds:
        - op: ">"
          value: 72
          status: red
        - op: ">"
          value: 24
          status: amber
        - op: "<="
          value: 24
          status: green
```

Supported operators: `>`, `>=`, `<`, `<=`, `==`, `!=`.

---

## where

Filter rows before aggregation. Uses the same expression language as mapper source filters:

```yaml
where:
  - merged_at.not_null()             # only merged PRs
  - state != 'draft'
  - merged_at >= now() - 12w         # last 12 weeks
  - Repository.archived == false     # traverse relation in filter
```

Multiple conditions are combined with AND.

---

## Multi-hop traversal

Both attributes and measure expressions support chained relation traversal:

```yaml
attributes:
  - team: Repository.Team.display_name          # PullRequest → Repository → Team
  - author: is_authored_by.subject.name         # dynamic relation
  - airline: Aircraft.Airline.name              # chained MANY_TO_ONE

measures:
  - avg_fleet_age:
      expr: avg(Aircraft.age_days)              # ONE_TO_MANY traversal inside aggregate
```

The engine resolves each hop through the ontology and generates the appropriate JOIN.

---

## Signal queries

A signal query is a standard query with `thresholds` on measures — it adds a status field to the result. Use signals for dashboards and alerting.

```yaml
name: team_delivery_signal
root: Sprint

attributes:
  - team: Team.display_name
    order: 1, asc

measures:
  - velocity_signal:
      expr: avg(completed_story_points)
      format:
        type: number
        pattern: "0"
      thresholds:
        - op: "<"
          value: 20
          status: red
        - op: "<"
          value: 40
          status: amber
        - op: ">="
          value: 40
          status: green

where:
  - sprint_end_date >= now() - 12w
```

---

## CLI reference

```bash
# List all queries
./run.sh query list --model my_model

# Inspect a query definition
./run.sh query get pr_throughput --model my_model

# Execute a query
./run.sh query execute pr_throughput --model my_model

# Execute with a row limit
./run.sh query execute pr_throughput --limit 100 --model my_model

# Execute and output CSV
./run.sh query execute pr_throughput --format csv --model my_model

# Upload / update a query YAML
./run.sh query upsert pr_throughput queries/pr_throughput.yaml --model my_model

# Validate query YAML without running
./run.sh query validate pr_throughput --model my_model
```

---

## Tips

**Validate before running.** Use `query validate` to catch expression errors and missing entity references before executing.

**Reference features by bare name.** If a feature is materialised on the root entity, you don't need to qualify it — just use `avg(build_duration)` not `avg(ProductVersion.build_duration)`.

**Use `where` to scope the date range.** Queries without a date filter will process all rows. Always add a `merged_at >= now() - Nw` or similar to avoid slow queries on large datasets.

**Use `format_time` as your primary time dimension.** Having a `month_key` or `week_key` attribute as the first dimension makes the output naturally time-ordered and enables window measures.

**Division in measures is null-safe.** The `/` operator wraps the denominator in `NULLIF(..., 0)` so you won't get divide-by-zero errors — you'll get null.

---

## Next steps

For the full expression syntax used in measure `expr:` and `where:` fields:

→ [[expression|Expression Language Reference]]
