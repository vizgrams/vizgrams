# Expression Language Reference

A single expression language is used in `expr:` fields across all three YAML definition types — **Mappers**, **Queries**, and **Features**. The same syntax and keywords apply everywhere; only the execution context changes (some functions are only available in certain contexts).

→ [[index|← Back to home]]

---

## Quick reference

| Category | Syntax |
|----------|--------|
| Field reference | `alias.column`, `Entity.field`, `Entity.Relation.field` |
| String literal | `"hello"` |
| Number literal | `42`, `3.14` |
| Boolean operators | `and`, `or` |
| Comparison | `==`, `!=`, `<`, `<=`, `>`, `>=` |
| Null tests | `expr.is_null()`, `expr.not_null()` |
| Membership | `expr in ["a", "b"]`, `expr not_in ["a", "b"]` |
| Arithmetic | `+`, `-`, `*`, `/` |
| Negation | `-expr` |
| Conditional | `case when cond then value [else fallback] end` |
| Aggregation | `count(x)`, `sum(x)`, `avg(x)`, `min(x)`, `max(x)` |
| Window | `agg(x).over(partition_col, order_col)`, `lag(x).over(partition_col, order_col)` |
| String functions | `concat(...)`, `trim(x)`, `lower(x)`, `title(x)`, `upper_snake(x)` |
| Type casting | `cast(x, string)`, `cast(x, integer)`, `cast(x, float)`, `cast(x, boolean)` |
| Null handling | `coalesce(a, b, ...)`, `default(x, fallback)`, `if_not_null(check, value)` |
| Enum mapping | `enum(x, mapping_name)` |
| ID generation | `ulid(seed)` |
| Pattern matching | `regex_extract(x, pattern)` |
| JSON | `json_extract(x, key)`, `json_find(arr, match_key, match_val, extract_key)` |
| Time formatting | `format_time(ts, pattern)` |
| Time arithmetic | `now() - Nd`, `now() - Nw` |
| Time difference | `datetime_diff(a, b, unit="hours")` |
| Filter methods | `.startswith(s)`, `.endswith(s)`, `.contains(v)`, `.containsAny([...])`, `.json_any(key, val)` |

---

## Field references

### In Mappers

Reference source columns using `alias.column`:

```yaml
expr: pr.repo_name
expr: id_author.identity_key
```

A bare column name (without a prefix) searches all source aliases and returns the first non-null match.

### In Queries and Features

Traverse entity relations using dot notation:

```yaml
expr: Repository.Team.display_name    # MANY_TO_ONE chain
expr: Commit.committed_at             # ONE_TO_MANY (use inside an aggregate)
expr: is_authored_by.subject.name     # relation name → dynamic entity → field
expr: merged_at                       # bare column on root entity
```

Multi-hop traversal chains can be as long as needed. Each hop follows a relation defined in the ontology.

---

## Literals

```yaml
expr: '"github"'     # string — outer single quotes protect YAML, inner double quotes mark the string
expr: 42             # integer
expr: 3.14           # float
```

> **YAML note:** To write a string literal in an expression, you must wrap the whole YAML value in single quotes, then use double quotes inside: `expr: '"github"'`. Alternatively, use YAML double-quoted strings and escape inner quotes: `expr: "\"github\""`.

---

## Arithmetic

Standard operators with null propagation — any null operand returns null:

```yaml
expr: pr_open_hours + pr_review_hours
expr: score * 100
expr: numerator / denominator          # denominator=0 returns null (NULLIF protection)
expr: closed_at - created_at
```

---

## Comparisons and boolean logic

```yaml
expr: status == "merged"
expr: score >= 90
expr: created_at != merged_at
expr: status == "open" and assignee.not_null()
expr: env == "prod" or env == "uat"
```

Supported comparison operators: `==`, `!=`, `<`, `<=`, `>`, `>=`.

---

## Null tests

```yaml
# As method calls (preferred in filter/where positions)
filter: merged_at.not_null()
filter: subject_key.is_null()
where:
  - build_duration is not null

# Inside expressions
expr: case when email.not_null() then email else handle end
```

---

## Membership

Test whether a value appears in a static list:

```yaml
filter: project_key in ["ITAL", "AIAD", "AIAR"]
filter: status not_in ["closed", "cancelled"]
```

---

## Conditional (`case when`)

```yaml
expr: case when outcome == "completed" then story_points end
expr: case when score >= 90 then "green" else "red" end
```

Multi-branch logic by nesting:

```yaml
expr: >-
  case when score >= 90 then "green"
       else case when score >= 60 then "amber"
                 else "red" end end
```

Use `case when` inside aggregates for conditional aggregation:

```yaml
expr: sum(case when status == "Done" then story_points end)
```

---

## Aggregation functions

Available in **Queries** and **Features** (not Mappers):

| Function | Description |
|----------|-------------|
| `count(x)` | Count non-null values |
| `sum(x)` | Sum of values |
| `avg(x)` | Average |
| `min(x)` | Minimum |
| `max(x)` | Maximum |

```yaml
expr: count(pull_request_key)
expr: sum(story_points)
expr: avg(hours_to_merge)
expr: min(Commit.committed_at)
```

---

## Window functions

Available in **Features** only. Append `.over(partition_col, order_col)` to any aggregation or `lag()`:

```yaml
# lag() — previous row value
expr: lag(released_at).over(product_key, released_at)

# Running sum partitioned by product
expr: sum(story_points).over(product_key, sprint_end_date)

# datetime_diff with lag for time-between-versions
expr: datetime_diff(lag(released_at).over(product_key, released_at), released_at, unit="days")
```

`.over(partition_col_1, ..., partition_col_N, order_col)` — the **last argument is always the order column**; all preceding arguments are partition columns.

---

## String functions

Available in **Mappers** only:

| Function | Description |
|----------|-------------|
| `concat(a, b, ...)` | Concatenate strings; null in any argument returns null |
| `trim(x)` | Remove leading/trailing whitespace |
| `lower(x)` | Convert to lowercase |
| `title(x)` | Convert to title case |
| `upper_snake(x)` | Convert to UPPER_SNAKE_CASE |

```yaml
expr: concat(repo_name, "#", pr_number)
expr: lower(status)
expr: upper_snake(category_label)
```

---

## Type casting

Available in **Mappers** only:

```yaml
expr: cast(sprint_id, string)
expr: cast(score, integer)
expr: cast(rate, float)
expr: cast(flag, boolean)
expr: cast(date_str, date)
expr: cast(datetime_str, datetime)
```

Supported types: `string`, `integer`, `float`, `boolean`, `date`, `datetime`.

---

## Null-handling functions

Available in **Mappers** only:

| Function | Description |
|----------|-------------|
| `coalesce(a, b, ...)` | Return the first non-null argument |
| `default(x, fallback)` | Return `x` if non-null, else `fallback` |
| `if_not_null(check, value)` | Return `value` if `check` is non-null, else null |

```yaml
expr: coalesce(person_key, machine_key)
expr: default(team_key, "unknown")
expr: if_not_null(person_key, "Person")

# Combine for type tagging
expr: coalesce(if_not_null(person_key, "Person"), if_not_null(machine_key, "Machine"))
```

---

## Enum mapping

Available in **Mappers** only. Map a raw source value to a canonical string using a named mapping defined in the mapper's `enums:` block:

```yaml
enums:
  deployment_environment:
    PROD: [production, prod, prd]
    DEV:  [dev, development]

targets:
  - entity: Deployment
    columns:
      - name: environment
        expr: enum(d.environment, deployment_environment)
```

If the source value doesn't match any mapping entry, an error is raised.

---

## Deterministic ID generation

Available in **Mappers** only. Generate a stable, deterministic 26-character Crockford base32 ID from a seed string. The same seed always produces the same ID:

```yaml
expr: concat("identity_", ulid(concat("github", "|", "handle", "|", gu_hdl.login)))
```

Use this to generate surrogate keys that are stable across re-runs without needing a sequence or UUID.

---

## Pattern matching

Available in **Mappers** only. Return the first regex match in a string, or null if no match:

```yaml
expr: regex_extract(branch_name, "PROJ-[0-9]+")
expr: regex_extract(email, "@(.+)$")
```

---

## JSON functions

Available in **Mappers** only:

| Function | Description |
|----------|-------------|
| `json_extract(x, key)` | Extract a top-level key from a JSON string |
| `json_find(arr, match_key, match_val, extract_key)` | Find first element in a JSON array where `element[match_key] == match_val`, return `element[extract_key]` |

```yaml
expr: json_extract(metadata, "version")
expr: json_find(fields, "fieldId", "status", "toString")
```

---

## Time formatting

Available in **Queries** only. Format a timestamp as a string for bucketing:

| Pattern | Output | Use for |
|---------|--------|---------|
| `"YYYY-MM"` | `"2026-03"` | Monthly grouping |
| `"YYYY-WW"` | `"2026-12"` | Weekly grouping (ISO week number) |
| `"YYYY-wWW"` | `"2026-w12"` | Weekly (prefixed format) |

```yaml
attributes:
  - week_key: format_time(merged_at, "YYYY-WW")
  - month_key: format_time(created_at, "YYYY-MM")
```

---

## Time arithmetic

Available in **filter/where** positions only. Compute a timestamp relative to now:

```yaml
where:
  - created_at >= now() - 4w     # within the last 4 weeks
  - merged_at >= now() - 30d     # within the last 30 days
```

Supported suffixes: `d` (days), `w` (weeks).

---

## Time difference

Available in **Features** only. Calculate the signed difference between two timestamps:

```yaml
expr: datetime_diff(created_at, merged_at, unit="hours")
expr: datetime_diff(min(Commit.committed_at), created_at, unit="hours")
```

Result is `b - a` — positive when `b` is later than `a`. Supported units: `"hours"`, `"days"`.

---

## Filter methods

Used in `filter:` (mapper source filters) and `where:` (query filters) only — not in value expressions:

| Method | Description |
|--------|-------------|
| `.is_null()` | True if the field is null |
| `.not_null()` | True if the field is not null |
| `.startswith(s)` | True if the string starts with `s` |
| `.endswith(s)` | True if the string ends with `s` |
| `.contains(v)` | True if the JSON array field contains `v` |
| `.containsAny([a, b])` | True if the JSON array field contains any of the given values |
| `.json_any(key, val)` | True if any element of the JSON array has `element[key] == val` |

```yaml
filter: owner.startswith("@myorg/")
filter: merged_at.not_null()
filter: items.json_any("fieldId", "status")
filter: labels.containsAny(["bug", "incident"])
```

---

## Context availability

Not all functions are available in all expression positions. This table summarises what's available where:

| Feature | Mappers (`expr:`) | Queries (`expr:`) | Features (`expr:`) |
|---------|:-----------------:|:-----------------:|:-----------------:|
| Field refs (`alias.col`) | yes | entity traversal | entity traversal |
| Literals, arithmetic | yes | yes | yes |
| Comparisons (`==`, `!=`, …) | yes | yes | yes |
| `case when` | yes | yes | yes |
| Null tests (`.is_null()`, `.not_null()`) | filter only | where only | no |
| Membership (`in`, `not_in`) | filter only | where only | no |
| Aggregation (`count`, `sum`, …) | no | yes | yes |
| Window functions | no | no | yes |
| `concat`, `trim`, `lower`, `title`, `upper_snake` | yes | no | no |
| `cast` | yes | no | no |
| `coalesce`, `default`, `if_not_null` | yes | no | no |
| `enum` | yes | no | no |
| `ulid` | yes | no | no |
| `regex_extract` | yes | no | no |
| `json_extract`, `json_find` | yes | no | no |
| `format_time` | no | yes | no |
| `now() - Nd/Nw` | filter only | where only | no |
| `datetime_diff` | no | no | yes |
| `lag` | no | no | yes |

---

## Common patterns

### Composite key

```yaml
expr: concat(repo_name, "#", pr_number)
```

### Stable surrogate ID

```yaml
expr: concat("pr_", ulid(concat(repo_name, "|", pr_number)))
```

### Null-safe ratio (percent)

```yaml
expr: sum(case when status == "Done" then story_points end) / sum(story_points)
# "/" automatically protects against divide-by-zero → returns null instead
```

### Multi-value coalesce for polymorphic type tag

```yaml
expr: coalesce(if_not_null(person_key, "Person"), if_not_null(machine_key, "Machine"))
```

### Relative time filter

```yaml
where:
  - created_at >= now() - 8w
```

### Conditional bucket

```yaml
expr: >-
  case when hours_to_merge < 24 then "fast"
       when hours_to_merge < 72 then "medium"
       else "slow" end
```

### Event-based timestamp (time of state transition)

```yaml
expr: >-
  min(case when LifecycleEvent.to_state = "merged"
      then LifecycleEvent.occurred_at end)
```
