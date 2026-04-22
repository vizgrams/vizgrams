# Writing Features

Features are computed columns attached to an entity. They're defined in YAML, calculated using the [[expression|expression language]], and materialised into a `feature_value` store. Features can be referenced in queries as if they were native entity columns.

→ [[index|← Back to home]]

---

## What belongs in a feature?

Features are for computed values that require aggregation, cross-entity traversal, or window logic. Examples:

- Time between two events (e.g. first commit → PR opened)
- Count of related entities (e.g. number of commits on a PR)
- Classification from a threshold (e.g. "fast" if merged in < 2 hours)
- Running totals or previous-row comparisons (e.g. days since last release)

Simple transformations (string cleaning, null coalescing) belong in mappers, not features.

---

## Feature YAML

Each feature is its own file in `models/<model>/features/`, named `<entity>.<feature_name>.yaml`.

```yaml
# models/my_model/features/pull_request.hours_to_merge.yaml

feature_id: pull_request.hours_to_merge
name: Hours to Merge
description: "Hours from PR creation to merge."
entity_type: PullRequest
entity_key: pull_request_key
data_type: FLOAT
materialization_mode: materialized
expr: datetime_diff(created_at, merged_at, unit="hours")
```

### Required fields

| Field | Description |
|-------|-------------|
| `feature_id` | Unique identifier in format `entity_type_lower.feature_name` |
| `name` | Human-readable label |
| `entity_type` | PascalCase entity name matching the ontology |
| `entity_key` | Primary key column name of that entity |
| `data_type` | `STRING`, `INTEGER`, or `FLOAT` |
| `materialization_mode` | `materialized` or `dynamic` |
| `expr` | Expression string — see [[expression|Expression Language]] |

### Optional fields

| Field | Description |
|-------|-------------|
| `description` | Explanation of what this feature means |
| `dependencies` | List of `feature_id`s that must materialise first |

---

## Materialization modes

| Mode | Behaviour |
|------|-----------|
| `materialized` | Computed once and stored in `feature_value`; used by queries |
| `dynamic` | Computed at query time (not currently persisted) |

Use `materialized` for most features.

---

## Expression context for features

Feature expressions run in the context of the root entity. You can:

- Reference columns directly: `created_at`, `story_points`
- Traverse MANY_TO_ONE relations: `Repository.default_branch`
- Traverse ONE_TO_MANY relations (inside aggregates): `Commit.committed_at`
- Use aggregate functions: `count()`, `sum()`, `avg()`, `min()`, `max()`
- Use window functions: `lag().over()`, `sum().over()`
- Use `datetime_diff()` for time differences
- Use `case when` for conditional logic

See [[expression|Expression Language]] for the full syntax.

---

## Examples

### Simple time difference

```yaml
feature_id: pull_request.hours_to_merge
name: Hours to Merge
entity_type: PullRequest
entity_key: pull_request_key
data_type: FLOAT
materialization_mode: materialized
expr: datetime_diff(created_at, merged_at, unit="hours")
```

### Traversing a ONE_TO_MANY relation

Use the entity name in the expression to traverse to related rows; wrap in an aggregate:

```yaml
feature_id: pull_request.commit_count
name: Commit Count
entity_type: PullRequest
entity_key: pull_request_key
data_type: INTEGER
materialization_mode: materialized
expr: count(Commit.sha)
```

### Traversing a MANY_TO_ONE relation

Reference the related entity directly (no aggregate needed):

```yaml
feature_id: pull_request.repo_default_branch
name: Repository Default Branch
entity_type: PullRequest
entity_key: pull_request_key
data_type: STRING
materialization_mode: materialized
expr: Repository.default_branch
```

### First commit to PR open (multi-level traversal + aggregate)

```yaml
feature_id: pull_request.fc_to_pr_open_hours
name: First Commit to PR Open (hours)
entity_type: PullRequest
entity_key: pull_request_key
data_type: FLOAT
materialization_mode: materialized
expr: datetime_diff(min(Commit.committed_at), created_at, unit="hours")
```

### Conditional aggregation

```yaml
feature_id: sprint.completed_story_points
name: Completed Story Points
entity_type: Sprint
entity_key: sprint_id
data_type: INTEGER
materialization_mode: materialized
expr: sum(case when Issue.status == "Done" then Issue.story_points end)
```

### Multi-condition conditional

```yaml
feature_id: pull_request.size_bucket
name: PR Size Bucket
entity_type: PullRequest
entity_key: pull_request_key
data_type: STRING
materialization_mode: materialized
expr: >-
  case when lines_changed < 50 then "small"
       when lines_changed < 200 then "medium"
       else "large" end
```

### Event-based feature (time between states)

```yaml
feature_id: pull_request.review_start_hours
name: Hours to First Review
entity_type: PullRequest
entity_key: pull_request_key
data_type: FLOAT
materialization_mode: materialized
expr: >-
  datetime_diff(
    created_at,
    min(case when PullRequestLifecycleEvent.to_state = "in_review"
        then PullRequestLifecycleEvent.occurred_at end),
    unit="hours")
```

### Window feature — previous-row comparison

Window functions require `.over(partition_col, order_col)`. The last argument to `.over()` is the order column; all preceding are partition columns.

```yaml
feature_id: release.days_since_prev_release
name: Days Since Previous Release
entity_type: Release
entity_key: release_key
data_type: INTEGER
materialization_mode: materialized
expr: datetime_diff(lag(released_at).over(repo_name, released_at), released_at, unit="days")
```

### Feature dependencies

When one feature's expression depends on another being materialised first, declare `dependencies`:

```yaml
feature_id: sprint.velocity_delta
name: Velocity Delta vs Previous Sprint
entity_type: Sprint
entity_key: sprint_id
data_type: INTEGER
materialization_mode: materialized
dependencies:
  - sprint.completed_story_points
expr: >-
  completed_story_points -
  lag(completed_story_points).over(team_key, sprint_end_date)
```

Notice that once `sprint.completed_story_points` is a dependency, you can reference it as a bare column name (`completed_story_points`) in the expression — the engine knows to join it in.

---

## CLI reference

```bash
# List features for an entity
./run.sh entity features PullRequest --model my_model

# Get details for a specific feature
./run.sh entity feature PullRequest hours_to_merge --model my_model

# Materialise a single feature
./run.sh entity feature-reconcile PullRequest hours_to_merge --model my_model

# Materialise all features for an entity
./run.sh entity feature-reconcile PullRequest '*' --model my_model

# Materialise all features across all entities
./run.sh feature reconcile --model my_model

# Materialise all features for a specific entity type
./run.sh feature reconcile --entity PullRequest --model my_model
```

---

## Tips

**One file per feature.** Keep features granular — a single feature per file makes them easy to manage, re-run, and understand.

**Use `dependencies` for window features.** If a window feature references another feature by bare column name, it must list that feature in `dependencies` so the engine materialises it first.

**Check data types.** `datetime_diff` returns a float. `count` returns an integer. Declare `data_type` accordingly or queries may produce unexpected results.

**Null propagation.** Most arithmetic and functions return null if any input is null. Use `default(x, 0)` or `coalesce()` in your expression if you want a fallback — but be careful not to mask real missing data.

**`>-` in YAML for multiline expressions.** Long expressions should use the YAML block scalar `>-` to write them on multiple lines without literal newlines being inserted.

---

## Next steps

→ [[queries|Writing Queries]] — aggregate features and entity columns into metrics
