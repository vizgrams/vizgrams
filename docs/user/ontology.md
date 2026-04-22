# Designing Your Ontology

The ontology defines the **semantic entity model** for your data — what things exist in your domain, what properties they have, and how they relate to each other. It lives in `models/<model>/ontology/` as one YAML file per entity.

→ [[index|← Back to home]]

---

## What is an entity?

An entity is a named, identifiable thing in your domain. Examples: `PullRequest`, `Sprint`, `Deployment`, `Repository`, `Engineer`. Each entity maps to a `sem_<entity_snake_case>` table in your database.

Start by listing the core concepts you want to track. Then ask: how do they relate? A `PullRequest` belongs to a `Repository`. A `Sprint` belongs to a `Team`. Those relationships become relations in your ontology.

---

## Entity YAML

```yaml
# models/my_model/ontology/pull_request.yaml

entity: PullRequest
description: "A GitHub Pull Request"

identity:
  pull_request_key:
    type: STRING
    semantic: PRIMARY_KEY

attributes:
  title:
    type: STRING
    semantic: IDENTIFIER
  created_at:
    type: STRING
    semantic: TIMESTAMP
  merged_at:
    type: STRING
    semantic: TIMESTAMP
  state:
    type: STRING
  story_points:
    type: INTEGER
    semantic: MEASURE

relations:
  belongs_to:
    target: Repository
    via: repository_key
    cardinality: MANY_TO_ONE
  is_authored_by:
    target: Identity
    via: author_identity_key > identity_key
    cardinality: MANY_TO_ONE
```

---

## identity block

Every entity must have exactly one identity column tagged `PRIMARY_KEY`. This is the stable unique identifier for each row.

```yaml
identity:
  pull_request_key:
    type: STRING
    semantic: PRIMARY_KEY
```

Choose identity column names that are stable and meaningful: `pull_request_key`, `sprint_id`, `deployment_key`.

---

## attributes block

Attributes are the columns on the entity. Declare each with a `type` and an optional `semantic` hint.

### Column types

| Type | Use for |
|------|---------|
| `STRING` | Text, timestamps (stored as ISO strings), identifiers |
| `INTEGER` | Counts, IDs, ordinal values |
| `FLOAT` | Measurements, rates, scores |

### Semantic hints

Semantic hints help the query engine understand what a column means. They're optional but enable smart behaviour (e.g. `TIMESTAMP` columns become eligible for `format_time` bucketing in queries).

| Hint | Meaning |
|------|---------|
| `PRIMARY_KEY` | Unique identifier — exactly one required per entity |
| `IDENTIFIER` | Human-readable label/name for the entity |
| `TIMESTAMP` | ISO datetime string; eligible for time bucketing in queries |
| `MEASURE` | Numeric value that can be aggregated |
| `ATTRIBUTE` | Plain descriptive field (default if omitted) |
| `RELATION` | Foreign key to another entity |
| `ENTITY` | Holds an entity type name (used for polymorphic relations) |
| `STATE` | Lifecycle state value |
| `INSERTED_AT` | Auto-timestamp for append-mode event rows (required on events) |
| `SCD_FROM` / `SCD_TO` | Slowly-changing-dimension validity range |
| `ORDERING` | Sequence or ordering column |

---

## relations block

Relations define how entities connect to each other. They enable dot-notation traversal in queries and features (`Repository.Team.display_name`).

```yaml
relations:
  belongs_to:
    target: Repository           # PascalCase entity name
    via: repository_key          # FK column on this entity
    cardinality: MANY_TO_ONE

  commits:
    target: Commit
    via: [pull_request_key]      # list: shared column on the target side
    cardinality: ONE_TO_MANY

  is_authored_by:
    target: Identity
    via: author_identity_key > identity_key   # local_col > target_pk when names differ
    cardinality: MANY_TO_ONE
```

### Cardinality

| Value | Meaning |
|-------|---------|
| `MANY_TO_ONE` | Many rows of this entity → one row of target (FK on this side) |
| `ONE_TO_MANY` | One row of this entity → many rows of target (FK on target side) |

### `via` formats

| Format | When to use |
|--------|-------------|
| `via: column_name` | FK is on this entity; column names are the same |
| `via: local_col > target_pk` | FK is on this entity; column names differ |
| `via: [shared_col]` | ONE_TO_MANY; target has a matching column of the same name |

---

## events block

Events are append-only log tables attached to an entity. Use them for state transitions, lifecycle changes, or any sequence of dated records.

```yaml
events:
  lifecycle:
    description: "PR state transitions"
    attributes:
      from_state:
        type: STRING
        semantic: STATE
      to_state:
        type: STRING
        semantic: STATE
      occurred_at:
        type: STRING
        semantic: TIMESTAMP
      inserted_at:
        type: STRING
        semantic: INSERTED_AT    # required on every event
```

An event named `lifecycle` on entity `PullRequest` creates a table `sem_pull_request_lifecycle_event`.

**Every event must have an `INSERTED_AT` column.** This is used to track when the event was written to the database.

---

## Polymorphic (dynamic) relations

When a relation's target entity varies per row, use `target: dynamic(field_name)` where the named field holds the entity type:

```yaml
entity: Identity
attributes:
  subject_type:
    type: STRING
    semantic: ENTITY       # e.g. holds "Person" or "Machine"
  subject_key:
    type: STRING

relations:
  subject:
    target: dynamic(subject_type)
    via: subject_key
    cardinality: MANY_TO_ONE
```

In queries you can then traverse: `is_authored_by.subject.name` — the engine resolves the correct entity at runtime.

---

## Table naming conventions

| Entity | Table name |
|--------|------------|
| `PullRequest` | `sem_pull_request` |
| `SprintDelivery` | `sem_sprint_delivery` |
| `PullRequest` + event `lifecycle` | `sem_pull_request_lifecycle_event` |

---

## Design tips

**Start small.** Define only the entities you need for your first query. Add more later — the schema is additive.

**One primary key per entity.** Composite natural keys should be combined with `concat()` in the mapper, not split across columns.

**Use `TIMESTAMP` consistently.** Any ISO datetime string column that you might want to filter or group by should be tagged `TIMESTAMP`. This enables `format_time()` bucketing and `now() - Nd` relative filters in queries.

**Prefer `STRING` for timestamps.** Store timestamps as ISO 8601 strings (`2026-03-01T14:00:00Z`). The expression engine handles date arithmetic on strings.

**Name relations by their meaning.** Use `belongs_to`, `is_authored_by`, `is_owned_by` — not just `repository` or `author`. This makes traversals read naturally: `is_authored_by.subject.name`.

**Separate slowly-changing attributes into events.** If a field changes over time and you care about the history (state, priority, owner), model it as an event rather than overwriting a column.

---

## Next steps

Once your ontology is defined, write mappers to populate the `sem_*` tables:

→ [[mappers|Creating Mappers]]
