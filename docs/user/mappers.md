# Creating Mappers

Mappers transform raw extracted data (`raw_*` tables) into clean semantic entity tables (`sem_*`). Each mapper describes the sources, joins, and column expressions needed to produce one or more entities.

→ [[index|← Back to home]]

---

## How mappers work

A mapper reads from one or more `raw_*` source tables, optionally joins them together, and writes columns to a `sem_*` table. Column values are computed using the [[expression|expression language]] in `expr:` fields.

```
raw_github_pull_requests  ──┐
                             ├──[mapper]──► sem_pull_request
sem_identity              ──┘
```

---

## Simple mapper

The most common case: one primary source table, optionally joined to others.

```yaml
# models/my_model/mappers/pull_request.yaml

mapper: pull_request
description: "Map GitHub pull requests to PullRequest entity"
depends_on: [identity]      # other mappers that must run first

grain: pr                   # alias for the primary source table

sources:
  - alias: pr
    table: raw_github_pull_requests
    columns: [repo_name, number, title, author, created_at, merged_at, state]

  - alias: id_author
    table: sem_identity
    columns: [identity_key, value, system, type]
    filter:
      and:
        - system == 'github'
        - type == 'handle'
    deduplicate: [value]    # keep one row per value when multiple matches exist

joins:
  - from: pr
    to: id_author
    type: left
    "on":
      - left: pr.author
        right: id_author.value

targets:
  - entity: PullRequest
    columns:
      - name: pull_request_key
        expr: concat(pr.repo_name, "#", pr.number)
      - name: repository_key
        expr: pr.repo_name
      - name: author_identity_key
        expr: id_author.identity_key
      - name: title
        expr: pr.title
      - name: created_at
        expr: pr.created_at
      - name: merged_at
        expr: pr.merged_at
      - name: state
        expr: pr.state
```

### Key fields

| Field | Description |
|-------|-------------|
| `mapper` | Unique mapper name (matches filename stem) |
| `depends_on` | List of mapper names that must run before this one |
| `grain` | Alias of the primary source — the table that drives the row count |
| `sources` | List of tables to read from |
| `joins` | How to join sources together |
| `targets` | Which entities to write columns to |

---

## Sources

```yaml
sources:
  - alias: pr
    table: raw_github_pull_requests
    columns: [id, repo_name, title, author]   # columns to select
    filter: state != 'draft'                   # optional row filter
    deduplicate: [id]                          # keep first match per key
```

### Source filters

Filters are applied before the join. They use the expression language — see [[expression#Filter methods|filter methods]] for the full syntax.

```yaml
# Simple equality
filter: system == 'github'

# Null test
filter: email.not_null()

# Combined
filter:
  and:
    - status != 'Closed'
    - or:
        - priority >= 2
        - assignee.not_null()
```

### `deduplicate`

When a source could produce multiple matching rows for a given key (e.g. two identity records with the same handle), use `deduplicate` to keep only one:

```yaml
deduplicate: [value]      # keep first row per distinct value
```

---

## Joins

```yaml
joins:
  - from: pr
    to: id_author
    type: left               # left | inner | right | full
    "on":
      - left: pr.author
        right: id_author.value
        operator: eq         # eq (default) | json_array_contains | scalar_or_array_contains
```

### Join operators

| Operator | When to use |
|----------|-------------|
| `eq` | Standard equality join (default) |
| `json_array_contains` | Left value exists in a JSON array on the right side; specify `json_path` on the condition |
| `scalar_or_array_contains` | Left value matches either a scalar or a JSON array column on the right |

---

## Column expressions

Each column in `targets` has a `name` and an `expr`. The expression can reference any source alias:

```yaml
columns:
  - name: pull_request_key
    expr: concat(pr.repo_name, "#", pr.number)   # build a composite key

  - name: author_identity_key
    expr: id_author.identity_key                  # from a joined source

  - name: environment
    expr: enum(d.env_raw, deployment_environment) # enum mapping

  - name: identity_key
    expr: concat("identity_", ulid(concat("github", "|", pr.author)))  # deterministic ID
```

Full expression reference: [[expression|Expression Language]]

---

## Enum mappings

Define named lookup tables directly in the mapper YAML, then reference them with `enum()`:

```yaml
enums:
  deployment_environment:
    PROD: [production, prod, prd]
    STAGING: [staging, stg]
    DEV: [dev, development, local]

targets:
  - entity: Deployment
    columns:
      - name: environment
        expr: enum(d.env_raw, deployment_environment)
```

If the source value doesn't match any mapping entry, an error is raised.

---

## Multi-group mappers

When multiple independent source groups should all produce rows for the same entity (e.g. identities from GitHub users, Jira users, and a static file), use the `rows:` structure inside `targets` instead of `columns:`. Omit `grain:` at the top level.

```yaml
mapper: identity
description: "Build identity entities from multiple sources"

sources:
  - alias: github_users
    table: raw_github_users
    columns: [login, name, email]

  - alias: jira_users
    table: raw_jira_users
    columns: [account_id, display_name, email_address]

  - alias: file_people
    table: raw_file_people
    columns: [person_key, github_handle, jira_id]
    deduplicate: [github_handle]

targets:
  - entity: Identity
    rows:
      # Row group 1: one Identity row per GitHub user
      - from: github_users
        joins:
          - to: file_people
            type: left
            "on":
              - left: github_users.login
                right: file_people.github_handle
        columns:
          - name: identity_key
            expr: concat("identity_", ulid(concat("github|handle|", github_users.login)))
          - name: system
            expr: '"github"'
          - name: type
            expr: '"handle"'
          - name: value
            expr: github_users.login
          - name: subject_type
            expr: if_not_null(file_people.person_key, "Person")

      # Row group 2: one Identity row per Jira user
      - from: jira_users
        joins:
          - to: file_people
            type: left
            "on":
              - left: jira_users.account_id
                right: file_people.jira_id
        columns:
          - name: identity_key
            expr: concat("identity_", ulid(concat("jira|account_id|", jira_users.account_id)))
          - name: system
            expr: '"jira"'
          - name: type
            expr: '"account_id"'
          - name: value
            expr: jira_users.account_id
          - name: subject_type
            expr: if_not_null(file_people.person_key, "Person")
```

Each `rows` entry has its own `from`, `joins`, and `columns` — essentially a separate mini-mapper that contributes rows to the same entity table.

---

## Mapping events

To populate event tables, add an event target alongside or instead of the entity target:

```yaml
targets:
  - entity: PullRequest
    columns:
      - name: pull_request_key
        expr: concat(pr.repo_name, "#", pr.number)
      # ... other columns

  - entity: PullRequest
    event: lifecycle
    columns:
      - name: pull_request_key
        expr: concat(pr.repo_name, "#", pr.number)
      - name: from_state
        expr: timeline.from_state
      - name: to_state
        expr: timeline.to_state
      - name: occurred_at
        expr: timeline.created_at
      - name: inserted_at
        expr: timeline.inserted_at
```

The `event:` field names the event defined in the ontology. Event tables use `APPEND` write mode — each mapper run adds new rows rather than replacing.

---

## CLI reference

```bash
# Show the mapper definition for an entity
./run.sh entity mapper PullRequest --model my_model

# Run the mapper for a single entity
./run.sh entity mapper-execute PullRequest --model my_model

# Run all mappers (in dependency order)
./run.sh entity mapper-execute '*' --model my_model

# Validate mapper YAML without running
./run.sh entity mapper-validate PullRequest --model my_model
```

Mapper runs are synchronous — they complete before the command returns.

---

## Validation

Before running, the system validates:

1. **Schema** — required fields present, types correct
2. **Sources** — referenced `raw_*` tables exist
3. **Column names** — output columns match the entity's ontology definition
4. **Expressions** — expression syntax is valid

Use `mapper-validate` to catch issues before committing to a run.

---

## Tips

**Map one entity per file.** It's easier to reason about and run selectively.

**Use `depends_on` explicitly.** If your mapper reads from a `sem_*` table, declare it in `depends_on` so the engine runs mappers in the right order.

**Build composite keys with `concat` + `ulid`.** Natural keys composed of multiple fields should be combined: `concat(repo_name, "#", pr_number)` or hashed with `ulid()` for a shorter stable ID.

**Use `deduplicate` defensively.** When joining to a lookup table, add `deduplicate` on the join key — if the lookup has duplicates, the join will multiply your rows.

**String literals need inner quotes.** To write the literal string `"github"` in an expression, quote it as `'"github"'` in YAML: the outer single quotes protect the YAML, the inner double quotes mark the string.

---

## Next steps

→ [[features|Writing Features]] — add computed columns to your entities
