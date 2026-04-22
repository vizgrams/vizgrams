# Managing Models

A **model** is a self-contained workspace. It has its own configuration, raw data, semantic entities, features, and queries. Think of it as a project: one model per product area, team, or use case.

→ [[index|← Back to home]]

---

## Directory structure

```
models/
  registry.yaml                  ← global model registry
  <model_name>/
    config.yaml                  ← tool credentials (managed via CLI)
    extractors/
      extractor_git.yaml         ← what data to pull from each tool
      extractor_jira.yaml
    input_data/                  ← static reference files (YAML, JSON, CSV)
    ontology/
      pull_request.yaml          ← one file per entity
      repository.yaml
    mappers/
      pull_request.yaml          ← one file per entity
    features/
      pull_request.fc_hours.yaml ← one file per computed column
    queries/
      pr_throughput.yaml         ← one file per query
    data/
      data.db                    ← SQLite database (auto-created)
```

---

## Registering a model

Models are registered via `model create` — you do not edit `models/registry.yaml` directly. The identifier you choose (`name`) is used in all CLI commands and API paths.

---

## Tool configuration (`config.yaml`)

Each model has a `config.yaml` that lists which tools are enabled and their credentials. It is written and managed through the CLI — you do not edit it directly.

Credentials must always use `env:` or `file:` references so the file is safe to commit:

| Format | Example | Behaviour |
|--------|---------|-----------|
| `file:<path>` | `file:~/.secrets/token` | Reads file content, strips whitespace |
| `env:<VAR>` | `env:GITHUB_PAT` | Reads from environment variable |

Literal credential values are rejected at write time.

```bash
# See what tools are configured
./run.sh tool config-list --model my_model

# Add or replace a tool's config
./run.sh tool config-set git --model my_model --data '{
  "enabled": true,
  "org": "MyOrg",
  "host": "github.com",
  "token": "env:GITHUB_PAT"
}'

./run.sh tool config-set jira --model my_model --data '{
  "enabled": true,
  "server": "https://mycompany.atlassian.net",
  "email": "me@example.com",
  "api_token": "env:JIRA_TOKEN"
}'

./run.sh tool config-set file --model my_model --data '{"enabled": true}'

# Toggle a tool off without losing its config
./run.sh tool config-patch jira --model my_model --data '{"enabled": false}'

# Remove a tool entirely
./run.sh tool config-delete jira --model my_model
```

---

## Selecting your active model

The system checks these sources in priority order:

1. `--model` flag on the CLI command
2. `WT_MODEL` environment variable
3. `.vz_context` file in the project root

For day-to-day work, set `.vz_context`:

```bash
echo "my_model" > .vz_context
```

Or with multiple settings:
```
MODEL=my_model
```

---

## CLI reference

```bash
# List all models
./run.sh model list

# Get a model's details
./run.sh model get --model my_model

# Create a new model (scaffolds directories + config.yaml + registry entry)
./run.sh model create --data '{
  "name": "my_model",
  "display_name": "My Model",
  "description": "What this model tracks",
  "owner": "team-name"
}'

# Update model metadata
./run.sh model update --data '{"description":"updated"}' --model my_model

# Archive a model (marks it inactive)
./run.sh model archive --reason "replaced by v2" --model my_model

# Set a model as active
./run.sh model set-active --model my_model
```

---

## Creating a model from scratch

`model create` does the full setup in one command — it creates the directory structure, writes an empty `config.yaml`, and registers the model:

```bash
./run.sh model create --data '{
  "name": "my_model",
  "display_name": "My Model",
  "description": "What this model tracks",
  "owner": "team-name",
  "tags": ["engineering"],
  "set_active": true
}'
```

The `set_active: true` flag also writes `.vz_context` so the model is immediately selected for subsequent commands.

Then configure your tools:

```bash
# Enable the tools you need (credentials via env vars or file references only)
./run.sh tool config-set git --model my_model --data '{
  "enabled": true,
  "org": "MyOrg",
  "host": "github.com",
  "token": "env:GITHUB_PAT"
}'

./run.sh tool config-set file --model my_model --data '{"enabled": true}'
```

Then follow the pipeline:

1. [[tools-and-extractors|Configure extractors]] and run them to populate `raw_*` tables
2. [[ontology|Design your ontology]] to define your entities
3. [[mappers|Write mappers]] to transform raw data into `sem_*` tables
4. [[features|Define features]] for computed columns
5. [[queries|Write queries]] to produce your metrics

---

## Full pipeline in one go

Once everything is configured:

```bash
# Extract all data
./run.sh extractor execute git
./run.sh extractor execute jira

# Map everything
./run.sh entity mapper-execute '*'

# Materialise features
./run.sh feature reconcile

# Run a query
./run.sh query execute my_query --format csv
```
