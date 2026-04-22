# Tools & Extractors

**Tools** are connectors to external systems (GitHub, Jira, local files). **Extractors** are YAML definitions that tell a tool what data to pull and how to store it. Running an extractor writes rows into `raw_*` tables in your model's database.

→ [[index|← Back to home]]

---

## Available tools

| Tool | What it connects to | Key commands |
|------|---------------------|--------------|
| `git` | GitHub REST API | `repos`, `pull_requests`, `commits`, `teams`, `deployments`, `workflow_runs`, … |
| `jira` | Jira REST API | `issues`, `boards`, `sprints`, `projects`, `users`, `fields` |
| `git_codeowners` | GitHub CODEOWNERS files | `owners` |
| `file` | Local YAML / JSON / CSV files | `load` |

Tools are enabled per-model in `config.yaml`. See [[models|Managing Models]] for credential setup.

---

## Extractor YAML structure

Extractors live in `models/<model>/extractors/` and are named `extractor_<tool>.yaml`. Each file defines one or more **tasks**.

```yaml
# models/my_model/extractors/extractor_git.yaml

tasks:
  - name: github_repos
    tool: git
    command: repos
    params:
      repo: "*"             # "*" = pull all repos in the org
    context:
      repo: repo_name       # write the expanded repo name into this column
    output:
      table: github_repos   # stored as raw_github_repos
      write_mode: UPSERT
      primary_keys: [id]
      columns:
        - name: id
          json_path: $.id
        - name: repo_name
          json_path: $.name
        - name: created_at
          json_path: $.created_at
```

### Write modes

| Mode | Behaviour | Best for |
|------|-----------|----------|
| `UPSERT` | Insert or replace by primary key | Reference data, issues, PRs |
| `APPEND` | Always insert; adds `inserted_at` column | Event logs, changelog entries |
| `REPLACE` | Delete everything, re-insert | Full-refresh dimension tables |

### Incremental extraction

Set `incremental: true` on a task to use the last-run timestamp as a cutoff for the API call — only newer records are fetched:

```yaml
  - name: github_commits
    tool: git
    command: commits
    incremental: true
    params:
      repo: "*"
    output:
      table: github_commits
      write_mode: APPEND
      columns:
        - name: sha
          json_path: $.sha
        - name: committed_at
          json_path: $.commit.author.date
```

---

## Wildcard expansion

When a param is set to `"*"`, the tool resolves it to a full list (e.g. all repos in the org) and runs the command once per value. Use `context:` to capture the expanded value as a column:

```yaml
params:
  repo: "*"               # expands to ["repo-a", "repo-b", "repo-c", ...]
context:
  repo: repo_name         # writes the repo name into a "repo_name" column
```

---

## Column extraction with `json_path`

Each column maps a JSON path from the API response to a database column:

```yaml
columns:
  - name: author_login
    json_path: $.user.login          # nested field
  - name: label_names
    json_path: $.labels[*].name      # array of values → JSON string
    type: JSON                       # store as JSON (default: STRING)
  - name: story_points
    json_path: $.fields.story_points
    type: INTEGER
```

Supported `type` values: `STRING` (default), `INTEGER`, `FLOAT`, `JSON`.

---

## Exploding nested arrays

Some API responses contain nested arrays that should become separate rows. Use `row_source.mode: EXPLODE`:

```yaml
  - name: jira_changelogs
    tool: jira
    command: issues
    params:
      project: "MYPROJ"
    outputs:                          # note: "outputs" (plural) for multiple tables
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
          json_path: $.changelog.histories    # iterate this array
          inherit:
            issue_key: $.key                  # pull parent field into each child row
        columns:
          - name: history_id
            json_path: $.id
          - name: author
            json_path: $.author.displayName
          - name: created
            json_path: $.created
```

Use `output` (singular) when producing one table; `outputs` (plural) when producing multiple tables from the same API call.

---

## Loading static files

Use the `file` tool to load reference data from YAML, JSON, or CSV files in `input_data/`:

```yaml
  - name: file_teams
    tool: file
    command: load
    params:
      path: models/my_model/input_data/teams.yaml
      format: yaml
    output:
      table: file_teams
      write_mode: REPLACE
      row_source:
        mode: EXPLODE
        json_path: $.teams           # iterate the teams array in the file
      columns:
        - name: team_key
          json_path: $.key
        - name: display_name
          json_path: $.name
        - name: members
          json_path: $.members
          type: JSON
```

Example `input_data/teams.yaml`:
```yaml
teams:
  - key: platform
    name: Platform Engineering
    members: ["alice", "bob"]
  - key: data
    name: Data Engineering
    members: ["carol"]
```

---

## CLI reference

```bash
# List extractors for a model
./run.sh extractor list --model my_model

# Inspect a specific tool's tasks
./run.sh extractor get git --model my_model

# Run all tasks for a tool
./run.sh extractor execute git --model my_model

# Run a single task
./run.sh extractor execute git --task github_commits --model my_model

# Force full refresh (ignore incremental state)
./run.sh extractor execute git --full-refresh --model my_model

# Validate extractor YAML without running
./run.sh extractor validate git --model my_model
```

Extractor runs are **asynchronous**. The command returns a job ID immediately; use `job list` or `job get` to check progress:

```bash
./run.sh job list --status running --model my_model
./run.sh job get <job_id> --model my_model
```

---

## Writing a custom tool

If you need to pull from a system that isn't built in, you can write your own tool.

→ [[writing-a-tool|Writing a Custom Tool]]

---

## Tool command reference

### `git` tool

| Command | Description | Key params |
|---------|-------------|------------|
| `repos` | Repositories in the org | `repo: "*"` |
| `pull_requests` | PRs for a repo | `repo`, `state` |
| `commits` | Commits for a repo | `repo` |
| `teams` | Teams in the org | — |
| `team_members` | Members of a team | `team` |
| `users` | Org members | — |
| `releases` | Releases for a repo | `repo` |
| `workflow_runs` | GitHub Actions runs | `repo` |
| `deployments` | Deployments for a repo | `repo` |
| `pr_timeline` | PR timeline events | `repo`, `pr_number` |
| `pr_commits` | Commits on a PR | `repo`, `pr_number` |
| `pr_reviews` | PR reviews | `repo`, `pr_number` |
| `tags` | Git tags | `repo` |

### `jira` tool

| Command | Description | Key params |
|---------|-------------|------------|
| `issues` | Issues/tickets (with changelog) | `project`, `board_id` |
| `boards` | Boards in a project | `project: "*"` |
| `sprints` | Sprints on a board | `board_id: "*"` |
| `projects` | All Jira projects | — |
| `users` | Jira users | — |
| `fields` | Custom field definitions | — |

### `git_codeowners` tool

| Command | Description | Key params |
|---------|-------------|------------|
| `owners` | CODEOWNERS entries per repo | `repo: "*"` |

### `file` tool

| Command | Description | Key params |
|---------|-------------|------------|
| `load` | Load a local file | `path`, `format` (`yaml`/`json`/`csv`) |
