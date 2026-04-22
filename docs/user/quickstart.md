# Quickstart — Build Your First Model

This walkthrough creates a minimal working model from scratch. By the end you'll have extracted GitHub PR data, mapped it to a semantic entity, and run a query.

→ [[index|← Back to home]]

---

## What we're building

A model called `my_eng` that tracks pull requests from GitHub and answers: _"How many PRs were merged per month, and what was the average cycle time?"_

---

## Prerequisites

- Python environment with dependencies installed
- GitHub personal access token saved to `~/.secrets/github_pat`
- API server running: `uvicorn api.main:app --reload`

---

## Step 1 — Register the model

Add your model to `models/registry.yaml`:

```yaml
models:
  my_eng:
    display_name: "My Engineering Model"
    description: "GitHub PR metrics"
    owner: "my-team"
    created_at: '2026-01-01T00:00:00Z'
    status: active
    tags:
      - engineering
```

Create the directory structure:

```bash
mkdir -p models/my_eng/{extractors,input_data,ontology,mappers,features,queries,data}
```

Set it as your active model:

```bash
echo "my_eng" > .vz_context
```

---

## Step 2 — Configure the tool

```yaml
# models/my_eng/config.yaml

tools:
  git:
    enabled: true
    org: "MyOrg"                          # your GitHub org
    host: "github.com"
    token: "file:~/.secrets/github_pat"
```

---

## Step 3 — Write an extractor

```yaml
# models/my_eng/extractors/extractor_git.yaml

tasks:
  - name: github_pull_requests
    tool: git
    command: pull_requests
    incremental: true
    params:
      repo: "*"                            # pull from all repos in the org
    context:
      repo: repo_name
    output:
      table: github_pull_requests
      write_mode: UPSERT
      primary_keys: [id]
      columns:
        - name: id
          json_path: $.id
        - name: repo_name
          json_path: $.base.repo.name
        - name: number
          json_path: $.number
        - name: title
          json_path: $.title
        - name: author
          json_path: $.user.login
        - name: state
          json_path: $.state
        - name: created_at
          json_path: $.created_at
        - name: merged_at
          json_path: $.merged_at
```

Run it:

```bash
./run.sh extractor execute git
```

Check the job completed:

```bash
./run.sh job list
```

---

## Step 4 — Define an entity

```yaml
# models/my_eng/ontology/pull_request.yaml

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
  author:
    type: STRING
  state:
    type: STRING
  created_at:
    type: STRING
    semantic: TIMESTAMP
  merged_at:
    type: STRING
    semantic: TIMESTAMP
  repository_key:
    type: STRING
```

---

## Step 5 — Write a mapper

```yaml
# models/my_eng/mappers/pull_request.yaml

mapper: pull_request
description: "Map GitHub PRs to PullRequest entity"
grain: pr

sources:
  - alias: pr
    table: raw_github_pull_requests
    columns: [id, repo_name, number, title, author, state, created_at, merged_at]

targets:
  - entity: PullRequest
    columns:
      - name: pull_request_key
        expr: concat(pr.repo_name, "#", cast(pr.number, string))
      - name: repository_key
        expr: pr.repo_name
      - name: title
        expr: pr.title
      - name: author
        expr: pr.author
      - name: state
        expr: pr.state
      - name: created_at
        expr: pr.created_at
      - name: merged_at
        expr: pr.merged_at
```

Run it:

```bash
./run.sh entity mapper-execute PullRequest
```

---

## Step 6 — Add a feature

```yaml
# models/my_eng/features/pull_request.hours_to_merge.yaml

feature_id: pull_request.hours_to_merge
name: Hours to Merge
description: "Hours from PR creation to merge."
entity_type: PullRequest
entity_key: pull_request_key
data_type: FLOAT
materialization_mode: materialized
expr: datetime_diff(created_at, merged_at, unit="hours")
```

Materialise it:

```bash
./run.sh feature reconcile
```

---

## Step 7 — Write a query

```yaml
# models/my_eng/queries/pr_throughput.yaml

name: pr_throughput
root: PullRequest

attributes:
  - month: format_time(merged_at, "YYYY-MM")
    order: 1, asc
  - repo: repository_key

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

where:
  - merged_at.not_null()
  - merged_at >= now() - 12w
```

Register and run it:

```bash
./run.sh query upsert pr_throughput models/my_eng/queries/pr_throughput.yaml
./run.sh query execute pr_throughput
```

---

## What's next?

- Add more entities: [[ontology|Designing Your Ontology]]
- Pull from Jira: [[tools-and-extractors|Tools & Extractors]]
- Join multiple sources in a mapper: [[mappers|Creating Mappers]]
- Build rolling window metrics: [[queries|Writing Queries]]
- Learn all expression syntax: [[expression|Expression Language Reference]]
