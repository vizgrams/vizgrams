# vizgrams — User Guide

vizgrams is a **declarative data pipeline platform** that lets you define your data model, pull data from external sources, transform it into a clean semantic layer, compute enriched features, and run analytical queries — all in YAML.

---

## What can I build with it?

vizgrams is designed for engineering and product intelligence use cases: tracking pull requests, sprint delivery, deployment frequency, build durations, team ownership, and any other metric you can derive from your toolchain. You define *what* your data means (ontology), *where* it comes from (extractors), *how* it maps to your model (mappers), and *what* you want to measure (features and queries).

Data is stored in a local SQLite database. You query it through the same YAML definitions regardless of your model's size or shape.

---

## Core concepts

| Concept | What it is | Where it lives |
|---------|-----------|----------------|
| **Model** | A named, self-contained workspace with its own data, config, and definitions | `models/<name>/` |
| **Extractor** | Pulls raw data from a Tool (Jira, GitHub, files) into `raw_*` tables | `extractors/extractor_*.yaml` |
| **Ontology** | Defines the semantic entity model — what entities exist and how they relate | `ontology/*.yaml` |
| **Mapper** | Transforms `raw_*` tables into clean `sem_*` semantic tables | `mappers/*.yaml` |
| **Feature** | A computed column materialised into a feature store | `features/<entity>.<name>.yaml` |
| **Query** | A declarative GROUP BY aggregation over entities and features | `queries/*.yaml` |

---

## How the pipeline flows

```
External systems (Jira, GitHub, files)
          │  Tools + Extractors
          ▼
     raw_* tables          ← raw, unprocessed data
          │  Mappers
          ▼
     sem_* tables          ← clean semantic entities
          │  Features
          ▼
   feature_value store     ← computed columns
          │  Queries
          ▼
   Analytical results      ← your metrics
```

---

## Getting started fast

New here? Start with the end-to-end walkthrough:

→ [[quickstart|Quickstart — Build Your First Model]]

---

## Step-by-step guide

**Step 1 — Create a model.** Register it in `models/registry.yaml` and set up its directory structure and `config.yaml`.

→ [[models|Managing Models]]

**Step 2 — Configure tools and extractors.** Tell the system where your data comes from and what to pull.

→ [[tools-and-extractors|Tools & Extractors]]

**Step 3 — Design your ontology.** Define the entities (PullRequest, Sprint, Deployment…) that your model tracks.

→ [[ontology|Designing Your Ontology]]

**Step 4 — Write mappers.** Transform your raw extracted data into the semantic entity tables.

→ [[mappers|Creating Mappers]]

**Step 5 — Define features.** Add computed columns — durations, ratios, classifications — to your entities.

→ [[features|Writing Features]]

**Step 6 — Write queries.** Define the aggregations and metrics you want to expose.

→ [[queries|Writing Queries]]

---

## Expression language

A single expression language is used in `expr:` fields across mappers, features, and queries. Learn the full syntax here:

→ [[expression|Expression Language Reference]]

---

## Running commands

The CLI (`run.sh`) wraps the REST API. Start the API server first:

```bash
uvicorn api.main:app --reload
```

Then use `run.sh`:

```bash
# Set your active model (one-time)
echo "example" > .vz_context

# Or pass --model on every command
./run.sh --model example query list
```

### Quick reference

```bash
# Extract data from all tools
./run.sh extractor execute git
./run.sh extractor execute jira

# Map raw data to semantic entities
./run.sh entity mapper-execute '*'

# Materialise computed features
./run.sh feature reconcile

# Run a query
./run.sh query execute my_query
```

See the relevant pages above for full command references.

---

## Interactive API docs

With the server running, visit `http://localhost:8000/docs` for the full interactive REST API.

---

## Other reference pages

- [[reference|YAML Schemas & CLI Reference]] — field-by-field schema and full CLI command listing
- [[quickstart|Quickstart]] — end-to-end walkthrough from zero to first query
