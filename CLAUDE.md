# Project orientation

## What is vizgrams

A semantic-layer + chart-publishing platform. Users define an **ontology**
(entities + relations), **extract** raw data into it, define **queries** /
**views** / **applications** over it, and **publish** chart snapshots to a
**feed** that other users can like / save / share. There's a **chat** surface
on top that authors queries + picks charts via an LLM tool-calling loop.

## Glossary

| Term | Meaning |
|---|---|
| **Entity** | An ontology type (`Person`, `PullRequest`). Has identity, attributes, relations. |
| **Mapper** | Transforms raw extracted rows into entity instances. One per target entity. |
| **Extractor** | Pulls raw data from a source (GitHub, Jira, files) into staging tables. |
| **Feature** | Computed column on an entity (`pull_request.is_open`). YAML defs; reconciled to a feature store. |
| **Query** | A semantic-layer SELECT — root entity + attributes + measures + filters. YAML def → compiled to dialect-specific SQL. |
| **View** | Visualisation config that wraps a query (`chart_type`, axes, drilldowns). YAML def. |
| **Application** | A layout of multiple views — a dashboard. |
| **Vizgram** | A published snapshot of a view's data + chart. Lives in the feed; can be liked / saved / shared. |
| **Chat turn** | One assistant exchange. Produces either a saved-view ref (path A) or an inline view yaml (paths B/C). |

## Module boundaries

See `docs/adr/0001-module-boundaries.md` for the full spec. Quick map:

```
feed       ← published vizgrams + ranking + engagement
chat       ← LLM tool loop + chat publish
catalog    ← queries / views / applications
features   ← computed features + expression engine
ontology   ← entity definitions
extraction ← extractors + mappers + scheduler
─────────────────────────────────────────────────
infra      ← metadata_db, vizgrams_db, db backends, rbac, config
```

Higher modules may depend on lower modules and on infra. Same-level
modules should not import each other's internals — chat can call
`query_service` / `view_service` (catalog's public surface), but
shouldn't reach into catalog internals.

## Layer rules

- `core/` must not import from `api/`, `batch/`, `engine/`.
- `semantic/` and `engine/` must not import from `api/` or `batch/`.
- Business logic used by exactly one module belongs in that module —
  not in `core/`. (e.g. `feed_significance.py` lives in
  `api/services/`, not `core/`.)

## Conventions

- **Service files**: `{concept}_service.py` (`query_service.py`,
  `view_service.py`). When a module has multiple cohesive files,
  group them in `{module}/` (`chat/service.py`, `chat/publish.py`).
- **No re-export shims**: when a file moves, update the call sites
  in the same PR.
- **YAML over JSON** for artefact configs (entities, mappers,
  queries, views, features, extractors).
- **All artefacts named** `^[a-z][a-z0-9_]*$`.

## Where things live

| You're looking for | Look in |
|---|---|
| HTTP routes | `api/routers/` |
| Pydantic request/response models | `api/schemas/` |
| Service-layer business logic | `api/services/` |
| YAML schemas (JSON Schema docs) | `schemas/` |
| Ontology / query / view parsers + validators | `semantic/` |
| Query → SQL compilation | `engine/` |
| LLM tools + orchestration | `semantic/llm/` |
| DB connection helpers + RBAC + config | `core/` |
| Background job scheduler | `batch_service/` |
| Extractor execution runtime | `batch/` |
| Frontend pages | `ui/src/pages/` |
| Frontend shared components | `ui/src/components/` |
| Frontend API client | `ui/src/api/client.ts` |
| Tests | `tests/` (mirrors source tree) |

## Running locally

```
poetry install
poetry run pytest tests/        # backend tests
cd ui && npm install && npm test  # UI tests
```

API + UI dev servers: see `Makefile` (`make api`, `make ui`).
