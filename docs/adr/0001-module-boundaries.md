# ADR-0001 — Module boundaries (modular monolith)

**Status:** accepted (2026-05-24)

## Context

The backend has grown to ~110 Python files across `api/`, `core/`, `engine/`,
`semantic/`, `batch/`, and `batch_service/`. There are six functional areas
that share the codebase:

1. **Extraction** — pulls raw data into the system (extractors, mappers,
   the batch scheduler, materialisation).
2. **Ontology** — entity + relationship definitions, schema, ontology editor.
3. **Features** — computed feature definitions, the expression engine,
   reconcile / materialise paths.
4. **Catalog** — queries, views, applications. The reusable semantic layer
   end users see in the library.
5. **Chat** — the LLM tool-calling orchestrator and chat-publish path.
6. **Feed** — vizgrams, ranking, significance, engagement, the public surface
   that aggregates published artefacts.

Plus cross-cutting infrastructure:

- **Auth / RBAC** (oauth2-proxy headers, role checks, service accounts)
- **Storage primitives** (SQLite per-model `metadata_db`, shared `vizgrams_db`,
  the `DBBackend` ABC over ClickHouse / DuckDB / SQLite)
- **Per-model config** (`model_config`, ontology dirs, tool registry)

These were spread across `api/`, `core/`, `semantic/`, and `engine/` based
on layering rules ("low-level vs high-level") rather than functional module.
Result: a flat 19-file `api/services/` directory where you cannot see at a
glance which files belong together.

## Decision

We are a **modular monolith** until at least one of these triggers fires:

- Two modules need very different scaling profiles (LLM throughput vs feed
  read traffic vs batch extraction)
- Two modules need very different reliability targets
- Team grows past ~3-5 engineers and module ownership starts to mean
  something
- A module needs a different runtime (Go / Rust hot path)

Until then: keep one Python codebase, one deployment artifact (API + a
small batch sidecar for scheduling), and **enforce boundaries by convention
+ doc**, not by network hops.

## Module map

```
┌─────────────────────────────────────────────────────────────────────┐
│ feed       — published vizgrams + ranking + engagement              │
│ chat       — LLM tool loop + chat publish flow                      │
│ catalog    — queries / views / applications (semantic layer)        │
│ features   — computed features + expression engine                  │
│ ontology   — entity definitions + ontology editor                   │
│ extraction — extractors + mappers + scheduler + materialisation     │
└─────────────────────────────────────────────────────────────────────┘
            ↑ each may depend on the modules below ↑

┌─────────────────────────────────────────────────────────────────────┐
│ infra  — metadata_db, vizgrams_db, db (backends), rbac, config,     │
│          job service, version routes, validation, retry             │
└─────────────────────────────────────────────────────────────────────┘
```

Files map roughly to:

| Module | Backend code |
|---|---|
| feed | `api/services/feed_*`, `api/routers/vizgrams.py`, `core/vizgrams_db.py` (mixed — see "Known issues") |
| chat | `api/services/chat/*`, `api/routers/chat.py`, `semantic/llm/*` |
| catalog | `api/services/{query,view,application}_service.py`, `api/routers/{queries,views,applications}.py`, `semantic/{query,view,application}.py`, `engine/query_runner.py` |
| features | `api/services/feature_service.py`, `api/routers/features.py`, `semantic/feature.py`, `semantic/expression.py`, `engine/expression_compiler.py` |
| ontology | `api/services/entity_service.py`, `api/routers/entities.py`, `semantic/ontology.py`, `semantic/types.py` |
| extraction | `api/services/{extractor,mapper,materialize,input_data}_service.py`, `batch_service/*`, `batch/*`, `engine/extractor.py`, `engine/mapper.py` |
| infra | `core/*` (excluding feed-y ones), `api/dependencies.py`, `api/main.py`, `api/limiter.py` |

## Rules

1. **Layers**: `core → engine + semantic → api`. The `core/` layer must
   not import anything from `api/`, `batch/`, or `engine/`. `semantic/`
   and `engine/` must not import from `api/` or `batch/`. Enforced by
   convention; CI lint deferred.

2. **Module ownership of cross-cutting code**: business logic used by
   exactly one module belongs in that module's files, not in `core/`.
   Recent moves (this ADR): `significance.py` moved from `core/` →
   `api/services/feed_significance.py` because only feed publish paths
   call it.

3. **Cross-module imports**: a higher-up module may import a lower-up
   module's *service-layer interface* (e.g. chat may call `query_service`,
   `view_service` to compose the catalog). Modules at the same level
   should not import each other's internals.

4. **Naming**: a service-layer file is `{module}_service.py` (singleton
   surface for one concept — `query_service.py`) or lives in a `{module}/`
   directory when there are multiple cohesive files (`chat/service.py`,
   `chat/publish.py`).

5. **Re-export shims are temporary**: when a file moves, prefer updating
   call sites in the same PR over leaving a re-export. Removed in this
   ADR: `core/version_routes.py` (re-exported from `api/`),
   `api/services/tool_service.py` (re-exported from `core/`).

## Known issues + deferred cleanups

- **`core/vizgrams_db.py` (838 LOC) is mixed concerns** — half schema +
  migrations + user table, half feed-query layer (list_feed, ranking).
  Future cleanup: split into `core/identity_db.py` (users) +
  `api/services/feed_store.py` (vizgrams + engagement + ranking).
- **`core/ranking.py` is feed-only** but `core/vizgrams_db.py:list_feed`
  calls it. Move comes with the vizgrams_db split above.
- **`core/caption_provider.py`** is used by both chat publish and the
  view-toolbar publish flow. Genuinely cross-cutting — stays in `core/`
  for now.

## Consequences

- New code goes into the module that owns the concept; the layer
  decision becomes secondary.
- Reviewing module boundaries is part of code review (any new
  cross-module import gets called out).
- When/if we split, the module groupings here become the seam.
