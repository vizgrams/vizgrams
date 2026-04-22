# Vizgrams — Product Spec

> Last updated: 2026-04-20

## Overview

Vizgrams is an "Instagram for charts" — a platform for publishing and consuming data visualisations as a social feed. Users publish "vizgrams" (chart posts) by slicing public (or private) datasets, with LLM-generated captions explaining what the viewer is seeing and calling out notable trends.

The semantic layer from work_tools2 is the underlying engine. Vizgrams is the product built on top of it.

**Domain:** `vizgrams.com` — confirmed purchased 2026-04-20 (both `.com` and `.io`)

---

## Two tiers

### Public (vizgrams.com)
- Public datasets only (curated by admin)
- Anyone can register as a Creator and publish vizgrams
- Feed is global (same for all viewers)
- No auth required to browse feed
- Monetisation: free tier + future paid features

### Enterprise (self-hosted / cloud)
- Internal private datasets (company DBs, APIs, files)
- SSO via OIDC (Dex — already built)
- Feed is org-scoped
- Apps (parameterised dashboards) as a first-class feature alongside feed
- LLM captions on private data, on-prem option (Ollama / BYOK)
- Paid product

---

## Three user planes

### Plane 1: Ontology Builder (Admin role)
Responsible for: ingest, modeling, ontology definition.

| Component | Description |
|---|---|
| Extractor config | Data source connections, ingestion schedule, run history |
| Mapper editor | Source fields → entity fields (visual or form-based) |
| Ontology browser | Entities, relationships, features — read + write |

**Current state:** 100% CLI/YAML. No UI.
**Plan:** Keep CLI for Phase 1 (admin = you for public version). UI deferred to Phase 2 when Enterprise self-service is needed.

---

### Plane 2: Vizgram Creator (Creator role)
Responsible for: building vizgrams from datasets and publishing them.

| Component | Description |
|---|---|
| Feature Builder | Define computed features on entities (currently YAML-only) |
| Entity Explorer | Browse entities and their underlying data |
| Query Builder | Guided "pick entity → measure → slice/filter" flow |
| View Builder | Parameterise queries for reuse |
| Publish action | Take query result + chart + caption → publish as vizgram |
| Caption editor | LLM draft + user edits before publishing |

**Current state:** ExploreShell covers Entity Explorer + ad-hoc query execution (~60% of technical creator needs). QueriesPage covers saved queries. AppFrame covers parameterised views. No publish action exists.

**Key UX challenge:** Self-serve Query Builder for non-technical creators. First version should be "pick from pre-defined query templates, apply filters" rather than free-form.

---

### Plane 3: User Feed (Viewer role)
Responsible for: consuming published vizgrams as an algorithmic feed.

| Component | Description |
|---|---|
| Feed card | Chart + title + LLM caption + metadata (source, last updated) |
| Feed page | Infinite scroll stream of published vizgrams |
| Ranking algorithm | Freshness × significance × diversity |
| Like / save | Optional v1 — low effort, low risk |
| Comments | Deferred — moderation complexity for public |
| Enterprise Apps | Apps stay as pinned/featured vizgrams for admin-curated dashboards |

**Current state:** Nothing. Entirely new.

---

## Roles

| Role | Public | Enterprise |
|---|---|---|
| Admin | You (application admin) | Company admin |
| Creator | Anyone registered (GitHub OAuth) | Users in Creator role (admin-assigned) |
| Viewer | Anyone (no auth required) | Viewer role (auth required) |

---

## A "vizgram" (atomic content unit)

```
vizgram {
  id
  dataset_ref        // model name
  slice_config       // query/view config (entity, measure, dimensions, filters)
  chart_config       // type, axes, colours
  title              // can include data-driven template values
  caption            // LLM-generated, user-edited, cached until data changes
  live: bool         // true = re-runs on schedule; false = snapshot at publish time
  last_updated       // when underlying data last changed
  significance_score // computed: trend delta, anomaly flag — used for feed ranking
  author_id
  published_at
  tags[]
}
```

Same dataset → many different vizgrams (different slices, different authors). Like the same location producing many different photos.

---

## LLM caption generation

- Generated async when a vizgram is published or data refreshes
- Cached until underlying data changes (hash the result set)
- Input: chart title + data as small JSON table + trend summary (max, min, period-over-period delta)
- Output: 2–3 sentences. "What you're seeing" + "Notable observation"
- Cost control: only regenerate if data hash changes
- Enterprise on-prem: Ollama or user-supplied API key

---

## Feed ranking algorithm (v1)

1. **Recency** — how recently did the underlying data update?
2. **Significance** — % delta vs previous period, or σ from rolling baseline
3. **Diversity** — avoid consecutive cards from same dataset or author
4. No personalisation in v1 — global feed, same for all viewers

---

## Build order

```
Phase 1 — Foundation
  1. Vizgram DB schema (dataset + slice + chart + caption + author)
  2. Role model (admin/creator/viewer) on top of existing Dex/OIDC
  3. Publish action in ExploreShell (Plane 2 → Plane 3 bridge)
  4. LLM caption generation (async on publish, cached)
  5. Feed card component + feed page (Plane 3)
  6. Basic ranking (freshness + significance computed on publish)

Phase 2 — Creator experience
  7. User registration — GitHub OAuth for public version
  8. Self-serve Query Builder (guided, non-technical)
  9. Enterprise feed + Apps unification

Phase 3 — Admin self-service
  10. Ontology Builder UI (Plane 1)
  11. Visual mapper/extractor editor
```

---

## Key risks

| Risk | Mitigation |
|---|---|
| Self-serve query builder UX is hard | Start with template-based query selection, not free-form |
| LLM captions are low quality | Good prompt engineering + easy user editing before publish |
| Feed cold-start (no content on day one) | Admin seeds initial vizgrams; show "explore datasets" when feed is empty |
| Public/Enterprise codebases diverge | Single codebase, feature-flagged by deployment config |
| LLM on private data (Enterprise trust) | On-prem option via Ollama or BYOK Claude/OpenAI key |

---

## What's already built (work_tools2 → vizgrams)

| work_tools2 | vizgrams |
|---|---|
| Semantic layer (`semantic/`, `engine/`) | Core query engine (shared across all planes) |
| Models (`models/*/`) | Datasets (Plane 1 output) |
| ExploreShell | Entity Explorer + proto Query Builder (Plane 2) |
| QueriesPage | Saved queries (Plane 2) |
| AppFrame | Apps → Enterprise feed pinned items (Plane 3) |
| Dex OIDC | Auth foundation for role model |
| LineBarChart, MapChart | Chart rendering (shared) |
| Extractors, Mappers (CLI) | Plane 1 (CLI, UI deferred) |
