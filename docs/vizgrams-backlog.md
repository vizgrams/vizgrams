# Vizgrams — Backlog

> Ticket format: `VG-NNN | Title | Status`
> Statuses: `todo` · `in-progress` · `done`
> See `docs/vizgrams-spec.md` for full product spec.

---

## Epic 1 — Foundation

| ID | Title | Status |
|---|---|---|
| VG-001 | Vizgram DB schema (dataset, slice_config, chart_config, caption, author, timestamps, significance_score) | done |
| VG-002 | Add Creator system-level role — `VZ_CREATORS` env var + `require_creator` dependency (mirrors existing `is_system_admin` pattern, ~20 lines) | done |
| VG-003 | Add `optional_user` dependency variant — returns `None` instead of 401 for unauthenticated requests (needed for public feed endpoints) | done |
| VG-004 | Project rename: work_tools2 → vizgrams (package names, docker image tags, API title, README) | done |
| VG-004a | Rename `WT_` env var prefix → `VZ_` across all env vars (`VZ_BASE_DIR`, `VZ_MODELS_DIR`, `VZ_SYSTEM_ADMINS`, `VZ_CREATORS`, etc.) — breaking change, requires `.env` migration note in release notes; do before public launch | done |
| VG-005 | Stable user identity — add `users` table `(id UUID, provider TEXT, external_id TEXT, email TEXT, display_name TEXT)`; on first auth resolve or create via `(provider, external_id)`; use internal UUID everywhere instead of email. Migrate existing `author_id` / `user_id` fields. oauth2-proxy: forward `X-Auth-Request-User` as sub; Entra ID sub = OID (stable UUID); GitHub sub = numeric user ID. Do before public launch. | done |

---

## Epic 2 — Publish Flow (Plane 2 → Plane 3 bridge)

| ID | Title | Status |
|---|---|---|
| VG-010 | "Publish as vizgram" action in ExploreShell (saves slice + chart config to DB) | done |
| VG-011 | Async LLM caption generation on publish (Claude API, cached by data hash) | done |
| VG-012 | Caption editor UI (show LLM draft, allow user edits before publishing) | done |
| VG-013 | Significance score computation on publish (period-over-period delta, σ from baseline) | done |
| VG-014 | Live vs static flag — live vizgrams re-run query on schedule, static are snapshots | todo |

---

## Epic 3 — Feed UI (Plane 3)

| ID | Title | Status |
|---|---|---|
| VG-020 | Feed card component (chart + title + caption + metadata: source, last updated, author) | done |
| VG-021 | Feed page (infinite scroll stream of published vizgrams) | done |
| VG-022 | Feed ranking algorithm v1 (freshness × significance × diversity) | done |
| VG-023 | Like / save actions on feed cards | done |
| VG-024 | Empty-feed state ("explore datasets" CTA for cold-start) | done |
| VG-025 | Seed initial vizgrams as admin (public version day-one content) | todo |
| VG-026 | Saved vizgrams page — `/saved` route showing cards the viewer has bookmarked; reuses feed infrastructure with a saved-filter on engagements | done |

---

## Epic 4 — User Accounts (Public version)

> Auth strategy: Dex as the OIDC hub (already in stack for local dev) federates multiple upstream providers into a single OIDC endpoint for oauth2-proxy. Self-hosters configure their own connectors; the hosted deployment uses Google + Apple.
> Apple Sign In is mandatory for iOS App Store apps that offer any third-party OAuth.

| ID | Title | Status |
|---|---|---|
| VG-030 | Google OAuth + Sign in with Apple — configure Dex production connectors; make Dex production-ready (persistent SQLite store, configurable issuer URL); update docker-compose for prod Dex | done |
| VG-031 | Creator role self-service (register → auto-granted Creator role on public tier) | done |
| VG-032 | Public user profile page (vizgrams authored, follower count) | todo |
| VG-033 | Migrate auth layer from Dex to a managed service (Auth0, Clerk, or WorkOS) — swap `OIDC_ISSUER_URL` in oauth2-proxy config; migrate `users` table `(provider, external_id)` rows to new provider's user IDs; remove Dex from compose stacks. Trigger: when MFA, org-level SSO, or compliance requirements (SOC 2) make self-operating auth infrastructure impractical. | todo |

---

## Epic 5 — Self-Serve Query Builder (Plane 2, non-technical creators)

| ID | Title | Status |
|---|---|---|
| VG-040 | Query template library (pre-defined queries users can slice via UI) | todo |
| VG-041 | Guided query builder UI (pick entity → measure → dimension/filter, no SQL exposed) | todo |
| VG-042 | View Builder — parameterise queries for reuse | todo |

---

## Epic 6 — Mobile App (iOS + Android)

> Stack: React Native + Expo. Feed consumption is v1 scope. Creator flow is v2.

| ID | Title | Status |
|---|---|---|
| VG-050 | Expo project scaffold (TypeScript, shared design tokens with web UI) | todo |
| VG-051 | Feed card component (native, reuses same API as web) | todo |
| VG-052 | Feed screen (infinite scroll, pull-to-refresh) | todo |
| VG-053 | Vizgram detail screen (full chart + caption + metadata) | todo |
| VG-054 | Like / save in mobile feed | todo |
| VG-055 | GitHub OAuth login on mobile (Expo AuthSession) | todo |
| VG-056 | Push notifications (new vizgrams from followed datasets / authors) | todo |
| VG-057 | App Store + Play Store submission | todo |

---

## Epic 7 — AWS Cloud Deployment

> **All work in this epic lives in the private `vizgrams/ops` repo**, not this monorepo.
> Deployment scripts, ClickHouse config, and prod CI/CD are bespoke operational infrastructure —
> kept private to separate open-source application code from our specific deployment.
>
> Strategy: single prod EC2 running Docker Compose; local Docker Compose serves as dev environment (no cloud dev instance).
> ClickHouse self-hosted in Docker on the same EC2 — cheapest option; EBS snapshots provide backup.
> S3 + CloudFront for UI static files. Scale to ECS / dedicated ClickHouse when load warrants.
> Dev = `make up-auth` locally. No separate cloud dev environment until staging is needed.
>
> Repo layout (`vizgrams/ops`):
>   `deploy/`      — EC2 provisioning + deploy scripts
>   `clickhouse/`  — CH init SQL, schema, model config
>   `.github/workflows/deploy-prod.yml` — triggered after public repo pushes to ECR

| ID | Title | Status |
|---|---|---|
| VG-060 | AWS account setup (IAM user + MFA, billing alerts at $50/$100, cost explorer) | todo |
| VG-061 | ECR repositories (api, batch-service, ui) | todo |
| VG-062 | EC2 t3.medium — prod, EBS 30GB gp3, Elastic IP, Docker Compose + self-hosted ClickHouse | todo |
| VG-063 | S3 bucket + CloudFront distribution for UI (`vizgrams.com`) | todo |
| VG-064 | SSM Parameter Store for secrets (ClickHouse password, API keys, Dex config, batch secret) | todo |
| VG-065 | Route53 hosted zone + DNS — apex A record → Elastic IP; `api.vizgrams.com` A record → Elastic IP | todo |
| VG-066 | ACM TLS certificate (apex, `www`, `api` subdomains) | todo |
| VG-067 | GitHub Actions CI/CD — build images → push ECR → SSH deploy to EC2; manual approval gate on `main` | todo |
| VG-073 | EBS snapshot schedule — AWS DLM daily snapshot, 7-day retention (covers SQLite + ClickHouse data) | todo |
| VG-074 | CloudWatch alarms — CPU >80%, disk >75%, API 5xx error rate (CloudWatch agent for memory + disk metrics) | todo |

### Estimated monthly cost
| Resource | Cost |
|---|---|
| EC2 t3.medium | ~$33 |
| EBS 30GB gp3 | ~$2.40 |
| EBS snapshots (7-day retention, ~30GB incremental) | ~$1 |
| S3 + CloudFront (UI) | ~$1 |
| Route53 hosted zone | ~$0.50 |
| ECR storage | ~$0.50 |
| Elastic IP (attached, running) | free |
| ClickHouse (self-hosted in Docker) | included in EC2 |
| **Total** | **~$38/mo** |

> Upgrade path: move to t3.large (~$66/mo) or dedicated ClickHouse instance when query load demands it.
> Add Postgres RDS only when concurrent SQLite write contention becomes measurable.
> ClickHouse Cloud Basic tier ($67/mo minimum) makes sense once operational burden outweighs cost savings.

---

## Epic 8 — Enterprise Features

| ID | Title | Status |
|---|---|---|
| VG-080 | Enterprise feed (org-scoped, same feed mechanics, different data) | todo |
| VG-081 | Apps as pinned/featured vizgrams (admin-curated dashboard items in feed) | todo |
| VG-082 | On-prem LLM option for captions (Ollama integration or BYOK API key) | todo |
| VG-083 | Multi-tenant deployment guide (separate model dirs per org) | todo |

---

## Epic 11 — Navigation & Role-Based Routing

> Restructure the sidebar and routes to reflect the three user planes.
> Current sections ("Build / Runtime / Explore") are engineer-oriented and don't
> map to the Admin / Creator / User distinction.
>
> Target nav layout:
>   Admin    — Extractors, Mappers, Ontology, Jobs (gated: system admin only)
>   Creator  — Features, Query Builder, View Builder, Graph (gated: creator+)
>   User     — Feed, Apps, Entity Explorer (open to all)

| ID | Title | Status |
|---|---|---|
| VG-110 | Expose `role` field on `/api/v1/me` response — return `'admin' \| 'creator' \| 'viewer'` derived from `is_system_admin` / `is_creator` for the current user | done |
| VG-111 | Add `useRole()` hook in UI — fetches role from `/api/v1/me`, memoised, shared via context | done |
| VG-112 | Restructure `Layout.tsx` sidebar sections → **Admin** (Extractors, Mappers, Ontology, Jobs) / **Creator** (Features, Query Builder, View Builder, Graph) / **User** (Feed, Apps, Entity Explorer) | done |
| VG-113 | Role-based nav visibility — hide Admin section entirely for non-admins; hide Creator section for viewers; no redirect, just not shown | done |
| VG-114 | Role-gated route guards in `App.tsx` — return 403 page if a viewer navigates directly to an Admin/Creator URL | done |
| VG-115 | Add `/feed` route placeholder — empty page wired into nav under User section, ready for Epic 3 | done |

---

## Epic 10 — DB Consolidation

> Replace N per-model SQLite metadata DBs with two service-level DBs.
> Bounded context: API service owns configuration + content; batch service owns execution history.
> The `artifact_versions` table already carries `model_id` — schema needs no change, only path resolution.

### api.db  (`{VZ_BASE_DIR}/data/api.db`)
Contains: artifact versions (all models), vizgrams, vizgram_engagements, users (future).

| ID | Title | Status |
|---|---|---|
| VG-100 | Update `metadata_db.py` path resolution — read `API_DB_PATH` env var, default to `{VZ_BASE_DIR}/data/api.db`; remove per-model `_db_path` logic | done |
| VG-101 | Merge `vizgrams_db.py` tables into `api.db` — single `_connect` / path helper shared across both modules; remove `VG_DB_PATH` env var | done |
| VG-102 | Migration script — iterate all existing model dirs, `seed_from_directory` into new central api.db; safe to re-run (content-hash deduplication already in place) | done |
| VG-103 | Update all tests that construct per-model DB paths to use `tmp_path / "api.db"` | done |
| VG-104 | Remove per-model `scryglass-metadata.db` files post-migration (add to `.gitignore`, document cleanup step) | done |

### batch.db  (`{VZ_BASE_DIR}/data/batch.db`)
Contains: jobs, pipeline runs, audit events.

| ID | Title | Status |
|---|---|---|
| VG-105 | Persist `JobService` state to `batch.db` — replace in-memory job dict with SQLite-backed store; schema: `jobs(id, type, status, model, created_at, updated_at, error)` | done |
| VG-106 | Pipeline run log table — record each extractor/mapper run: `pipeline_runs(id, job_id, model_id, stage, started_at, finished_at, rows_affected, status, error)` | done |
| VG-107 | Migrate `core/registry.py` audit events into `batch.db` — `audit_events(id, model_id, event, detail, actor, created_at)` | done |
| VG-108 | Update batch service to read `BATCH_DB_PATH` env var, default to `{VZ_BASE_DIR}/data/batch.db` | done |

---

## Epic 9 — Ontology Builder UI (Plane 1, deferred)

| ID | Title | Status |
|---|---|---|
| VG-090 | Extractor config UI (data source connections, schedule, run history) | todo |
| VG-091 | Mapper editor UI (visual source → entity field mapping) | todo |
| VG-092 | Ontology browser (entities, relationships, features — read + write) | todo |

---

## Epic 12 — Open Source

> Publish vizgrams as an open-source project under the vizgrams GitHub org.
>
> **Repo strategy: monorepo.** API, batch service, UI, core, docs, and example model all live in `vizgrams/vizgrams`. A single PR covers full-stack changes; splitting into separate repos adds coordination overhead with no current benefit. Revisit if a separate mobile team takes over the UI.

| ID | Title | Status |
|---|---|---|
| VG-120 | Create `vizgrams/vizgrams` monorepo (API + batch service + UI + core + docs) in the vizgrams GitHub org; start private, flip public after VG-124 CI is green | done |
| VG-121 | Audit codebase for secrets, internal references, and internal-only config before first public push | done |
| VG-122 | Add LICENSE (Apache-2.0 already in file headers), CONTRIBUTING.md, and public README | done |
| VG-123 | Ensure no customer model data is included in the public repo (models live in external `VZ_MODELS_DIR`) | done |
| VG-124 | Set up GitHub Actions CI on public repo (lint, test, docker build) | done |

---

## Epic 13 — Model Sync (Metadata as Code)

> GitOps workflow for enterprise customers: store model YAML files in their own
> git repo and sync to Vizgrams via CI/CD.  The model repo is the source of
> truth; Vizgrams is the runtime.  Changes flow one way: repo → Vizgrams.
>
> Canonical model repo layout mirrors the Vizgrams directory structure:
>   `ontology/`, `features/`, `queries/`, `views/`, `mappers/`,
>   `extractors/`, `applications/`
>
> The `models/example` directory in this repo serves as a reference
> implementation of the expected layout.

### Auth — Service Accounts (blocker for all sync work)

| ID | Title | Status |
|---|---|---|
| VG-130 | Service account DB schema — `service_accounts(id UUID, model_id TEXT, name TEXT, token_hash TEXT, created_by TEXT, created_at TEXT, last_used_at TEXT, is_active INTEGER)`; stored in `api.db`; token is a random 32-byte secret shown once on creation | todo |
| VG-131 | `require_service_account` FastAPI dependency — validates `X-API-Key` header against hashed tokens; resolves the model scope the token is authorised for; returns 401 if absent/invalid, 403 if wrong model | todo |
| VG-132 | Service account management endpoints (admin-only) — `POST /api/v1/model/{model}/service-accounts` (create, returns plaintext token once), `GET /api/v1/model/{model}/service-accounts` (list, no tokens), `DELETE /api/v1/model/{model}/service-accounts/{id}` (revoke) | todo |
| VG-133 | Accept `X-API-Key` as an alternative auth path on all artifact upsert/read endpoints — service accounts bypass oauth2-proxy for their scoped model only; all other endpoints remain OIDC-only | todo |

### Sync Tooling

| ID | Title | Status |
|---|---|---|
| VG-134 | `vzctl sync` script (`tools/vzctl.py`) — walks a model directory, PUTs each artifact to the API; reads `VZ_API_URL` + `VZ_API_KEY` from env or flags; reports created / updated / unchanged / failed per artifact; exits non-zero on any failure | todo |
| VG-135 | `--prune` flag for full-sync semantics — deletes artifacts present in Vizgrams but absent from the local directory; default off (append-only); requires explicit opt-in to prevent accidental deletion | todo |
| VG-136 | GitHub Actions reusable workflow template (`tools/sync-workflow.yml`) — triggers on push to paths matching model YAML dirs; calls `vzctl sync`; uses `VZ_API_URL` + `VZ_API_KEY` repository secrets; customers copy and configure | todo |
| VG-137 | Sync docs — README section explaining the model-as-code pattern, how to generate a service account token, and how to wire up the GitHub Action | todo |

