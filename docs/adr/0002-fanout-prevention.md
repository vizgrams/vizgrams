# ADR-0002 — Preventing mapper fan-out at source

**Status:** proposed (2026-06-20)

## Context

`FanOutError` keeps recurring in production. Recent incidents:

| Date | Symptom | Root cause |
|---|---|---|
| 2026-06-20 | iagai Sprint mapper | `jira_sprint_reports` accumulated 1,200 dupes from extractor runs against a PK-less table (PRs #148, #151) |
| 2026-06-20 | default Country mapper | `__feature_value` had 707k dupes; airline mapper fanned PR counts 27× via 3 LEFT JOINs (PR #151) |
| 2026-06-20 | prod default airline mapper | 4 rows for iata_code `OE` in the raw OpenFlights data — three real-world airlines share that code, two of which are byte-identical duplicates |
| 2026-06-20 | prod default earthquake mapper | hourly `APPEND`-mode extractor accumulated 29 copies of every quake in the lookback window |

The current `_detect_fan_out` check (`engine/mapper.py`) walks the grain rows after the source SELECT and fails the job the moment it sees the same grain tuple twice. That's the right *last line of defence* but it surfaces too late:

- It runs after the SQL has already executed, so the user sees the cryptic grain-tuple error rather than something pointing at *why* the source has dupes.
- It treats byte-identical dupes (a maintenance nuisance) the same as conflicting dupes (a real data quality problem), even though the former can be safely collapsed.
- There is no proactive check — a malformed schema or accumulating dupes can sit in the database for weeks until a mapper run trips on them.

## Decision

A two-layer strategy: **collapse what's safe, alert on what isn't, prevent it from coming back.**

### Layer 1 — `engine/mapper.py`: classify fan-out and auto-collapse byte-identical rows

Replace the single-pass detector with two passes:

1. **Group by grain tuple.** For each grain-tuple, collect all candidate rows that produced it.
2. **For each group of >1 rows:**
   - If every row is **byte-identical** across *all* selected columns (not just grain): silently drop the dupes and log INFO `fan-out collapsed N→1 for grain (...)`. This is the OpenFlights "WestAir Airlines" case — the source has two identical rows and neither carries any information the other doesn't.
   - If rows differ on any non-grain column: this is a **conflicting** fan-out. Fail with the existing `FanOutError`, but include the differing columns in the message so the user can see *which* fields disagree and decide whether to dedupe upstream or change the grain.
3. Report counts in the job's `result` payload: `{records_written, fanout_collapsed, fanout_conflicting}`. The UI surfaces collapsed counts as a warning rather than a failure.

**Why this is safe.** Byte-identical fan-out is a no-op semantically — picking any row gives the same outcome. The only reason to fail today is that the detector is paranoid. Genuine ambiguity (different fields) still hard-fails so we never silently lose distinct data.

### Layer 2 — `core/db.py`: schema invariant healthcheck

Every `DBBackend` already knows the table's actual PK constraint (`_get_primary_keys`). Surface that against the *declared* schema:

- **At startup (api + batch):** for every entity in the ontology, verify the sem table's PK matches `entity.primary_key.name`. For every extractor output with `primary_keys`, verify the raw table's PK matches. Log warnings at WARN; expose as a structured list at `GET /api/v1/healthz/schema`.
- **In dev (`make dev`):** the api refuses to start if any PK mismatch exists — this is the case that bit us during the CH→DuckDB migration, where the arrow round-trip dropped PK constraints and nobody noticed until a mapper failed weeks later.
- **In prod:** WARN-only by default; turn on hard-fail via env var when an operator wants stricter posture.

The healthcheck doubles as **the right place to surface accumulating-dupe risk**: it can count `rows vs distinct(pk)` for every table with a declared PK and flag tables where the ratio is > 1.01 (configurable). PR #148/#151 fixed the *write* paths so this can't accumulate further on tables with a constraint — but the healthcheck still catches the case where the constraint was never applied in the first place (the CH→DuckDB scar).

### Layer 3 — `tools/dedup_duckdb_raw_tables.py` becomes idempotent + scheduled

The dedup tool already exists and handles raw + sem + meta tables. Two small follow-ups make it a routine maintenance task rather than an emergency runbook:

- Add a `--check-only` mode that exits non-zero if anything would be rebuilt. Wire into `make check-data` and into the prod monitoring panel as a daily cron.
- Fix the cosmetic bug where skipped rows report `dupes_removed = rows_before` in the summary line.

## Trade-offs

**Why not push dedup all the way upstream into the extractor YAML?** Some sources (OpenFlights iata_code) have real-world ambiguity that's a data-modelling decision, not a hygiene one. Forcing dedup at extract time hides that ambiguity from the mapper — which is the layer with enough context (entity definition + grain choice) to decide whether collapsing is the right call.

**Why not just disable fan-out detection?** Conflicting fan-out is a real correctness issue — without it, the bulk_upsert downstream would pick a row non-deterministically and the entity table would silently flip-flop between runs.

**What this doesn't solve.** Cross-table fan-out from LEFT JOINs to a duplicated sem/meta table is still a real failure mode — but PRs #148/#151 closed the *cause* of that (PK-less writes accumulating), and the Layer 2 healthcheck catches the lingering schema drift.

## Implementation plan

1. **Engine PR:** new `_detect_fan_out` returning a (collapsed_count, conflicting_count, error_message) tuple. Update the two call sites in `run_mapper` to handle the warning case. ~80 LOC + 4 unit tests.
2. **Healthcheck PR:** new `core/schema_invariants.py` with `check_model_schema(model_dir) -> list[Invariant]` plus a `/api/v1/healthz/schema` route. ~150 LOC + tests + Helm probe wiring.
3. **Tooling PR:** `--check-only` flag on the dedup tool + summary bug fix. ~20 LOC.

Sequenced so each PR is independently mergeable. Layer 1 alone fixes the OpenFlights case without any operator action; layer 2 alone catches future migrations dropping constraints.

## Status updates

- Proposed: 2026-06-20
- Accepted: TBD
- Implemented: TBD
