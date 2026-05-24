# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""FastAPI application entry point."""

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

try:
    from dotenv import load_dotenv
    # Load .env from the project root (two levels up from this file: api/ → project root).
    # Using an explicit path means the server can be started from any working directory.
    _env_file = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=_env_file)
except ImportError:
    pass

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from batch.logging_config import configure_logging
from batch.tracing import configure_tracing
from core.db import BackendUnavailableError

# ---------------------------------------------------------------------------
# Logging + tracing — initialised before any other imports touch the OTel API
# ---------------------------------------------------------------------------
configure_logging(service="vizgrams-api")
configure_tracing(service="vizgrams-api")

_startup_logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from api.dependencies import get_base_dir
    from core.metadata_db import migrate_from_legacy_db, seed_from_directory
    from core.registry import mark_orphaned_jobs
    from core.vizgrams_db import migrate_email_ids
    base_dir = get_base_dir()
    models_dir_env = os.environ.get("VZ_MODELS_DIR")
    models_dir = Path(models_dir_env) if models_dir_env else base_dir / "models"
    _startup_logger.info("Models directory: %s", models_dir)
    n = mark_orphaned_jobs(models_dir)
    if n:
        _startup_logger.warning(
            "Marked %d orphaned job(s) as failed — server restarted while they were running", n
        )
    migrated = migrate_email_ids()
    if migrated:
        _startup_logger.info("Migrated %d email-based user identities to stable UUIDs", migrated)
    # VG-102: seed central api.db from each model directory — idempotent.
    # 1. Import from legacy scryglass-metadata.db (models without YAML files).
    # 2. Seed from YAML files on disk (canonical source going forward).
    if models_dir.is_dir():
        seeded = 0
        for model_dir in sorted(models_dir.iterdir()):
            if model_dir.is_dir():
                seeded += migrate_from_legacy_db(model_dir)
                seeded += seed_from_directory(model_dir)
        if seeded:
            _startup_logger.info("Seeded %d new artifact version(s) into api.db", seeded)

        # Migrate registry.yaml → models table (idempotent; remove after all deployments migrated).
        from core.vizgrams_db import seed_model_config, seed_model_registry
        reg_seeded = seed_model_registry(models_dir)
        if reg_seeded:
            _startup_logger.info("Migrated %d model(s) from registry.yaml into api.db models table", reg_seeded)

        # Seed tools_config / database_config from config.yaml (VG-142).
        cfg_seeded = seed_model_config(models_dir)
        if cfg_seeded:
            _startup_logger.info("Seeded config for %d model(s) from config.yaml into DB", cfg_seeded)

    # Discover external tools from VZ_TOOLS_DIR (VG-150).
    from core.tool_service import init_system_tools
    init_system_tools()

    # Scan every model for the "exactly one mapper per entity" rule and log a
    # loud warning on any violation. Doesn't crash startup — a broken model
    # shouldn't take the whole API down — but the mapper run will refuse
    # to execute (see batch_service/executor.py) until the model is fixed.
    if models_dir.is_dir():
        from api.services.mapper_service import find_duplicate_target_mappers
        for model_dir in sorted(models_dir.iterdir()):
            if not model_dir.is_dir():
                continue
            try:
                dupes = find_duplicate_target_mappers(model_dir)
            except Exception as exc:
                _startup_logger.debug("Skip mapper-violation scan for %s: %s", model_dir.name, exc)
                continue
            for entity, mappers in dupes.items():
                _startup_logger.warning(
                    "Model %r has %d mappers writing to entity %r: %s. "
                    "Mapper runs will refuse to execute until this is resolved.",
                    model_dir.name, len(mappers), entity, mappers,
                )

    # Wire the embeddings indexer (Epic 20 VG-230). Best-effort: if the
    # provider can't be constructed (no API key) or the CH store can't
    # ensure its schema, log and disable rather than crash startup.
    try:
        from core import metadata_db
        from semantic.llm.embeddings import get_default_provider
        from semantic.llm.embeddings.index import configure as configure_indexer
        from semantic.llm.embeddings.index import index_artifact_async
        from semantic.llm.embeddings.store import EmbeddingsStore

        provider = get_default_provider()
        if provider is None:
            _startup_logger.info(
                "Embeddings disabled — no provider configured (set OPENAI_API_KEY to enable).",
            )
        else:
            store = EmbeddingsStore()
            try:
                store.ensure_schema()
            except Exception as exc:
                _startup_logger.warning(
                    "Embeddings store schema check failed (ClickHouse unavailable?): %s. "
                    "Indexing disabled this session.", exc,
                )
            else:
                configure_indexer(provider=provider, store=store)
                metadata_db.set_index_hook(index_artifact_async)
                _startup_logger.info(
                    "Embeddings indexer wired — provider=%s store=%s.%s",
                    provider.model, store.DATABASE, store.TABLE,
                )

                # Self-heal stale rows in a background thread so we don't
                # block startup on OpenAI / CH availability. Runs once per
                # boot; cheap when nothing is stale (one CH query per model).
                import threading

                from semantic.llm.embeddings.reconcile import reconcile_all_models

                def _do_reconcile():
                    try:
                        report = reconcile_all_models(
                            models_dir, provider=provider, store=store,
                        )
                        if report["total_stale"]:
                            _startup_logger.info(
                                "Embeddings reconcile: %d stale rows across %d model(s); "
                                "%d re-indexed, %d failed.",
                                report["total_stale"],
                                sum(1 for m in report["models"].values() if m["stale"]),
                                report["total_reindexed"],
                                report["total_failed"],
                            )
                    except Exception as exc:  # noqa: BLE001
                        _startup_logger.warning(
                            "Embeddings reconcile failed (non-fatal): %s", exc,
                        )

                threading.Thread(
                    target=_do_reconcile, name="embed-reconcile", daemon=True,
                ).start()
    except Exception as exc:
        _startup_logger.warning("Embeddings setup failed (non-fatal): %s", exc)

    yield

    # Best-effort shutdown for the indexer thread pool.
    try:
        from semantic.llm.embeddings.index import shutdown as shutdown_indexer
        shutdown_indexer()
    except Exception:  # noqa: BLE001
        pass


from api.limiter import limiter
from api.routers import (
    applications,
    batch,
    chat,
    entities,
    explore,
    expression,
    extractors,
    features,
    graph,
    input_data,
    jobs,
    mappers,
    me,
    models,
    queries,
    service_accounts,
    tool_config,
    tools,
    views,
    vizgrams,
)

app = FastAPI(
    title="vizgrams API",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.exception_handler(BackendUnavailableError)
async def _backend_unavailable_handler(request: Request, exc: BackendUnavailableError):
    return JSONResponse(status_code=503, content={"detail": str(exc)})

import os as _os

_allowed_origins = _os.environ.get(
    "ALLOWED_ORIGINS", "http://localhost:5173"
).split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check outside versioned prefix
@app.get("/healthz", tags=["health"])
def healthz():
    return {"status": "ok", "version": "1.0.0"}


# Versioned routers
PREFIX = "/api/v1"

app.include_router(me.router, prefix=PREFIX)
app.include_router(batch.router, prefix=PREFIX)
app.include_router(models.router, prefix=PREFIX)
app.include_router(models.tools_router, prefix=PREFIX)
app.include_router(extractors.router, prefix=PREFIX)
app.include_router(tools.router, prefix=PREFIX)
app.include_router(tool_config.router, prefix=PREFIX)
app.include_router(entities.router, prefix=PREFIX)
app.include_router(mappers.router, prefix=PREFIX)
app.include_router(mappers.crud_router, prefix=PREFIX)
app.include_router(features.router, prefix=PREFIX)
app.include_router(features.reconcile_router, prefix=PREFIX)
app.include_router(features.model_feature_router, prefix=PREFIX)
app.include_router(expression.router, prefix=PREFIX)
app.include_router(queries.router, prefix=PREFIX)
app.include_router(views.router, prefix=PREFIX)
app.include_router(applications.router, prefix=PREFIX)
app.include_router(graph.router, prefix=PREFIX)
app.include_router(explore.router, prefix=PREFIX)
app.include_router(chat.router, prefix=PREFIX)
app.include_router(jobs.router, prefix=PREFIX)
app.include_router(input_data.router, prefix=PREFIX)
app.include_router(vizgrams.router, prefix=PREFIX)
app.include_router(service_accounts.router, prefix=PREFIX)

# ---------------------------------------------------------------------------
# OTel instrumentation — must run after app + routers are fully configured
# ---------------------------------------------------------------------------
import os as _otel_os

if _otel_os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()  # propagates trace context to batch service
