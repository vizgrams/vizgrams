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
    yield


from api.limiter import limiter
from api.routers import (
    applications,
    batch,
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
app.include_router(jobs.router, prefix=PREFIX)
app.include_router(input_data.router, prefix=PREFIX)
app.include_router(vizgrams.router, prefix=PREFIX)

# ---------------------------------------------------------------------------
# OTel instrumentation — must run after app + routers are fully configured
# ---------------------------------------------------------------------------
import os as _otel_os

if _otel_os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()  # propagates trace context to batch service
