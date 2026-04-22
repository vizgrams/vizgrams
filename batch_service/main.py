# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""vizgrams-batch FastAPI application.

Runs on port 8001 (configurable via BATCH_SERVICE_PORT).
The main API (vizgrams-api, port 8000) proxies all job and schedule
operations to this service via BatchClient.

Start locally::

    poetry run uvicorn batch_service.main:app --port 8001 --reload

Or use the helper script::

    ./start_batch_service.sh
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

try:
    from dotenv import load_dotenv
    _env_file = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=_env_file)
except ImportError:
    pass

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from batch.logging_config import configure_logging
from batch.tracing import configure_tracing

configure_logging(service="vizgrams-batch")
configure_tracing(service="vizgrams-batch")
_log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from datetime import UTC, datetime

    from batch_service import db as jobdb
    from batch_service.config import get_models_dir
    from batch_service.scheduler import start_scheduler
    from core.registry import load_registry

    models_dir = get_models_dir()
    _log.info("Batch service starting. Models dir: %s", models_dir)

    # Clean up jobs that were running when the process last died
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        registry = load_registry(models_dir)
        total_orphaned = 0
        for model_name in registry:
            model_dir = models_dir / model_name
            if model_dir.is_dir():
                n = jobdb.mark_orphaned_jobs(model_dir, now)
                if n:
                    _log.warning(
                        "Marked %d orphaned job(s) as failed for model %s",
                        n, model_name,
                    )
                    total_orphaned += n
        if total_orphaned:
            _log.warning("Total orphaned jobs cleaned up: %d", total_orphaned)
    except Exception:
        _log.exception("Error during orphaned job cleanup")

    # Start background scheduler
    start_scheduler(models_dir)
    _log.info("Scheduler started")

    yield

    _log.info("Batch service shutting down")


from batch_service.routers import jobs, schedules

app = FastAPI(
    title="vizgrams-batch",
    version="1.0.0",
    description="Batch extraction scheduler and executor microservice.",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

try:
    from prometheus_fastapi_instrumentator import Instrumentator
    Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)
except ImportError:
    pass

import os as _otel_os

if _otel_os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    FastAPIInstrumentor.instrument_app(app)

_allowed_origins = os.environ.get(
    "ALLOWED_ORIGINS", "http://localhost:5173,http://localhost:8000"
).split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_BATCH_SECRET = os.environ.get("BATCH_SERVICE_SECRET")


@app.middleware("http")
async def require_batch_secret(request, call_next):
    """Reject requests that don't carry the shared service secret.

    Only active when BATCH_SERVICE_SECRET is set.  The /healthz endpoint is
    always exempt so container health checks continue to work without the
    secret.
    """
    secret_required = _BATCH_SECRET and request.url.path not in {"/healthz", "/metrics"}
    if secret_required and request.headers.get("X-Batch-Secret") != _BATCH_SECRET:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)


@app.get("/healthz", tags=["health"])
def healthz():
    return {"status": "ok", "service": "vizgrams-batch", "version": "1.0.0"}


app.include_router(jobs.router)
app.include_router(schedules.router)
