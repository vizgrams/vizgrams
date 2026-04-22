# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Structured JSON logging configuration for the API server and batch runner.

Call ``configure_logging()`` once at process startup.  All subsequent
``logging.getLogger(__name__)`` calls throughout the codebase emit in the
configured format automatically — no per-module changes needed.

Format is controlled by the ``LOG_FORMAT`` environment variable:

  ``json`` (default) — JSON Lines to stdout.  One object per line, compatible
      with fluentd (``@type json``), Datadog, Google Cloud Logging, and Elastic.
  ``text`` — human-readable, useful for local development.

Log level is controlled by ``LOG_LEVEL`` (default ``INFO``).

File output is opt-in via ``VZ_LOG_DIR``.  In production (k8s) leave it unset
and rely on stdout capture; in development set it to ``logs/`` to get a
rotating file alongside stdout.

Example log line (JSON format)::

    {"timestamp": "2026-03-30T10:00:00.123Z", "level": "INFO",
     "logger": "batch.runner", "message": "Extractor completed",
     "service": "vizgrams-batch", "model": "mymodel",
     "extractor": "jira", "job_id": "01JXYZ...", "duration_ms": 4312}
"""

import logging
import logging.handlers
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from pythonjsonlogger.json import JsonFormatter


class _ISOJsonFormatter(JsonFormatter):
    """JsonFormatter that emits ISO 8601 UTC timestamps and renames noisy fields.

    Standard renames applied to every record:
      levelname → level
      name      → logger
      asctime   → timestamp
    """

    _RENAME = {"levelname": "level", "name": "logger", "asctime": "timestamp"}

    def __init__(self, service: str, **kwargs):
        fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
        super().__init__(fmt=fmt, rename_fields=self._RENAME, **kwargs)
        self._service = service

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Return an ISO 8601 UTC timestamp with millisecond precision."""
        dt = datetime.fromtimestamp(record.created, tz=UTC)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(record.msecs):03d}Z"

    def add_fields(
        self,
        log_record: dict,
        record: logging.LogRecord,
        message_dict: dict,
    ) -> None:
        super().add_fields(log_record, record, message_dict)
        # Inject service name into every record so downstream filters can route
        # by service without parsing the logger name.
        log_record["service"] = self._service


def configure_logging(
    service: str = "vizgrams",
    level: str | None = None,
) -> None:
    """Configure the root logger.  Call once at process startup.

    Args:
        service: Identifies this process in the ``service`` field of every log
            line.  Use distinct values for the API server and batch runner so
            log routers can split them into separate streams.
        level: Override log level (DEBUG/INFO/WARNING/ERROR).  Falls back to
            the ``LOG_LEVEL`` environment variable, then INFO.
    """
    log_level_name = level or os.environ.get("LOG_LEVEL", "INFO")
    log_level = getattr(logging, log_level_name.upper(), logging.INFO)

    fmt = os.environ.get("LOG_FORMAT", "json").lower()

    if fmt == "json":
        formatter: logging.Formatter = _ISOJsonFormatter(service=service)
    else:
        formatter = logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)-35s  %(message)s"
        )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(log_level)
    handlers: list[logging.Handler] = [stdout_handler]

    log_dir_env = os.environ.get("VZ_LOG_DIR")
    if log_dir_env:
        log_dir = Path(log_dir_env)
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir / f"{service}.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        handlers.append(file_handler)

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()
    for handler in handlers:
        root.addHandler(handler)

    # Redirect uvicorn's private loggers into our pipeline.
    # By default uvicorn installs its own text-format handlers on "uvicorn" and
    # "uvicorn.access" with propagate=False, sending startup/error lines to
    # stderr and access lines to stdout (plain text).  Clearing those handlers
    # and enabling propagation means all uvicorn records flow through the root
    # logger above — JSON to stdout — so fluentd sees a uniform stream.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv = logging.getLogger(name)
        uv.handlers.clear()
        uv.propagate = True
