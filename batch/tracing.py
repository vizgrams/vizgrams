# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""OpenTelemetry tracing configuration for the API server and batch service.

Call ``configure_tracing()`` once at process startup, before the FastAPI app
is instrumented.  When ``OTEL_EXPORTER_OTLP_ENDPOINT`` is not set the function
is a no-op, so local ``make dev`` requires no extra services.

Traces are exported via OTLP/HTTP.  Point to any compatible backend:

  Jaeger (local):   OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
  Grafana Tempo:    OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4318
  Honeycomb:        OTEL_EXPORTER_OTLP_ENDPOINT=https://api.honeycomb.io

The service name embedded in every span is the ``service`` argument, which
should match the value passed to ``configure_logging`` so log and trace records
can be correlated by service name in your observability backend.
"""

from __future__ import annotations

import logging
import os

_log = logging.getLogger(__name__)


def configure_tracing(service: str) -> None:
    """Set up the OTel SDK and OTLP exporter.

    No-op when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is unset.

    Args:
        service: Value for the ``service.name`` resource attribute on every span.
    """
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create({"service.name": service})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=endpoint.rstrip("/") + "/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _log.info("OTel tracing configured: service=%s endpoint=%s", service, endpoint)
