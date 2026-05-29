# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the unified chart upsert endpoint (PUT /chart/{name})."""

from api.main import app


def test_chart_route_registered():
    """The /chart router must be wired into the FastAPI app.

    Full upsert integration is covered by tests/test_api_views.py +
    tests/test_api_queries.py since this endpoint just orchestrates the
    same service-layer calls. We just need to confirm the wiring.
    """
    paths = [r.path for r in app.routes if hasattr(r, "path")]
    assert "/api/v1/model/{model}/chart/{chart}" in paths
