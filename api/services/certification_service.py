# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Certification API helpers (VG-258 / VG-259).

Thin wrappers that build the certification fields included on view, query,
and feature responses. The DB layer (``core/metadata_db``) stays pure; this
module is where we resolve the ``certified_by`` UUID to a display name by
joining against the ``users`` table in ``vizgrams_db``.
"""

from __future__ import annotations

from pathlib import Path

from core import metadata_db
from core.vizgrams_db import get_user_display_name


def cert_payload(cert: dict | None) -> dict:
    """Build the four-field certification payload from a raw cert row.

    Returns the "uncertified" shape when ``cert`` is None so callers can
    spread ``**cert_payload(...)`` into responses without conditionals.
    """
    if cert is None:
        return {
            "is_certified": False,
            "certified_by": None,
            "certified_by_display": None,
            "certified_at": None,
        }
    user_id = cert.get("certified_by")
    display = get_user_display_name(user_id) if user_id else None
    return {
        "is_certified": True,
        "certified_by": user_id,
        "certified_by_display": display,
        "certified_at": cert.get("certified_at"),
    }


def get_cert_payload(model_dir: Path, artifact_type: str, name: str) -> dict:
    """Single-artifact lookup. Use for get_view / get_query / get_feature."""
    return cert_payload(metadata_db.get_certification(model_dir, artifact_type, name))


def list_cert_payloads(
    model_dir: Path, artifact_type: str,
) -> dict[str, dict]:
    """Batched lookup keyed by ``name`` for an artifact type.

    Use in list_views / list_queries / list_features so we don't hit the
    DB once per row.
    """
    rows = metadata_db.list_certifications(model_dir, artifact_type)
    return {name: cert_payload(row) for (t, name), row in rows.items() if t == artifact_type}
