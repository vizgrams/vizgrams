# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Role-based access control for model-level permissions.

Two tiers of access:

  System-level  — controlled via environment variables.  Three platform roles:

    System Admin — ``VZ_SYSTEM_ADMINS`` (comma-separated emails / domain
                   wildcards).  Full platform access.  ``DEV_USER`` is always
                   treated as a system admin so local development is
                   frictionless.  System admins implicitly satisfy all lower
                   roles (Creator, Viewer).

    Creator      — ``VZ_CREATORS`` (same format as VZ_SYSTEM_ADMINS).  Can
                   publish vizgrams and use the query/view builder.  Cannot
                   manage model ontology or platform config.

    Viewer       — any authenticated user not in the above.  Can browse the
                   feed and run queries; cannot publish.

  Model-level   — controlled via the ``access:`` block in each model's
                  ``config.yaml``.  Entries are evaluated in order; the first
                  match wins.  Supports exact email, domain wildcard
                  (``*@domain.com``), and catch-all (``*``).

Role hierarchy (higher includes all permissions of lower):

  VIEWER    — view model, run queries, run Applications, view job history
  OPERATOR  — VIEWER + trigger extractions, cancel jobs
  ADMIN     — OPERATOR + edit extractors, tool config, model metadata

When a model has no ``access:`` block, all authenticated users get ADMIN
access (open by default — opt in to restrictions by adding the block).

When a user matches no entry in the access list, they have no access and the
model is hidden from their model listing.
"""

from __future__ import annotations

import os
from enum import IntEnum
from pathlib import Path


class ModelRole(IntEnum):
    VIEWER = 1
    OPERATOR = 2
    ADMIN = 3


def _matches(email: str, pattern: str) -> bool:
    """Return True if *email* matches *pattern*.

    Patterns:
      ``*``            — matches everyone
      ``*@domain.com`` — matches any address at that domain
      ``user@x.com``   — exact match (case-insensitive)
    """
    if pattern == "*":
        return True
    if pattern.startswith("*@"):
        return email.lower().endswith(pattern[1:].lower())
    return email.lower() == pattern.lower()


def is_system_admin(email: str) -> bool:
    """Return True if *email* is a system administrator.

    Checked against:
    - ``DEV_USER`` env var (local dev bypass — never set in production)
    - ``VZ_SYSTEM_ADMINS`` env var: comma-separated emails or domain wildcards
      e.g. ``oliver@example.com,*@example.com``
    """
    if os.environ.get("DEV_USER") == email:
        return True
    raw = os.environ.get("VZ_SYSTEM_ADMINS", "")
    patterns = [p.strip() for p in raw.split(",") if p.strip()]
    return any(_matches(email, p) for p in patterns)


def is_creator(email: str) -> bool:
    """Return True if *email* has Creator platform role or higher.

    System admins implicitly satisfy Creator.  Checked against:
    - ``VZ_SYSTEM_ADMINS`` / ``DEV_USER`` (system admin implies creator)
    - ``VZ_CREATORS`` env var: comma-separated emails or domain wildcards
      e.g. ``alice@example.com,*@startup.io``
    """
    if is_system_admin(email):
        return True
    raw = os.environ.get("VZ_CREATORS", "")
    patterns = [p.strip() for p in raw.split(",") if p.strip()]
    return any(_matches(email, p) for p in patterns)


def get_model_role(model_dir: Path, email: str) -> ModelRole | None:
    """Return the user's role for *model_dir*, or ``None`` for no access.

    Resolution order:
    1. System admins → ADMIN unconditionally.
    2. No ``config.yaml`` or no ``access:`` block → ADMIN (open by default).
    3. First matching entry in the ``access:`` list → that role.
    4. No match → ``None`` (model hidden, all requests rejected with 403).
    """
    if is_system_admin(email):
        return ModelRole.ADMIN

    config_path = Path(model_dir) / "config.yaml"
    if not config_path.exists():
        return ModelRole.ADMIN

    from core.model_config import load_config_yaml
    data = load_config_yaml(model_dir)

    access = data.get("access")
    if not access:
        return ModelRole.ADMIN

    for entry in access:
        if _matches(email, entry.get("email", "")):
            role_str = entry.get("role", "").upper()
            try:
                return ModelRole[role_str]
            except KeyError:
                continue

    return None
