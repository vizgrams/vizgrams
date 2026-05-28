# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Role-based access control for vizgrams.

Two tiers (collapsed from three in Epic 26 VG-292 — viewer and creator
merged into ``member``):

  System-level  — controlled via environment variables.  Two platform roles:

    System Admin — ``VZ_SYSTEM_ADMINS`` (comma-separated emails / domain
                   wildcards).  Full platform access.  ``DEV_USER`` is always
                   treated as a system admin so local development is
                   frictionless.

    Member       — any authenticated user.  Can browse the feed, run
                   queries, author charts + computed features, and use
                   chat.  Governance-sensitive surfaces (ontology rows,
                   mappers, extractors) use the propose-change flow
                   (Epic 26 VG-294+) for members; admins write directly.

  Model-level   — controlled via the ``access:`` block in each model's
                  ``config.yaml`` or DB-driven access rules.  Entries are
                  evaluated in order; the first match wins.  Supports
                  exact email, domain wildcard (``*@domain.com``), and
                  catch-all (``*``).

Model role hierarchy (higher includes all permissions of lower):

  VIEWER    — view model, run queries, run Applications, view job history
  OPERATOR  — VIEWER + trigger extractions, cancel jobs
  ADMIN     — OPERATOR + edit extractors, tool config, model metadata

When a model has no ``access:`` block, all authenticated users get ADMIN
access (open by default — opt in to restrictions by adding the block).

When a user matches no entry in the access list, they have no access and
the model is hidden from their model listing.
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


def is_member(email: str | None) -> bool:
    """Return True if *email* is an authenticated user.

    Replaces the old ``is_creator``: any authenticated user is now a
    Member with chart + computed authoring rights. Admins (above) still
    have the extra governance powers.
    """
    return bool(email)


def get_model_role(model_dir: Path, email: str) -> ModelRole | None:
    """Return the user's role for *model_dir*, or ``None`` for no access.

    Resolution order:
    1. System admins → ADMIN unconditionally.
    2. DB access_rules set → use those (empty list = open/ADMIN).
    3. No DB rules → fall back to ``config.yaml`` access block.
    4. No access block anywhere → ADMIN (open by default).
    5. No match in access list → ``None`` (model hidden, 403).
    """
    if is_system_admin(email):
        return ModelRole.ADMIN

    model_id = Path(model_dir).name
    from core.vizgrams_db import get_model_access_rules
    db_rules = get_model_access_rules(model_id)

    if db_rules is not None:
        # DB is authoritative; empty list means open to all authenticated users
        if not db_rules:
            return ModelRole.ADMIN
        for entry in db_rules:
            if _matches(email, entry.get("email", "")):
                role_str = entry.get("role", "").upper()
                try:
                    return ModelRole[role_str]
                except KeyError:
                    continue
        return None

    # Fall back to config.yaml
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
