# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel


class ValidationError(BaseModel):
    path: str
    message: str


class ValidationResult(BaseModel):
    valid: bool
    errors: list[ValidationError] = []
    compiled_sql: str | None = None


class YAMLContent(BaseModel):
    """Raw YAML string for create/upsert endpoints."""
    content: str


class CertFields(BaseModel):
    """Certification surface for user-facing artifacts (VG-258).

    Mixed into View / Query / Feature summaries and details so the UI can
    filter the library by ``is_certified`` and show "certified by X on Y"
    on the detail page. Defaults are NULL/false so the response remains
    backward-compatible for callers that pre-date the certification PR.
    """
    is_certified: bool = False
    certified_by: str | None = None         # user UUID; resolve via vizgrams_db
    certified_by_display: str | None = None
    certified_at: str | None = None


class OwnerFields(BaseModel):
    """Ownership surface for user-facing artifacts (VG-252).

    Mixed into View / Query / Feature summaries and details so the UI can
    show "created by X" + filter to "things I own". The ``created_via``
    enum reveals which surface authored the save — useful for distinguishing
    library work from chat-spawned drafts.
    """
    created_by: str | None = None           # user UUID; null for legacy/system rows
    created_by_display: str | None = None
    created_via: str | None = None          # 'editor' | 'chat' | 'sync' | 'system'
    created_at: str | None = None
