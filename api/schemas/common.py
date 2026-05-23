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
