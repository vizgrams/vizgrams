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
