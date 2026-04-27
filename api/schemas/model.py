# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel


class ModelStatus(StrEnum):
    active = "active"
    experimental = "experimental"
    archived = "archived"


class AccessRule(BaseModel):
    email: str
    role: str  # VIEWER | OPERATOR | ADMIN


class ModelSummary(BaseModel):
    name: str
    display_name: str
    description: str
    owner: str
    created_at: str
    status: ModelStatus
    tags: list[str] = []
    is_active: bool = False


class ConfigSummary(BaseModel):
    tools_enabled: list[str] = []
    managed: dict[str, Any] = {}


class DbStats(BaseModel):
    path: str
    present: bool
    raw_tables: int = 0
    raw_row_count: int = 0
    semantic_tables: int = 0
    semantic_row_count: int = 0
    last_extract_at: str | None = None
    last_map_at: str | None = None


class AuditEntry(BaseModel):
    timestamp: str
    event: str
    actor: str
    detail: str | Any


class ModelDetail(ModelSummary):
    config: ConfigSummary | None = None
    database: DbStats
    audit: list[AuditEntry] = []
    access_rules: list[AccessRule] | None = None


class ModelCreate(BaseModel):
    name: str
    display_name: str
    description: str
    owner: str
    status: ModelStatus = ModelStatus.experimental
    tags: list[str] = []
    set_active: bool = False


class ModelPatch(BaseModel):
    display_name: str | None = None
    description: str | None = None
    owner: str | None = None
    tags: list[str] | None = None


class ArchiveRequest(BaseModel):
    reason: str | None = None


class SetActiveResponse(BaseModel):
    active: str


class AccessRulesUpdate(BaseModel):
    rules: list[AccessRule] | None  # None clears DB rules (reverts to config.yaml fallback)


class ModelConfigResponse(BaseModel):
    """GET /model/{model}/config — credential values are masked."""
    tools: dict[str, Any] = {}
    database: dict[str, Any] = {}


class ModelConfigUpdate(BaseModel):
    """PUT /model/{model}/config — update tools and/or database config."""
    tools: dict[str, Any] | None = None
    database: dict[str, Any] | None = None
