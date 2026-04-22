# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Input schemas (POST / PUT)
# ---------------------------------------------------------------------------

class AttributeIn(BaseModel):
    type: str
    semantic: str | None = None
    references: str | None = None


class RelationIn(BaseModel):
    target: str
    cardinality: str
    via: str | list[str] | None = None
    description: str | None = None


class EntityCreate(BaseModel):
    """Request body for POST /entity and PUT /entity/{entity}."""
    name: str
    description: str | None = None
    identity: dict[str, AttributeIn] = {}
    attributes: dict[str, AttributeIn] = {}
    relations: dict[str, RelationIn] = {}
    # Pass-through blocks (advanced — not validated here)
    history: dict[str, Any] | None = None
    events: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Output schemas (GET responses)
# ---------------------------------------------------------------------------

class AttributeOut(BaseModel):
    name: str
    type: str
    semantic: str


class RelationOut(BaseModel):
    name: str | None = None
    target: str
    cardinality: str
    via: list[str] = []


class EntityDbStats(BaseModel):
    present: bool
    row_count: int = 0
    last_updated_at: str | None = None


class FeatureOut(BaseModel):
    feature_id: str
    name: str
    description: str | None = None
    data_type: str
    expr: str


class EntitySummary(BaseModel):
    name: str
    table_name: str
    attribute_count: int
    relation_count: int
    feature_count: int = 0
    row_count: int | None = None
    table_exists: bool = False


class EntityDetail(BaseModel):
    name: str
    table_name: str
    attributes: list[AttributeOut] = []
    relations: list[RelationOut] = []
    features: list[FeatureOut] = []
    database: EntityDbStats
    display_list: list[str] = []
    display_detail: list[str] = []
    display_order: list[dict[str, str]] = []  # [{column: str, direction: "asc"|"desc"}]
    raw_yaml: str | None = None
