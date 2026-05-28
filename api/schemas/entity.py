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


# ---------------------------------------------------------------------------
# Entity-scoped views (Epic 26 VG-290) — `/entity/{entity}/charts`
# ---------------------------------------------------------------------------

# Mirrors ViewSummary but adds chart_type (flattened from ViewDef.type +
# visualization.chart_type) so consumers can judge fit without looking at
# the visualization blob.
from api.schemas.view import ViewSummary  # noqa: E402 — avoid pydantic forward-ref ceremony


class ChartSummary(ViewSummary):
    chart_type: str = ""  # bar | line | kpi | table | scatter | ...


# ---------------------------------------------------------------------------
# Entity pipeline (Epic 26 VG-290) — `/entity/{entity}/pipeline`
#
# Mapper-rooted lineage graph: each entity has at most one mapper feeding
# it (current convention); that mapper joins one or more raw tables, each
# produced by an extractor whose task names a tool. The Pipeline tab in
# /explore renders this as `[Tool → Extractor → Raw] × N → Mapper → Entity`.
# ---------------------------------------------------------------------------

class PipelineSource(BaseModel):
    tool: str | None = None        # may be None if the extractor isn't resolvable
    extractor: str | None = None
    raw_table: str


class PipelineMapperGroup(BaseModel):
    # The `from_alias` of a RowGroup inside the mapper's target — when the
    # mapper has `rows:` entries this discriminates the sub-groups (e.g.
    # `authors` / `reviews` / `commits` for a Contribution mapper).
    name: str


class PipelineMapper(BaseModel):
    name: str
    groups: list[PipelineMapperGroup] = []


class PipelineSummary(BaseModel):
    entity: str
    sources: list[PipelineSource] = []
    mapper: PipelineMapper | None = None


# ---------------------------------------------------------------------------
# Entity activity feed (Epic 26 VG-290) — `/entity/{entity}/activity`
#
# Aggregates changes touching this entity across multiple artifact kinds:
# - Entity artifact diffs projected per row (attribute / relation changes)
# - Feature artifact version bumps for features scoped to this entity
# - View artifact version bumps for views rooted on this entity
# - Mapper artifact version bumps for mappers targeting this entity
#
# Pagination is offset-based — the Activity tab loads the top N and asks
# for more on scroll.
# ---------------------------------------------------------------------------

class ActivityEvent(BaseModel):
    actor: str | None = None        # username, or null for system-generated
    action: str                     # created | updated | deleted | restored | ran
    object_kind: str                # chart | computed | attribute | relation | mapper
    object_name: str
    created_at: str                 # ISO 8601
    note: str | None = None         # e.g. "v3 → v4" or "412 rows in 1.2s"
    # Present when the event came from a projection over this entity's own
    # artifact-version timeline. Multiple events sharing the same value
    # came from the same version bump — the UI clusters them.
    ontology_version: str | None = None


class ActivityFeed(BaseModel):
    events: list[ActivityEvent]
    has_more: bool = False


# ---------------------------------------------------------------------------
# Computed Describe-it (Epic 26 VG-293) — LLM helper for the Schema tab
# ---------------------------------------------------------------------------


class ComputedDescribeRequest(BaseModel):
    description: str  # natural-language brief, e.g. "lead time in hours"


class ComputedDescribeResponse(BaseModel):
    name: str    # snake_case identifier
    expr: str    # vizgrams expression DSL string
