# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Shared types for the semantic layer."""

import re
from dataclasses import dataclass, field
from enum import Enum


class EntityKind(Enum):
    BASE = "BASE"
    EVENT = "EVENT"


class HistoryType(Enum):
    SCD2 = "SCD2"


class Cardinality(Enum):
    ONE_TO_ONE = "ONE_TO_ONE"
    ONE_TO_MANY = "ONE_TO_MANY"
    MANY_TO_ONE = "MANY_TO_ONE"
    MANY_TO_MANY = "MANY_TO_MANY"


class SemanticHint(Enum):
    PRIMARY_KEY = "PRIMARY_KEY"
    IDENTIFIER = "IDENTIFIER"
    RELATION = "RELATION"
    SCD_FROM = "SCD_FROM"
    SCD_TO = "SCD_TO"
    INSERTED_AT = "INSERTED_AT"
    STATE = "STATE"
    TIMESTAMP = "TIMESTAMP"
    ORDERING = "ORDERING"
    MEASURE = "MEASURE"
    ATTRIBUTE = "ATTRIBUTE"
    ENTITY = "ENTITY"


class ColumnType(Enum):
    STRING = "STRING"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"


@dataclass
class AttributeDef:
    name: str
    col_type: ColumnType
    semantic: SemanticHint | None = None
    references: str | None = None
    description: str | None = None


@dataclass
class HistoryDef:
    history_type: HistoryType
    columns: list[AttributeDef] = field(default_factory=list)
    initial_valid_from: str | None = None


@dataclass
class EventDef:
    name: str
    description: str | None = None
    grain: str | None = None
    attributes: list[AttributeDef] = field(default_factory=list)


@dataclass
class RelationDef:
    name: str
    target: str
    via: str | list[str] | None = None  # MANY_TO_ONE: FK col name; ONE_TO_MANY: list of shared col names
    via_target: str | None = None  # explicit target column when via uses "local_col > target_col" syntax
    source: str | None = None
    cardinality: Cardinality = Cardinality.MANY_TO_ONE
    description: str | None = None
    dynamic_field: str | None = None  # set when target: dynamic(field_name)
    inverse: str | None = None  # name of the corresponding relation on the target entity


def _to_snake(name: str) -> str:
    """Convert PascalCase name to snake_case."""
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


@dataclass
class EntityDef:
    name: str
    description: str | None = None
    identity: list[AttributeDef] = field(default_factory=list)
    attributes: list[AttributeDef] = field(default_factory=list)
    history: HistoryDef | None = None
    events: list[EventDef] = field(default_factory=list)
    relations: list[RelationDef] = field(default_factory=list)
    display_list: list[str] = field(default_factory=list)
    display_detail: list[str] = field(default_factory=list)
    display_order: list[tuple[str, str]] = field(default_factory=list)  # [(col, "asc"|"desc")]

    @property
    def table_name(self) -> str:
        """Convert PascalCase name to snake_case table name."""
        return _to_snake(self.name)

    @property
    def primary_key(self) -> AttributeDef | None:
        for a in self.identity:
            if a.semantic == SemanticHint.PRIMARY_KEY:
                return a
        return None

    @property
    def relation_columns(self) -> list[AttributeDef]:
        """RELATION attrs from identity."""
        return [a for a in self.identity if a.semantic == SemanticHint.RELATION]

    @property
    def all_base_columns(self) -> list[AttributeDef]:
        """Identity + attributes + history columns (for materialisation)."""
        cols = list(self.identity) + list(self.attributes)
        if self.history:
            cols.extend(self.history.columns)
        return cols

    @property
    def tracked_columns(self) -> list[AttributeDef]:
        """Columns compared for SCD2 change detection: identity RELATION columns + all attributes."""
        identity_relations = [a for a in self.identity if a.semantic == SemanticHint.RELATION]
        return identity_relations + list(self.attributes)

    def event_table_name(self, event: EventDef) -> str:
        """e.g. product_version_lifecycle_event"""
        return f"{_to_snake(self.name)}_{event.name}_event"

    def event_columns(self, event: EventDef) -> list[AttributeDef]:
        """Parent PK as FK + event attributes."""
        cols = []
        pk = self.primary_key
        if pk:
            cols.append(AttributeDef(
                name=pk.name,
                col_type=pk.col_type,
                semantic=SemanticHint.RELATION,
                references=self.name,
            ))
        cols.extend(event.attributes)
        return cols
