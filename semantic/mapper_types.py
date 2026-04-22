# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Shared types for the mapper subsystem."""

from dataclasses import dataclass, field
from enum import Enum


class JoinType(Enum):
    LEFT = "left"
    INNER = "inner"


@dataclass
class SourceDef:
    alias: str
    table: str | None          # mutually exclusive with union
    columns: list[str]
    union: list[str] | None = None   # UNION ALL of multiple tables
    filter: dict | str | None = None
    deduplicate: list[str] | None = None
    static: dict[str, str] | None = None  # constant column values (schema only)


@dataclass
class JoinCondition:
    left: str   # alias.column
    right: str  # alias.column
    operator: str = "eq"  # "eq" or "json_array_contains"
    json_path: str | None = None  # e.g. "$.jira" — only for json_array_contains
    prefix: str | None = None  # e.g. "@ORG/" — prepended to left before comparison


@dataclass
class JoinDef:
    from_alias: str
    to_alias: str
    join_type: JoinType
    on: list[JoinCondition]


@dataclass
class TargetColumn:
    name: str
    expression: str


@dataclass
class RowGroup:
    from_alias: str
    joins: list["JoinDef"]
    columns: list[TargetColumn]


@dataclass
class TargetDef:
    entity_name: str
    columns: list[TargetColumn] = field(default_factory=list)
    rows: list[RowGroup] = field(default_factory=list)


@dataclass
class EnumMapping:
    name: str
    mapping: dict[str, list[str]]

    def reverse_lookup(self, value: str) -> str:
        """Find the canonical key for a raw value."""
        for canonical, raw_values in self.mapping.items():
            if value in raw_values:
                return canonical
        raise ValueError(f"No enum mapping found for value {value!r} in {self.name}")


@dataclass
class MapperConfig:
    name: str
    description: str | None = None
    depends_on: list[str] = field(default_factory=list)
    grain: str | None = None
    enums: list[EnumMapping] = field(default_factory=list)
    sources: list[SourceDef] = field(default_factory=list)
    joins: list[JoinDef] = field(default_factory=list)
    targets: list[TargetDef] = field(default_factory=list)

    def get_enum(self, name: str) -> EnumMapping | None:
        for e in self.enums:
            if e.name == name:
                return e
        return None

    def get_source(self, alias: str) -> SourceDef | None:
        for s in self.sources:
            if s.alias == alias:
                return s
        return None
