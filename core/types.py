# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Shared types for the data pipeline."""

from dataclasses import dataclass, field
from enum import Enum


class WriteMode(Enum):
    APPEND = "APPEND"
    UPSERT = "UPSERT"
    REPLACE = "REPLACE"


@dataclass
class ColumnDef:
    name: str
    json_path: str
    type: str | None = None  # Optional override: TEXT, INTEGER, REAL, JSON


@dataclass
class RowSource:
    mode: str = "SINGLE"  # SINGLE or EXPLODE
    json_path: str | None = None
    inherit: dict[str, str] | None = None  # col_name -> parent json_path


@dataclass
class OutputConfig:
    table: str
    write_mode: WriteMode
    primary_keys: list[str] = field(default_factory=list)
    columns: list[ColumnDef] = field(default_factory=list)
    row_source: RowSource | None = None


@dataclass
class TaskConfig:
    name: str
    tool: str
    command: str
    params: dict = field(default_factory=dict)
    context: dict = field(default_factory=dict)
    outputs: list[OutputConfig] = field(default_factory=list)
    since: str | None = None
    incremental: bool = False
