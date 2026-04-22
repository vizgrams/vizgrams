# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from pydantic import BaseModel


class QuerySummary(BaseModel):
    name: str
    root: str | None = None
    measure_count: int = 0
    group_by_count: int = 0
    description: str | None = None


class OrderByClause(BaseModel):
    field: str
    direction: str = "asc"


class SliceDetail(BaseModel):
    field: str
    alias: str = ""
    format_pattern: str = ""


class QueryDetail(BaseModel):
    name: str
    root: str | None = None
    description: str | None = None
    group_by: list[str] = []
    attributes: list[SliceDetail] = []
    measures: dict[str, Any] = {}
    where: list[str] = []
    order_by: list[OrderByClause] = []
    compiled_sql: str | None = None
    raw_yaml: str | None = None


class FormatSpec(BaseModel):
    type: str
    pattern: str | None = None
    unit: str | None = None


class QueryResult(BaseModel):
    query: str
    sql: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    total_row_count: int
    duration_ms: int
    truncated: bool
    formats: dict[str, FormatSpec] = {}
