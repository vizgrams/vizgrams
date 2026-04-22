# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from pydantic import BaseModel


class ViewSummary(BaseModel):
    name: str
    type: str
    query: str


class ViewDetail(BaseModel):
    name: str
    type: str
    query: str
    measure: str | None = None
    visualization: dict[str, Any] = {}
    inputs: dict[str, Any] = {}
    params: list[dict[str, Any]] = []
    raw_yaml: str | None = None


class ViewResult(BaseModel):
    name: str
    type: str
    query: str
    measure: str | None = None
    visualization: dict[str, Any] = {}
    inputs: dict[str, Any] = {}
    params: list[dict[str, Any]] = []
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    total_row_count: int
    duration_ms: int
    truncated: bool
    formats: dict[str, Any] = {}
