# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from pydantic import BaseModel


class ApplicationSummary(BaseModel):
    name: str
    view_count: int


class LayoutRowOut(BaseModel):
    row: list[str]


class ApplicationDetail(BaseModel):
    name: str
    views: list[str]
    layout: list[LayoutRowOut] = []
    params: list[dict[str, Any]] = []
    raw_yaml: str | None = None
