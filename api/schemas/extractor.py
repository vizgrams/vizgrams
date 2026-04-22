# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from pydantic import BaseModel


class TaskSummary(BaseModel):
    name: str
    command: str
    table: str | None = None


class TaskDetail(TaskSummary):
    params: dict[str, Any] = {}
    incremental: bool = False


class ExtractorSummary(BaseModel):
    tool: str
    tasks: list[TaskSummary] = []


class ExtractorDetail(BaseModel):
    tool: str
    tasks: list[TaskDetail] = []
    raw_yaml: str | None = None
