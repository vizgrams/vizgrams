# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from pydantic import BaseModel

from api.services.job_service import JobStatus


class JobOut(BaseModel):
    job_id: str
    model: str
    operation: str
    status: JobStatus
    started_at: str
    extractor: str | None = None
    entity: str | None = None
    task: str | None = None
    completed_at: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    progress: list[str] = []
    warnings: list[str] = []
