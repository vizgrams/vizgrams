# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel


class FeatureSummary(BaseModel):
    feature_id: str | None = None
    name: str
    entity: str
    feature_type: str
    description: str | None = None
    data_type: str | None = None
    expr: str | None = None
    raw_yaml: str | None = None


class FeatureDetail(FeatureSummary):
    expression: str | None = None
    raw_sql: str | None = None
    compiled_sql: str | None = None
