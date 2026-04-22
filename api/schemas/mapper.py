# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel


class SourceOut(BaseModel):
    alias: str | None = None
    table: str | None = None
    columns: list[str] = []


class JoinOut(BaseModel):
    from_alias: str | None = None
    to: str | None = None
    type: str = "left"


class TargetColumnOut(BaseModel):
    name: str | None = None
    expression: str | None = None


class MapperOut(BaseModel):
    name: str
    file: str
    depends_on: list[str] = []
    target_table: str | None = None
    entity: str | None = None
    sources: list[SourceOut] = []
    joins: list[JoinOut] = []
    target_columns: list[TargetColumnOut] = []
    raw_yaml: str | None = None
