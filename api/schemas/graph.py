# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel


class GraphNode(BaseModel):
    id: str
    label: str


class GraphEdge(BaseModel):
    id: str
    source: str
    target: str
    label: str
    cardinality: str


class GraphOut(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]
