# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Graph endpoint — entity relationship graph and OWL export."""

from pathlib import Path

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response

from api.dependencies import resolve_model_dir
from api.schemas.graph import GraphEdge, GraphNode, GraphOut
from semantic.owl_generator import to_jsonld, to_turtle
from semantic.yaml_adapter import YAMLAdapter

router = APIRouter(prefix="/model/{model}/graph", tags=["graph"])

_NS_BASE = "https://w3id.org/vizgrams/ontology/"


@router.get("", response_model=GraphOut)
def get_graph(
    model_dir: Path = Depends(resolve_model_dir),
    format: str = Query(default="json", pattern="^(json|turtle|jsonld)$"),
):
    """Return the entity relationship graph.

    - ``format=json`` (default) — Cytoscape-ready node/edge JSON
    - ``format=turtle`` — OWL Turtle (``text/turtle``)
    - ``format=jsonld`` — OWL JSON-LD (``application/ld+json``)
    """
    entities = YAMLAdapter.load_entities(model_dir / "ontology")
    namespace = f"{_NS_BASE}{model_dir.name}#"

    if format == "turtle":
        return Response(
            content=to_turtle(entities, namespace),
            media_type="text/turtle",
            headers={"Content-Disposition": f'attachment; filename="{model_dir.name}.owl.ttl"'},
        )
    if format == "jsonld":
        return Response(
            content=to_jsonld(entities, namespace),
            media_type="application/ld+json",
        )

    entity_names = {e.name for e in entities}
    nodes = [GraphNode(id=e.name, label=e.name) for e in entities]

    seen: set[str] = set()
    edges: list[GraphEdge] = []
    for entity in entities:
        for rel in entity.relations:
            if not rel.target or rel.target not in entity_names or rel.target == entity.name:
                continue
            key = "|".join(sorted([entity.name, rel.target, rel.name]))
            if key in seen:
                continue
            seen.add(key)
            edges.append(GraphEdge(
                id=key,
                source=entity.name,
                target=rel.target,
                label=rel.name,
                cardinality=rel.cardinality.value,
            ))

    return GraphOut(nodes=nodes, edges=edges)
