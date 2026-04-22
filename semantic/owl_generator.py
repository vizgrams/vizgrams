# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""OWL/RDF generator for the semantic ontology layer.

Converts a list of EntityDef objects into an rdflib Graph serialisable as
OWL Turtle, JSON-LD, or any other RDF format.  Custom annotation properties
carry system-specific metadata (semantic hints, SCD2, event tables) that has
no standard OWL equivalent.

Property naming: all properties are scoped to their declaring entity to avoid
OWL domain-intersection semantics from shared column names across entities.
Format: ``{EntityName}_{propertyName}`` (e.g. ``PullRequest_created_at``).
"""

from __future__ import annotations

from rdflib import OWL, RDF, RDFS, XSD, BNode, Graph, Literal, Namespace, URIRef

from semantic.types import Cardinality, ColumnType, EntityDef

# Annotation-property namespace for vizgrams-specific metadata
VZ_META = Namespace("https://w3id.org/vizgrams/meta#")

_XSD_MAP: dict[ColumnType, URIRef] = {
    ColumnType.STRING: XSD.string,
    ColumnType.INTEGER: XSD.integer,
    ColumnType.FLOAT: XSD.decimal,
}

# (min_cardinality, max_cardinality); None = unconstrained
_CARD_MAP: dict[Cardinality, tuple[int | None, int | None]] = {
    Cardinality.ONE_TO_ONE:   (1, 1),
    Cardinality.MANY_TO_ONE:  (None, 1),
    Cardinality.ONE_TO_MANY:  (1, None),
    Cardinality.MANY_TO_MANY: (None, None),
}


def generate_owl(entities: list[EntityDef], namespace: str) -> Graph:
    """Return an rdflib Graph representing *entities* as OWL classes and properties.

    Args:
        entities:  Loaded EntityDef objects for a model.
        namespace: IRI prefix for this model's ontology, ending with ``#`` or
                   ``/``.  E.g. ``"https://example.org/my-model#"``.
    """
    g = Graph()
    NS = Namespace(namespace)

    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)
    g.bind("wt", VZ_META)
    g.bind("", NS)

    ontology_iri = URIRef(namespace.rstrip("#/"))
    g.add((ontology_iri, RDF.type, OWL.Ontology))

    # Declare custom annotation properties once
    for ann_name in ("semantic", "via", "scd2", "eventOf", "dynamicField"):
        g.add((VZ_META[ann_name], RDF.type, OWL.AnnotationProperty))

    entity_names = {e.name for e in entities}

    for entity in entities:
        cls = NS[entity.name]
        g.add((cls, RDF.type, OWL.Class))
        if entity.description:
            g.add((cls, RDFS.comment, Literal(entity.description)))
        if entity.history:
            g.add((cls, VZ_META.scd2, Literal(True)))

        # Identity + regular attributes
        for attr in entity.identity + entity.attributes:
            _add_datatype_property(g, NS, cls, entity.name, attr.name, attr.col_type, attr.semantic)

        # SCD2 history columns
        if entity.history:
            for attr in entity.history.columns:
                _add_datatype_property(g, NS, cls, entity.name, attr.name, attr.col_type, attr.semantic)

        # Events → separate OWL classes annotated as event-of this entity
        for event in entity.events:
            event_cls_name = f"{entity.name}{event.name.title()}Event"
            event_cls = NS[event_cls_name]
            g.add((event_cls, RDF.type, OWL.Class))
            if event.description:
                g.add((event_cls, RDFS.comment, Literal(event.description)))
            g.add((event_cls, VZ_META.eventOf, cls))
            for attr in event.attributes:
                _add_datatype_property(g, NS, event_cls, event_cls_name, attr.name, attr.col_type, attr.semantic)

        # Relations → scoped ObjectProperties
        for rel in entity.relations:
            prop = NS[f"{entity.name}_{rel.name}"]
            g.add((prop, RDF.type, OWL.ObjectProperty))
            g.add((prop, RDFS.domain, cls))

            if rel.dynamic_field:
                g.add((prop, RDFS.range, OWL.Thing))
                g.add((prop, VZ_META.dynamicField, Literal(rel.dynamic_field)))
            elif rel.target in entity_names:
                g.add((prop, RDFS.range, NS[rel.target]))
            else:
                g.add((prop, RDFS.range, OWL.Thing))

            if rel.inverse:
                g.add((prop, OWL.inverseOf, NS[f"{rel.target}_{rel.inverse}"]))

            if rel.via is not None:
                if isinstance(rel.via, list):
                    via_str = ", ".join(rel.via)
                elif rel.via_target:
                    via_str = f"{rel.via} > {rel.via_target}"
                else:
                    via_str = rel.via
                g.add((prop, VZ_META.via, Literal(via_str)))

            # Add max-cardinality restriction for the ≤1 side
            _, max_card = _CARD_MAP[rel.cardinality]
            if max_card == 1:
                restriction = BNode()
                g.add((restriction, RDF.type, OWL.Restriction))
                g.add((restriction, OWL.onProperty, prop))
                g.add((restriction, OWL.maxCardinality,
                       Literal(1, datatype=XSD.nonNegativeInteger)))
                g.add((cls, RDFS.subClassOf, restriction))

    return g


def _add_datatype_property(
    g: Graph,
    NS: Namespace,
    domain_cls: URIRef,
    entity_prefix: str,
    attr_name: str,
    col_type: ColumnType,
    semantic,
) -> None:
    prop = NS[f"{entity_prefix}_{attr_name}"]
    g.add((prop, RDF.type, OWL.DatatypeProperty))
    g.add((prop, RDFS.domain, domain_cls))
    g.add((prop, RDFS.range, _XSD_MAP[col_type]))
    if semantic is not None:
        g.add((prop, VZ_META.semantic, Literal(semantic.value)))


def to_turtle(entities: list[EntityDef], namespace: str) -> str:
    """Serialise *entities* as OWL Turtle text."""
    return generate_owl(entities, namespace).serialize(format="turtle")


def to_jsonld(entities: list[EntityDef], namespace: str) -> str:
    """Serialise *entities* as JSON-LD text."""
    return generate_owl(entities, namespace).serialize(format="json-ld", indent=2)
