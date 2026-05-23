# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Build a compact, prompt-ready summary of a model's semantic schema.

Output goes into the system prompt of text2X tools. Density and concision
matter: a 200-entity enterprise model must fit in roughly one context
window alongside conversation history. Target ~100 tokens per entity, so
~20k tokens for a 200-entity model — still within frontier-model limits
with substantial headroom.

Pure function: takes already-loaded ``EntityDef`` + features. Loading from
the DB is the caller's job.
"""

from __future__ import annotations

from semantic.feature import FeatureDef
from semantic.types import Cardinality, EntityDef


def _entity_block(
    e: EntityDef,
    features: list[FeatureDef],
    *,
    max_attrs: int,
    max_features: int,
) -> str:
    """Render one entity's schema in <=10 short lines."""
    lines = [f"ENTITY {e.name}"]
    if e.description:
        lines[0] += f" — {e.description}"
    if e.identity:
        lines.append(f"  identity: {', '.join(a.name for a in e.identity)}")
    if e.attributes:
        attrs = e.attributes[:max_attrs]
        rendered = ", ".join(f"{a.name}:{a.col_type.value}" for a in attrs)
        if len(e.attributes) > max_attrs:
            rendered += f", … ({len(e.attributes) - max_attrs} more)"
        lines.append(f"  attributes: {rendered}")
    if e.relations:
        rels = []
        for r in e.relations:
            card = "1→N" if r.cardinality == Cardinality.ONE_TO_MANY else "N→1"
            rels.append(f"{r.name} ({card} {r.target})")
        lines.append(f"  relations: {', '.join(rels)}")
    if features:
        feat_names = [f.feature_id.split(".")[-1] for f in features[:max_features]]
        rendered = ", ".join(feat_names)
        if len(features) > max_features:
            rendered += f", … ({len(features) - max_features} more)"
        lines.append(f"  features: {rendered}")
    return "\n".join(lines)


def build_schema_context(
    model_name: str,
    entities: list[EntityDef],
    features_by_entity: dict[str, list[FeatureDef]] | None = None,
    *,
    max_attrs_per_entity: int = 20,
    max_features_per_entity: int = 10,
) -> str:
    """Render a model's schema as a system-prompt-ready string.

    ``entities`` is the full list (typically loaded via ``YAMLAdapter``).
    ``features_by_entity`` maps entity name → list of features defined on
    that entity. Both are passed in rather than loaded internally so the
    function stays pure and testable.
    """
    features_by_entity = features_by_entity or {}
    blocks = [
        _entity_block(
            e,
            features_by_entity.get(e.name, []),
            max_attrs=max_attrs_per_entity,
            max_features=max_features_per_entity,
        )
        for e in entities
    ]
    return f"MODEL: {model_name}\n\n" + "\n\n".join(blocks)
