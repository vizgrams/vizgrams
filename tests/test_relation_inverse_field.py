# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for the ``inverse`` field on ``RelationIn``.

Before the fix, ``POST /model/{m}/entity`` couldn't create a bidirectional
relation in a single call: the schema silently dropped ``inverse`` from
the request body, and any follow-up ``PUT /entity/{name}/yaml`` failed
ontology validation with

    inverse '<other>.<rel>' declares inverse='None' but should declare
    inverse='<rel>'

on both sides — chicken-and-egg. The only workaround was stripping
relations entirely and manually editing the YAML files later. This
made installers like ops/scripts/install_model.py unable to reproduce
a bidirectional-relation model without hand-editing prod state.

These tests pin:
1. The schema accepts and preserves ``inverse``.
2. The YAML serialiser propagates it through to the on-disk YAML that
   subsequent PUT calls validate against.
"""

from api.schemas.entity import RelationIn


def test_relation_in_accepts_inverse():
    """Schema-level: ``inverse`` is a first-class field, not silently
    dropped as an ``additionalProperty``."""
    r = RelationIn(
        target="PersonalRecord",
        cardinality="ONE_TO_MANY",
        via=["activity_id"],
        inverse="activity",
    )
    assert r.inverse == "activity"


def test_relation_in_inverse_optional():
    """Bare MANY_TO_ONE relations that don't need inverse (validator
    only requires it for ONE_TO_MANY) still round-trip. Keeps the
    field truly optional — no breaking change for existing callers."""
    r = RelationIn(target="Product", cardinality="MANY_TO_ONE", via="product_key")
    assert r.inverse is None


def test_rel_to_yaml_writes_inverse():
    """Service-level: ``_rel_to_yaml`` propagates inverse into the
    YAML dict that ends up in metadata_db. Previously this helper
    only serialised target/cardinality/via/description — inverse
    was lost between the request body and the on-disk YAML."""
    from api.services.entity_service import _rel_to_yaml
    result = _rel_to_yaml({
        "target": "PersonalRecord",
        "cardinality": "ONE_TO_MANY",
        "via": ["activity_id"],
        "inverse": "activity",
    })
    assert result == {
        "target": "PersonalRecord",
        "cardinality": "ONE_TO_MANY",
        "via": ["activity_id"],
        "inverse": "activity",
    }


def test_rel_to_yaml_skips_missing_inverse():
    """No inverse → key absent from the emitted YAML (not
    ``inverse: null``). Keeps the on-disk YAML clean for the common
    MANY_TO_ONE case where inverse is irrelevant."""
    from api.services.entity_service import _rel_to_yaml
    result = _rel_to_yaml({
        "target": "Product",
        "cardinality": "MANY_TO_ONE",
        "via": "product_key",
    })
    assert "inverse" not in result


def test_rel_to_yaml_skips_empty_string_inverse():
    """An explicit empty string inverse (a client sending
    ``inverse: ""`` rather than omitting it) shouldn't become a
    dangling ``inverse: ''`` in the YAML. ``rel.get("inverse")``
    on falsy value → skip is deliberate; this guards it."""
    from api.services.entity_service import _rel_to_yaml
    result = _rel_to_yaml({
        "target": "Product",
        "cardinality": "MANY_TO_ONE",
        "via": "product_key",
        "inverse": "",
    })
    assert "inverse" not in result


def test_end_to_end_bidirectional_relation_via_model_dump():
    """The full pipe: pydantic RelationIn → .model_dump() → _rel_to_yaml.
    Mirrors the router path (create_entity → entity_service). Ensures
    the plumbing works when both go through in one call, so a caller
    can POST an entity with a bidirectional relation and have the
    inverse land in the stored YAML."""
    from api.services.entity_service import _rel_to_yaml
    rel = RelationIn(
        target="PersonalRecord",
        cardinality="ONE_TO_MANY",
        via=["activity_id"],
        inverse="activity",
    )
    yaml_dict = _rel_to_yaml(rel.model_dump())
    assert yaml_dict.get("inverse") == "activity"
