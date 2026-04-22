# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Ontology YAML parser and validator for the semantic layer."""

from pathlib import Path

import yaml

from core.validation import ValidationError, validate_schema
from semantic.types import (
    AttributeDef,
    Cardinality,
    ColumnType,
    EntityDef,
    EventDef,
    HistoryDef,
    HistoryType,
    RelationDef,
    SemanticHint,
)


def _parse_attributes(attrs_dict: dict) -> list[AttributeDef]:
    """Parse a dict of attribute definitions into AttributeDef list."""
    result = []
    for attr_name, attr_def in attrs_dict.items():
        semantic_val = attr_def.get("semantic")
        result.append(AttributeDef(
            name=attr_name,
            col_type=ColumnType(attr_def["type"]),
            semantic=SemanticHint(semantic_val) if semantic_val else None,
            references=attr_def.get("references"),
            description=attr_def.get("description"),
        ))
    return result


def parse_entity_dict(data: dict) -> EntityDef:
    """Parse an entity dict (already YAML-loaded) into an EntityDef."""
    identity = _parse_attributes(data.get("identity", {}))
    attributes = _parse_attributes(data.get("attributes", {}))

    history = None
    history_data = data.get("history")
    if history_data:
        history_type = HistoryType(history_data["type"])
        hist_attrs = {k: v for k, v in history_data.items() if k not in ("type", "initial_valid_from")}
        history = HistoryDef(
            history_type=history_type,
            columns=_parse_attributes(hist_attrs),
            initial_valid_from=history_data.get("initial_valid_from"),
        )

    events = []
    for event_name, event_data in (data.get("events") or {}).items():
        events.append(EventDef(
            name=event_name,
            description=event_data.get("description"),
            grain=event_data.get("grain"),
            attributes=_parse_attributes(event_data.get("attributes", {})),
        ))

    relations = []
    for rel_name, rel_data in (data.get("relations") or {}).items():
        raw_via = rel_data.get("via")
        via, via_target = raw_via, None
        if isinstance(raw_via, str) and ">" in raw_via:
            parts = [p.strip() for p in raw_via.split(">", 1)]
            via, via_target = parts[0], parts[1]
        raw_target = rel_data["target"]
        dynamic_field = None
        if isinstance(raw_target, str) and raw_target.startswith("dynamic(") and raw_target.endswith(")"):
            dynamic_field = raw_target[8:-1].strip()
            raw_target = ""
        relations.append(RelationDef(
            name=rel_name,
            target=raw_target,
            via=via,
            via_target=via_target,
            source=rel_data.get("source"),
            cardinality=Cardinality(rel_data["cardinality"]),
            description=rel_data.get("description"),
            dynamic_field=dynamic_field,
            inverse=rel_data.get("inverse"),
        ))

    display = data.get("display") or {}
    display_list = display.get("list") or []
    display_detail = display.get("detail") or []
    display_order: list[tuple[str, str]] = []
    for item in display.get("order") or []:
        if isinstance(item, dict):
            for col, dir_ in item.items():
                direction = str(dir_).lower()
                if direction not in ("asc", "desc"):
                    direction = "asc"
                display_order.append((str(col), direction))
                break

    return EntityDef(
        name=data["entity"],
        description=data.get("description"),
        identity=identity,
        attributes=attributes,
        history=history,
        events=events,
        relations=relations,
        display_list=display_list,
        display_detail=display_detail,
        display_order=display_order,
    )


def parse_entity_yaml(path: str | Path) -> EntityDef:
    """Parse a single entity YAML file into an EntityDef."""
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)
    return parse_entity_dict(data)


def load_all_entities(ontology_dir: str | Path) -> list[EntityDef]:
    """Load all entities from the metadata DB (ontology_dir.parent is the model_dir)."""
    from core import metadata_db
    model_dir = Path(ontology_dir).parent
    result = []
    for name in metadata_db.list_artifact_names(model_dir, "entity"):
        content = metadata_db.get_current_content(model_dir, "entity", name)
        if content:
            try:
                result.append(parse_entity_dict(yaml.safe_load(content)))
            except Exception:
                pass
    return result


def load_entity_by_name(name: str, ontology_dir: str | Path) -> EntityDef | None:
    """Load a single entity by its PascalCase name."""
    for entity in load_all_entities(ontology_dir):
        if entity.name == name:
            return entity
    return None


def _check_entity_rules(data: dict) -> list[ValidationError]:
    """Phase 2: semantic cross-field checks for entity configs."""
    errors = []
    identity = data.get("identity", {})

    # identity_required — non-empty identity block
    if not identity:
        errors.append(ValidationError(
            path="identity",
            message="entity must have a non-empty identity block",
            rule="identity_required",
        ))

    # entity_requires_pk — exactly one PRIMARY_KEY in identity
    pk_count = sum(
        1 for a in identity.values() if a.get("semantic") == "PRIMARY_KEY"
    )
    if pk_count != 1:
        errors.append(ValidationError(
            path="identity",
            message=f"identity must have exactly one PRIMARY_KEY (found {pk_count})",
            rule="entity_requires_pk",
        ))

    # Check identity attributes
    for attr_name, attr_def in identity.items():
        semantic = attr_def.get("semantic")
        references = attr_def.get("references")
        prefix = f"identity.{attr_name}"

        # relation_requires_references
        if semantic == "RELATION" and not references:
            errors.append(ValidationError(
                path=prefix,
                message=f"attribute '{attr_name}' has RELATION semantic but no 'references'",
                rule="relation_requires_references",
            ))

        # references_requires_relation
        if references and semantic != "RELATION":
            errors.append(ValidationError(
                path=prefix,
                message=f"attribute '{attr_name}' has 'references' but semantic is not RELATION",
                rule="references_requires_relation",
            ))

    # history_requires_scd_columns
    history = data.get("history")
    if history:
        hist_attrs = {k: v for k, v in history.items() if k not in ("type", "initial_valid_from")}
        has_scd_from = any(
            a.get("semantic") == "SCD_FROM" for a in hist_attrs.values()
        )
        has_scd_to = any(
            a.get("semantic") == "SCD_TO" for a in hist_attrs.values()
        )
        if not has_scd_from or not has_scd_to:
            errors.append(ValidationError(
                path="history",
                message="history block must have SCD_FROM and SCD_TO attributes",
                rule="history_requires_scd_columns",
            ))

    # event_requires_inserted_at
    for event_name, event_data in (data.get("events") or {}).items():
        event_attrs = event_data.get("attributes", {})
        has_inserted_at = any(
            a.get("semantic") == "INSERTED_AT" for a in event_attrs.values()
        )
        if not has_inserted_at:
            errors.append(ValidationError(
                path=f"events.{event_name}",
                message=f"event '{event_name}' must have an INSERTED_AT attribute",
                rule="event_requires_inserted_at",
            ))

    # dynamic_field_must_exist_and_have_entity_semantic
    attribute_defs = data.get("attributes", {})
    for rel_name, rel_data in (data.get("relations") or {}).items():
        raw_target = rel_data.get("target", "")
        if not (isinstance(raw_target, str) and raw_target.startswith("dynamic(") and raw_target.endswith(")")):
            continue
        dyn_field = raw_target[8:-1].strip()
        if dyn_field not in attribute_defs:
            errors.append(ValidationError(
                path=f"relations.{rel_name}",
                message=f"dynamic field '{dyn_field}' not found in attributes",
                rule="dynamic_field_must_exist",
            ))
        elif attribute_defs[dyn_field].get("semantic") != "ENTITY":
            errors.append(ValidationError(
                path=f"relations.{rel_name}",
                message=f"dynamic field '{dyn_field}' must have semantic: ENTITY",
                rule="dynamic_field_must_have_entity_semantic",
            ))

    # relation_via_must_exist
    # Validates that the *local* (this entity) side of every via expression exists here.
    # MANY_TO_ONE:  via="fk_col" or "fk_col > target_col" — local col is always left of >.
    # ONE_TO_MANY:  via="local_col > child_col" — validate local_col here.
    #               Array form (via=[child_col, ...]) has no local col; skip.
    identity_names = set(identity.keys())
    attribute_names = set(data.get("attributes", {}).keys())
    via_candidates = identity_names | attribute_names
    for rel_name, rel_data in (data.get("relations") or {}).items():
        via = rel_data.get("via")
        if not via:
            continue
        cardinality = rel_data.get("cardinality")
        if cardinality == "ONE_TO_MANY":
            # Only the "local_col > child_col" string form has a local col to validate.
            if isinstance(via, str) and ">" in via:
                local_col = via.split(">")[0].strip()
                if local_col not in via_candidates:
                    errors.append(ValidationError(
                        path=f"relations.{rel_name}",
                        message=f"relation 'via' column '{local_col}' not found in identity or attributes",
                        rule="relation_via_must_exist",
                    ))
            continue
        via_cols = [via] if isinstance(via, str) else via
        for col in via_cols:
            # Strip "local_col > target_col" syntax — validate only the local col
            col = col.split(">")[0].strip() if isinstance(col, str) and ">" in col else col
            if col not in via_candidates:
                errors.append(ValidationError(
                    path=f"relations.{rel_name}",
                    message=f"relation 'via' column '{col}' not found in identity or attributes",
                    rule="relation_via_must_exist",
                ))

    # relation_source_must_exist
    event_names = set((data.get("events") or {}).keys())
    for rel_name, rel_data in (data.get("relations") or {}).items():
        source = rel_data.get("source")
        if source and source not in event_names:
            errors.append(ValidationError(
                path=f"relations.{rel_name}",
                message=f"relation 'source' event '{source}' not found in events",
                rule="relation_source_must_exist",
            ))

    return errors


_INVERSE_CARDINALITY = {
    Cardinality.MANY_TO_ONE: Cardinality.ONE_TO_MANY,
    Cardinality.ONE_TO_MANY: Cardinality.MANY_TO_ONE,
    Cardinality.ONE_TO_ONE: Cardinality.ONE_TO_ONE,
    Cardinality.MANY_TO_MANY: Cardinality.MANY_TO_MANY,
}


def _requires_inverse(rel: "RelationDef", entity_map: "dict[str, EntityDef]") -> bool:
    """Return True if this relation must declare an inverse.

    Exempt cases:
    - Dynamic target (target entity is not known statically).
    - Via uses '>' syntax (FK column name differs from target PK; ONE_TO_MANY inverse
      cannot be expressed under current via semantics).
    - ONE_TO_MANY with composite via (2+ columns; no single MANY_TO_ONE inverse column).
    - MANY_TO_ONE where the via column name differs from the target entity's PK name
      (asymmetric FK, e.g. subject_key → person_key).
    """
    if rel.dynamic_field is not None:
        return False
    if isinstance(rel.via, str) and ">" in rel.via:
        return False
    if isinstance(rel.via, list) and len(rel.via) >= 2:
        return False
    if rel.cardinality == Cardinality.MANY_TO_ONE:
        target = entity_map.get(rel.target)
        if target:
            pk = target.primary_key
            if pk and isinstance(rel.via, str) and rel.via != pk.name:
                return False
    return True


def validate_all_entities(entities: list[EntityDef]) -> list[ValidationError]:
    """Cross-entity validation checks."""
    errors = []
    entity_names = {e.name for e in entities}
    entity_map: dict[str, EntityDef] = {e.name: e for e in entities}

    # Also build a set of event entity names
    event_entity_names = set()
    for e in entities:
        for ev in e.events:
            # Event entity name = ParentName + EventName (PascalCase) + Event
            # But for relation target lookup, we check against event table resolution
            event_entity_names.add(f"{e.name}.{ev.name}")

    # Build column-name lookup for cross-entity validation
    entity_col_names: dict[str, set[str]] = {}
    for e in entities:
        entity_col_names[e.name] = {a.name for a in e.all_base_columns}

    # Build relation-name lookup for bidirectionality validation
    entity_rel_map: dict[str, dict[str, RelationDef]] = {}
    for e in entities:
        entity_rel_map[e.name] = {r.name: r for r in e.relations}

    for entity in entities:
        # relation_target_exists (skip dynamic relations)
        for rel in entity.relations:
            if rel.dynamic_field is not None:
                continue
            if rel.target not in entity_names:
                errors.append(ValidationError(
                    path=f"{entity.name}.relations.{rel.name}",
                    message=f"relation target '{rel.target}' not found in entities",
                    rule="relation_target_exists",
                ))

        # reference_entity_exists
        for attr in entity.identity:
            if attr.references and attr.references not in entity_names:
                errors.append(ValidationError(
                    path=f"{entity.name}.identity.{attr.name}",
                    message=f"referenced entity '{attr.references}' not found",
                    rule="reference_entity_exists",
                ))

        # one_to_many_via_cols_must_match
        # For ONE_TO_MANY relations with a list via, each column must exist
        # on both this entity and the target entity (names must be identical).
        for rel in entity.relations:
            if rel.cardinality != Cardinality.ONE_TO_MANY:
                continue
            if not isinstance(rel.via, list):
                continue
            target_cols = entity_col_names.get(rel.target, set())
            for col in rel.via:
                if col not in target_cols:
                    errors.append(ValidationError(
                        path=f"{entity.name}.relations.{rel.name}",
                        message=(
                            f"ONE_TO_MANY via column '{col}' not found on "
                            f"target entity '{rel.target}' — via column names must match on both sides"
                        ),
                        rule="one_to_many_via_cols_must_match",
                    ))

        # via_target_must_exist
        # When "local_col > target_col" syntax is used, target_col must exist
        # on the target entity.
        for rel in entity.relations:
            if not rel.via_target:
                continue
            target_cols = entity_col_names.get(rel.target, set())
            if rel.via_target not in target_cols:
                errors.append(ValidationError(
                    path=f"{entity.name}.relations.{rel.name}",
                    message=(
                        f"via target column '{rel.via_target}' not found on "
                        f"target entity '{rel.target}'"
                    ),
                    rule="via_target_must_exist",
                ))

        # Bidirectionality: every non-exempt relation must declare inverse,
        # and the inverse must be consistent.
        for rel in entity.relations:
            if not _requires_inverse(rel, entity_map):
                continue

            path = f"{entity.name}.relations.{rel.name}"

            # relation_requires_inverse
            if rel.inverse is None:
                errors.append(ValidationError(
                    path=path,
                    message=(
                        f"relation '{rel.name}' on '{entity.name}' must declare 'inverse' "
                        f"(the name of the corresponding relation on '{rel.target}')"
                    ),
                    rule="relation_requires_inverse",
                ))
                continue

            target = entity_map.get(rel.target)
            if target is None:
                continue  # already reported by relation_target_exists

            inv_rels = entity_rel_map.get(rel.target, {})

            # relation_inverse_exists
            if rel.inverse not in inv_rels:
                errors.append(ValidationError(
                    path=path,
                    message=(
                        f"inverse '{rel.inverse}' not found on target entity '{rel.target}'"
                    ),
                    rule="relation_inverse_exists",
                ))
                continue

            inv_rel = inv_rels[rel.inverse]

            # relation_inverse_points_back
            if inv_rel.target != entity.name:
                errors.append(ValidationError(
                    path=path,
                    message=(
                        f"inverse '{rel.target}.{rel.inverse}' targets '{inv_rel.target}' "
                        f"but should target '{entity.name}'"
                    ),
                    rule="relation_inverse_points_back",
                ))

            # relation_inverse_mutual
            if inv_rel.inverse != rel.name:
                errors.append(ValidationError(
                    path=path,
                    message=(
                        f"inverse '{rel.target}.{rel.inverse}' declares inverse='{inv_rel.inverse}' "
                        f"but should declare inverse='{rel.name}'"
                    ),
                    rule="relation_inverse_mutual",
                ))

            # relation_inverse_cardinality
            expected = _INVERSE_CARDINALITY.get(rel.cardinality)
            if expected and inv_rel.cardinality != expected:
                errors.append(ValidationError(
                    path=path,
                    message=(
                        f"cardinality mismatch: '{entity.name}.{rel.name}' is "
                        f"{rel.cardinality.value} but inverse "
                        f"'{rel.target}.{rel.inverse}' is {inv_rel.cardinality.value} "
                        f"(expected {expected.value})"
                    ),
                    rule="relation_inverse_cardinality",
                ))

    return errors


def validate_ontology_yaml(path: str | Path) -> list[ValidationError]:
    """Validate an ontology YAML config file (both phases)."""
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)

    # Phase 1: structural
    errors = validate_schema(data, "ontology")
    if errors:
        return errors

    # Phase 2: semantic (only if structure is valid)
    errors.extend(_check_entity_rules(data))
    return errors
