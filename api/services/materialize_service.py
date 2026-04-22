# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Materialize service: create semantic layer tables from ontology YAML definitions."""

from pathlib import Path

from semantic.yaml_adapter import YAMLAdapter


def materialize(
    model_dir: Path,
    entity_names: list[str] | None = None,
    yaml_file: str | None = None,
) -> list[str]:
    """Create semantic tables. Returns list of table names created."""
    from semantic.ontology import load_entity_by_name, parse_entity_yaml

    ontology_dir = model_dir / "ontology"
    all_entities = YAMLAdapter.load_entities(ontology_dir)

    if yaml_file:
        yp = Path(yaml_file)
        if not yp.is_absolute():
            yp = ontology_dir / yp if yp.parent == Path(".") else model_dir.parent.parent / yp
        if not yp.exists():
            raise FileNotFoundError(f"File not found: {yp}")
        targets = [parse_entity_yaml(yp)]
    elif entity_names:
        targets = []
        for name in entity_names:
            e = load_entity_by_name(name, ontology_dir)
            if e is None:
                raise KeyError(f"Entity '{name}' not found in ontology.")
            targets.append(e)
    else:
        targets = all_entities

    from core.db import get_backend
    from semantic.materialize import materialize_with_backend

    backend = get_backend(model_dir)
    backend.connect()
    try:
        return materialize_with_backend(targets, backend)
    finally:
        backend.close()
