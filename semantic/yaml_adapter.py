# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""YAML Adapter Layer — centralised seam for YAML→dataclass translation.

Reads all semantic artifacts from the vizgrams-metadata.db database.
Call sites pass subdirectory paths (e.g. model_dir / "ontology"); this
adapter derives model_dir via .parent so no call sites need updating.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from core import metadata_db
from semantic.feature import parse_feature_dict
from semantic.mapper import parse_mapper_dict
from semantic.ontology import parse_entity_dict
from semantic.query import parse_query_dict
from semantic.application import parse_application_dict
from semantic.view import parse_view_dict
from semantic.types import EntityDef


class YAMLAdapter:
    """Centralised seam for loading YAML definitions from the metadata DB."""

    @staticmethod
    def load_entities(ontology_dir) -> list[EntityDef]:
        model_dir = Path(ontology_dir).parent
        names = metadata_db.list_artifact_names(model_dir, "entity")
        result = []
        for name in names:
            content = metadata_db.get_current_content(model_dir, "entity", name)
            if content:
                try:
                    result.append(parse_entity_dict(yaml.safe_load(content)))
                except Exception:
                    pass
        return result

    @staticmethod
    def load_mappers(mappers_dir) -> list:
        model_dir = Path(mappers_dir).parent
        names = metadata_db.list_artifact_names(model_dir, "mapper")
        result = []
        for name in names:
            content = metadata_db.get_current_content(model_dir, "mapper", name)
            if content:
                try:
                    result.append(parse_mapper_dict(yaml.safe_load(content)))
                except Exception:
                    pass
        return result

    @staticmethod
    def load_features(features_dir) -> list:
        model_dir = Path(features_dir).parent
        names = metadata_db.list_artifact_names(model_dir, "feature")
        result = []
        for name in names:
            content = metadata_db.get_current_content(model_dir, "feature", name)
            if content:
                try:
                    result.append(parse_feature_dict(yaml.safe_load(content)))
                except Exception:
                    pass
        return result

    @staticmethod
    def load_queries(queries_dir) -> list:
        model_dir = Path(queries_dir).parent
        names = metadata_db.list_artifact_names(model_dir, "query")
        result = []
        for name in names:
            content = metadata_db.get_current_content(model_dir, "query", name)
            if content:
                try:
                    result.append(parse_query_dict(yaml.safe_load(content)))
                except Exception:
                    pass
        return result

    @staticmethod
    def load_query(name: str, queries_dir) -> object | None:
        model_dir = Path(queries_dir).parent
        content = metadata_db.get_current_content(model_dir, "query", name)
        if content is None:
            return None
        try:
            return parse_query_dict(yaml.safe_load(content))
        except Exception:
            return None

    @staticmethod
    def load_views(views_dir) -> list:
        model_dir = Path(views_dir).parent
        names = metadata_db.list_artifact_names(model_dir, "view")
        result = []
        for name in names:
            content = metadata_db.get_current_content(model_dir, "view", name)
            if content:
                try:
                    result.append(parse_view_dict(yaml.safe_load(content)))
                except Exception:
                    pass
        return result

    @staticmethod
    def load_view(name: str, views_dir) -> object | None:
        model_dir = Path(views_dir).parent
        content = metadata_db.get_current_content(model_dir, "view", name)
        if content is None:
            return None
        try:
            return parse_view_dict(yaml.safe_load(content))
        except Exception:
            return None

    @staticmethod
    def load_applications(apps_dir) -> list:
        model_dir = Path(apps_dir).parent
        names = metadata_db.list_artifact_names(model_dir, "application")
        result = []
        for name in names:
            content = metadata_db.get_current_content(model_dir, "application", name)
            if content:
                try:
                    result.append(parse_application_dict(yaml.safe_load(content)))
                except Exception:
                    pass
        return result

    @staticmethod
    def load_application(name: str, apps_dir) -> object | None:
        model_dir = Path(apps_dir).parent
        content = metadata_db.get_current_content(model_dir, "application", name)
        if content is None:
            return None
        try:
            return parse_application_dict(yaml.safe_load(content))
        except Exception:
            return None
