# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Reusable version sub-routes — included by each artifact router."""
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import resolve_model_dir
from core import metadata_db


def make_version_routes(artifact_type: str, tags: list[str] | None = None) -> APIRouter:
    router = APIRouter(tags=tags or ["versions"])

    @router.get("/{name}/versions")
    def list_versions(name: str, model_dir: Path = Depends(resolve_model_dir)):
        return metadata_db.list_versions(model_dir, artifact_type, name)

    @router.get("/{name}/versions/{version_id}")
    def get_version(
        name: str, version_id: str, model_dir: Path = Depends(resolve_model_dir)
    ):
        record = metadata_db.get_version(model_dir, artifact_type, name, version_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Version not found")
        return record

    return router
