# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tool configuration router: read and write the tools block of config.yaml."""

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import require_role, resolve_model_dir
from core.rbac import ModelRole
from api.schemas.tool import ToolConfigPatch, ToolConfigResponse, ToolConfigWrite
from api.services import tool_config_service

router = APIRouter(prefix="/model/{model}/config/tool", tags=["tool-config"])


@router.get("", response_model=list[ToolConfigResponse])
def list_tool_configs(model_dir: str = Depends(resolve_model_dir)):
    """List all tool configurations for this model."""
    return tool_config_service.list_tool_configs(model_dir)


@router.get("/{tool}", response_model=ToolConfigResponse)
def get_tool_config(tool: str, model_dir: str = Depends(resolve_model_dir)):
    """Get a single tool's configuration."""
    try:
        return tool_config_service.get_tool_config(model_dir, tool)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.put("/{tool}", response_model=ToolConfigResponse)
def put_tool_config(
    tool: str,
    body: ToolConfigWrite,
    model_dir: str = Depends(resolve_model_dir),
    _=Depends(require_role(ModelRole.ADMIN)),
):
    """Create or fully replace a tool's configuration."""
    try:
        return tool_config_service.put_tool_config(model_dir, tool, body.to_config_dict())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.patch("/{tool}", response_model=ToolConfigResponse)
def patch_tool_config(
    tool: str,
    body: ToolConfigPatch,
    model_dir: str = Depends(resolve_model_dir),
    _=Depends(require_role(ModelRole.ADMIN)),
):
    """Partially update a tool's configuration."""
    try:
        return tool_config_service.patch_tool_config(model_dir, tool, body.to_patch_dict())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.delete("/{tool}", status_code=204)
def delete_tool_config(
    tool: str,
    model_dir: str = Depends(resolve_model_dir),
    _=Depends(require_role(ModelRole.ADMIN)),
):
    """Remove a tool's configuration block."""
    try:
        tool_config_service.delete_tool_config(model_dir, tool)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
