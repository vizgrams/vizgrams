# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import resolve_model_dir
from api.schemas.tool import ToolDetail, ToolSummary
from api.services import tool_service

router = APIRouter(prefix="/model/{model}/tool", tags=["tools"])


@router.get("", response_model=list[ToolSummary])
def list_tools(model_dir: str = Depends(resolve_model_dir)):
    return tool_service.list_tools(model_dir)


@router.get("/{tool}", response_model=ToolDetail)
def get_tool(tool: str, model_dir: str = Depends(resolve_model_dir)):
    try:
        return tool_service.get_tool_info(model_dir, tool)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Tool '{tool}' not configured.") from None
