# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from pydantic import BaseModel, model_validator


class ToolSummary(BaseModel):
    name: str
    enabled: bool
    config: dict[str, Any] = {}


class ToolDetail(ToolSummary):
    commands: list[str] = []


# ---------------------------------------------------------------------------
# Tool config write schemas
# ---------------------------------------------------------------------------

_CREDENTIAL_KEYS = frozenset({"api_token", "token", "password", "secret", "api_key"})


class ToolConfigWrite(BaseModel):
    """Full replacement body for PUT /config/tool/{tool}.

    All fields beyond 'enabled' are tool-specific and passed through.
    Credential fields (token, api_token, etc.) must use env: or file: format.
    """

    enabled: bool = True
    model_config = {"extra": "allow"}

    @model_validator(mode="before")
    @classmethod
    def _check_credentials(cls, values: dict) -> dict:
        _assert_safe_credentials(values)
        return values

    def to_config_dict(self) -> dict:
        return self.model_dump()


class ToolConfigPatch(BaseModel):
    """Partial update body for PATCH /config/tool/{tool}.

    Only provided fields are merged into the existing config.
    """

    enabled: bool | None = None
    model_config = {"extra": "allow"}

    @model_validator(mode="before")
    @classmethod
    def _check_credentials(cls, values: dict) -> dict:
        _assert_safe_credentials(values)
        return values

    def to_patch_dict(self) -> dict:
        return {k: v for k, v in self.model_dump().items() if v is not None}


class ToolConfigResponse(BaseModel):
    """A tool's full configuration as stored — credential references included."""

    name: str
    enabled: bool = False
    model_config = {"extra": "allow"}


def _assert_safe_credentials(values: dict) -> None:
    bad = [
        k for k, v in values.items()
        if k in _CREDENTIAL_KEYS and isinstance(v, str)
        and not (v.startswith("env:") or v.startswith("file:"))
    ]
    if bad:
        raise ValueError(
            f"Credential field(s) {bad} must use env:<VAR> or file:<path> format. "
            "Literal credentials must not be stored in config.yaml."
        )
