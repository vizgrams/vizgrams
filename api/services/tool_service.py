# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Re-exports from core.tool_service for backwards compatibility."""

from core.tool_service import (  # noqa: F401
    BUILTIN_REGISTRY,
    get_tool_info,
    get_tool_instance,
    list_tools,
)
