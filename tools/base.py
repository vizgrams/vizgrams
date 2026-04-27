# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Base tool interface."""

from abc import ABC, abstractmethod
from collections.abc import Iterator


class BaseTool(ABC):
    """Abstract base for data pipeline tools.

    Each tool connects to an external system and yields raw records.
    """

    PARAMS: dict[str, dict] = {}
    """Declare configuration parameters for this tool.

    Keys are parameter names; values are descriptor dicts with:
      required    — bool, whether the parameter must be provided (default False)
      description — str, human-readable description
      credential  — bool, if True the value must use env:/file: format (default False)
      default     — optional default value

    Example::

        PARAMS = {
            "org":   {"required": True, "description": "GitHub organization"},
            "token": {"required": True, "description": "API token", "credential": True},
            "host":  {"required": False, "description": "API host", "default": "github.com"},
        }
    """

    @abstractmethod
    def run(self, command: str, params: dict | None = None) -> Iterator[dict]:
        """Execute a command and yield raw records."""
        ...

    @abstractmethod
    def list_commands(self) -> list[str]:
        """Return available command names."""
        ...

    def resolve_wildcard(self, param_name: str, param_value: str) -> list:
        """Expand a wildcard parameter value into concrete values.

        Override in subclasses that support wildcard expansion.
        Returns a list of concrete values to iterate over.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support wildcard for {param_name!r}"
        )
