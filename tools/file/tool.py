# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""File-based tool: loads curated data from YAML/JSON/CSV files."""

import csv
import json
import logging
from collections.abc import Iterator
from pathlib import Path

import yaml

from tools.base import BaseTool

logger = logging.getLogger(__name__)

COMMANDS = ["load"]
SUPPORTED_FORMATS = {"yaml", "json", "csv"}


class FileTool(BaseTool):
    """Generic file loader for curated reference data (YAML/JSON)."""

    def __init__(self, config: dict | None = None, model_dir: Path | None = None, **_kwargs):
        self._model_dir = Path(model_dir) if model_dir else Path(__file__).resolve().parent.parent.parent

    def list_commands(self) -> list[str]:
        return list(COMMANDS)

    def run(self, command: str, params: dict | None = None) -> Iterator[dict]:
        if command != "load":
            raise ValueError(f"Unknown file command: {command!r}")
        params = params or {}
        yield from self._load(params)

    def _load(self, params: dict) -> Iterator[dict]:
        path = params.get("path")
        if not path:
            raise ValueError("Missing required param: 'path'")

        fmt = params.get("format")
        if not fmt:
            raise ValueError("Missing required param: 'format'")
        if fmt not in SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {fmt!r} (expected one of {SUPPORTED_FORMATS})")

        file_path = Path(path)
        if not file_path.is_absolute():
            file_path = self._model_dir / file_path

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        logger.info("Loading %s file: %s", fmt, file_path)

        with open(file_path) as f:
            if fmt == "yaml":
                data = yaml.safe_load(f)
            elif fmt == "json":
                data = json.load(f)
            elif fmt == "csv":
                reader = csv.DictReader(f)
                data = {"rows": list(reader)}

        yield data
