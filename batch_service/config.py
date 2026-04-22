# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Runtime configuration for the batch service."""

from __future__ import annotations

import os
from pathlib import Path


def get_models_dir() -> Path:
    """Return the models directory from VZ_MODELS_DIR env var or project default."""
    env = os.environ.get("VZ_MODELS_DIR")
    if env:
        return Path(env)
    # Default: project root / models (batch_service/ is one level below project root)
    return Path(__file__).resolve().parents[1] / "models"


def get_model_dir(model: str) -> Path:
    """Resolve and validate a model directory path."""
    model_dir = get_models_dir() / model
    if not model_dir.is_dir():
        raise KeyError(f"Model '{model}' not found.")
    return model_dir


def get_batch_service_port() -> int:
    return int(os.environ.get("BATCH_SERVICE_PORT", "8001"))
