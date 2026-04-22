# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Input data service: upload raw files to a model's input_data/ directory."""

from pathlib import Path


def upload_file(model_dir: Path, filename: str, content: str) -> dict:
    """Write text content to input_data/{filename}. Creates directory if absent."""
    input_dir = model_dir / "input_data"
    input_dir.mkdir(parents=True, exist_ok=True)

    # Reject path traversal attempts
    target = (input_dir / filename).resolve()
    if not str(target).startswith(str(input_dir.resolve())):
        raise ValueError(f"Invalid filename: '{filename}'")

    target.write_text(content, encoding="utf-8")
    return {"file": filename, "size": target.stat().st_size}


def list_files(model_dir: Path) -> list[dict]:
    """List files in input_data/."""
    input_dir = model_dir / "input_data"
    if not input_dir.is_dir():
        return []
    return [
        {"file": p.name, "size": p.stat().st_size}
        for p in sorted(input_dir.iterdir())
        if p.is_file()
    ]
