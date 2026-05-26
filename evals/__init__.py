# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""LLM eval harness (Epic 23).

Runs curated prompts through the chat orchestrator and scores the
responses with a separate LLM-as-judge. Standalone Python — no eval
framework dependency. CLI entry point: ``poetry run python -m evals.run``.

Goal: a closed-loop signal for prompt + system-prompt + tool-description
changes. The orchestrator currently flies blind; this harness gives us
a regression alarm and a baseline for iteration.

See ``docs/adr/0002-evals.md`` (when written) for the design rationale.
"""
