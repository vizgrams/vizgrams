# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Execute one eval case against the chat orchestrator.

The runner is thin — it calls the same ``chat_turn`` production uses,
collects the response + trace, and packages everything the judge needs
to score. Real-LLM by default; tests inject a ``FakeLLMClient`` so the
framework itself is testable without burning credits.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from api.services.chat.service import ChatTurnResult, chat_turn

if TYPE_CHECKING:
    from evals.case import EvalCase
    from semantic.llm.provider import LLMClient


@dataclass
class CaseRun:
    """The orchestrator's response to one eval case, plus diagnostics
    the judge consumes."""

    case: EvalCase
    result: ChatTurnResult
    duration_s: float
    # Tool sequence as a flat list of names — easy for the judge to skim.
    # The trace itself is on result.trace if more detail is needed.
    tool_sequence: list[str] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.result.success


def run_case(
    case: EvalCase,
    *,
    models_root: Path,
    llm_client: LLMClient | None = None,
) -> CaseRun:
    """Run ``case`` against ``chat_turn``. Times the call; captures trace.

    ``llm_client`` is injectable for self-tests (FakeLLMClient). Default
    None → ``get_default_client()`` reads env (OPENAI_API_KEY etc).
    """
    model_dir = models_root / case.model
    if not model_dir.is_dir():
        raise FileNotFoundError(
            f"Model {case.model!r} not found at {model_dir}. "
            "Set evals.runner models_root, or check ``cases/*.yaml model:`` fields."
        )

    t0 = time.monotonic()
    result = chat_turn(
        model_dir=model_dir,
        message=case.prompt,
        llm_client=llm_client,
    )
    duration = time.monotonic() - t0

    return CaseRun(
        case=case,
        result=result,
        duration_s=duration,
        tool_sequence=[t.name for t in (result.trace or [])],
    )
