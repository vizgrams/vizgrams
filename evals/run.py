# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""CLI entry point — run every case + judge + write the report.

Usage:
  poetry run python -m evals.run                       # all cases, default models
  poetry run python -m evals.run --case-id widget_kpi  # one case
  poetry run python -m evals.run --tag time_series     # filter by tag
  poetry run python -m evals.run --out evals/reports/2026-05-26-fresh-prompt

Real LLM calls go through ``get_default_client()`` — same env vars as
production (OPENAI_API_KEY etc). The judge defaults to a different
provider/model than the authoring one — set ``EVAL_JUDGE_MODEL`` to
override (a Sonnet- or GPT-4-class model recommended).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

# Load .env from the project root before importing anything that reads
# env at import time. Mirrors api/main.py — running `python -m evals.run`
# from any cwd should pick up the same OPENAI_API_KEY / VZ_LLM_PROVIDER
# as the API server.
try:
    from dotenv import load_dotenv
    _env_file = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=_env_file)
except ImportError:
    pass

from evals.case import load_cases  # noqa: E402
from evals.judge import judge  # noqa: E402
from evals.report import build_report, to_scored_case, write_report  # noqa: E402
from evals.runner import run_case  # noqa: E402
from semantic.llm.provider import get_default_client  # noqa: E402

logger = logging.getLogger(__name__)

# Default location for case YAMLs + reports — both anchored at the
# repo root so the CLI works from anywhere.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_CASES_DIR = _REPO_ROOT / "evals" / "cases"
_DEFAULT_REPORTS_DIR = _REPO_ROOT / "evals" / "reports"
_DEFAULT_MODELS_ROOT = _REPO_ROOT / "models"


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    cases = load_cases(args.cases_dir)
    if args.case_id:
        cases = [c for c in cases if c.id == args.case_id]
        if not cases:
            print(f"No case with id {args.case_id!r} in {args.cases_dir}",
                  file=sys.stderr)
            return 1
    if args.tag:
        cases = [c for c in cases if args.tag in c.tags]

    if not cases:
        print("No cases matched the filter.", file=sys.stderr)
        return 1

    print(f"Running {len(cases)} case(s) against authoring + judge models…\n")

    # One client for authoring, one for the judge. If EVAL_JUDGE_MODEL is
    # set we use the same provider with that model id — production
    # quality usually means a smarter judge than author.
    author_client = get_default_client()
    judge_client = get_default_client()
    judge_model = os.environ.get("EVAL_JUDGE_MODEL")

    scored = []
    for i, case in enumerate(cases, start=1):
        print(f"[{i}/{len(cases)}] {case.id}  ({case.prompt!r})", flush=True)
        try:
            run = run_case(case, models_root=args.models_root, llm_client=author_client)
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ runner failed: {exc}", flush=True)
            continue

        score = judge(run, llm_client=judge_client, judge_model=judge_model)
        scored_case = to_scored_case(run, score)
        scored.append(scored_case)

        if score.error:
            print(f"  ⚠ judge error: {score.error}", flush=True)
        else:
            print(
                f"  query={score.query_score}/5  chart={score.chart_score}/5"
                f"  · {score.summary}",
                flush=True,
            )

    print()
    report = build_report(scored)
    out_dir = args.out or _default_out_dir()
    json_path, md_path = write_report(report, out_dir=out_dir)
    print(f"Report: {md_path}")
    print(f"Raw:    {json_path}")
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="evals.run")
    p.add_argument("--cases-dir", type=Path, default=_DEFAULT_CASES_DIR,
                   help="Directory of YAML eval cases (default: evals/cases/).")
    p.add_argument("--models-root", type=Path, default=_DEFAULT_MODELS_ROOT,
                   help="Directory containing model subdirs (default: models/).")
    p.add_argument("--case-id", type=str, default=None,
                   help="Run only this case id.")
    p.add_argument("--tag", type=str, default=None,
                   help="Run only cases with this tag.")
    p.add_argument("--out", type=Path, default=None,
                   help="Report output directory (default: evals/reports/<timestamp>/).")
    return p.parse_args(argv)


def _default_out_dir() -> Path:
    stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S")
    return _DEFAULT_REPORTS_DIR / stamp


if __name__ == "__main__":
    sys.exit(main())
