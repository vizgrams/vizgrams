# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Job-runner CLI — one subprocess per job.

Each batch_service job (extract, map, materialize, reconcile) runs in its
own child process spawned via ``python -m batch_service.runner <subcmd>``.
The subcommand dispatches to the same ``_run_*_job`` functions the
in-process executor used to call directly.

Why a subprocess: DuckDB 1.5.x has an internal assertion ("Attempting to
dereference an optional pointer that is not set") that on trigger marks
the connection — and by extension the whole Python process — as
invalidated. In-process, one crash wedged every subsequent job for as
long as the batch_service stayed up (we've seen 8-day bad windows). A
crash in a child process only kills that child; the parent stays healthy
and the next job spawns a fresh interpreter with a clean DuckDB state.

The runner is intentionally a thin CLI: argparse, look up the job function,
call it. All progress + status writes go to the batch DB via the existing
``_run_*_job`` internals; the parent doesn't need to hear anything from
the child except its exit code.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


def _configure_logging() -> None:
    """Match the batch_service parent's log format so child stderr reads
    the same in aggregated logs."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _cmd_extract(args: argparse.Namespace) -> int:
    from batch_service.executor import _run_job
    _run_job(
        Path(args.model_dir),
        args.job_id,
        args.tool,
        args.task,
        args.since,
    )
    return 0


def _cmd_map(args: argparse.Namespace) -> int:
    from batch_service.executor import _run_mapper_job
    _run_mapper_job(Path(args.model_dir), args.job_id, args.mapper)
    return 0


def _cmd_materialize(args: argparse.Namespace) -> int:
    from batch_service.executor import _run_materialize_job
    _run_materialize_job(Path(args.model_dir), args.job_id, args.entity)
    return 0


def _cmd_reconcile(args: argparse.Namespace) -> int:
    from batch_service.executor import _run_reconcile_job
    _run_reconcile_job(
        Path(args.model_dir),
        args.job_id,
        args.entity,
        args.feature_id,
    )
    return 0


_HANDLERS = {
    "extract": _cmd_extract,
    "map": _cmd_map,
    "materialize": _cmd_materialize,
    "reconcile": _cmd_reconcile,
}


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="batch_service.runner",
        description="Run one batch_service job in this process, then exit.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("extract", help="Run one extractor task")
    e.add_argument("--model-dir", required=True)
    e.add_argument("--job-id", required=True)
    e.add_argument("--tool", required=True)
    e.add_argument("--task", default=None)
    e.add_argument("--since", default=None)

    m = sub.add_parser("map", help="Run one mapper or all mappers")
    m.add_argument("--model-dir", required=True)
    m.add_argument("--job-id", required=True)
    m.add_argument("--mapper", default=None,
                   help="Mapper name; omit to run all mappers in topological order")

    mat = sub.add_parser("materialize", help="Materialize + reconcile entity tables")
    mat.add_argument("--model-dir", required=True)
    mat.add_argument("--job-id", required=True)
    mat.add_argument("--entity", default=None,
                     help="Entity name; omit to materialize all entities")

    r = sub.add_parser("reconcile", help="Reconcile feature definitions")
    r.add_argument("--model-dir", required=True)
    r.add_argument("--job-id", required=True)
    r.add_argument("--entity", default=None)
    r.add_argument("--feature-id", default=None)

    return p


def main(argv: list[str] | None = None) -> int:
    _configure_logging()
    args = _build_parser().parse_args(argv)
    return _HANDLERS[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
