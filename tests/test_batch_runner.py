# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for ``batch_service/runner.py`` — the subprocess CLI entrypoint.

The runner is a thin dispatch: parse argv, look up the right ``_run_*_job``
function on the executor module, and call it with the parsed args. These
tests exercise the parser + dispatch, mocking out the underlying job
functions so we don't actually hit DuckDB.
"""

from unittest.mock import patch

import pytest

from batch_service import runner

# ---------------------------------------------------------------------------
# extract
# ---------------------------------------------------------------------------


def test_extract_dispatches_with_all_args():
    with patch("batch_service.executor._run_job") as mock:
        rc = runner.main([
            "extract",
            "--model-dir", "/models/foo",
            "--job-id", "job-1",
            "--tool", "git",
            "--task", "commits",
            "--since", "2026-01-01",
        ])
    assert rc == 0
    args, _ = mock.call_args
    # Positional signature is (model_dir, job_id, tool, task, since)
    assert str(args[0]) == "/models/foo"
    assert args[1] == "job-1"
    assert args[2] == "git"
    assert args[3] == "commits"
    assert args[4] == "2026-01-01"


def test_extract_defaults_optional_args_to_none():
    with patch("batch_service.executor._run_job") as mock:
        rc = runner.main([
            "extract",
            "--model-dir", "/m",
            "--job-id", "j",
            "--tool", "git",
        ])
    assert rc == 0
    _, task, since = mock.call_args[0][3], mock.call_args[0][3], mock.call_args[0][4]
    assert task is None and since is None


def test_extract_missing_required_arg_exits():
    with pytest.raises(SystemExit):
        # missing --tool
        runner.main(["extract", "--model-dir", "/m", "--job-id", "j"])


# ---------------------------------------------------------------------------
# map
# ---------------------------------------------------------------------------


def test_map_dispatches_with_mapper_name():
    with patch("batch_service.executor._run_mapper_job") as mock:
        rc = runner.main([
            "map",
            "--model-dir", "/models/foo",
            "--job-id", "job-2",
            "--mapper", "team",
        ])
    assert rc == 0
    args, _ = mock.call_args
    assert str(args[0]) == "/models/foo"
    assert args[1] == "job-2"
    assert args[2] == "team"


def test_map_without_mapper_runs_all_mappers():
    """Omitting --mapper means "run all mappers" — the job function must
    receive ``None`` so it takes the all-mappers branch, not an empty
    string that would try to find a mapper named ``""``."""
    with patch("batch_service.executor._run_mapper_job") as mock:
        rc = runner.main([
            "map", "--model-dir", "/m", "--job-id", "j",
        ])
    assert rc == 0
    assert mock.call_args[0][2] is None


# ---------------------------------------------------------------------------
# materialize
# ---------------------------------------------------------------------------


def test_materialize_dispatches_with_entity():
    with patch("batch_service.executor._run_materialize_job") as mock:
        rc = runner.main([
            "materialize",
            "--model-dir", "/models/foo",
            "--job-id", "job-3",
            "--entity", "Team",
        ])
    assert rc == 0
    assert mock.call_args[0][2] == "Team"


def test_materialize_without_entity_materializes_all():
    with patch("batch_service.executor._run_materialize_job") as mock:
        runner.main([
            "materialize", "--model-dir", "/m", "--job-id", "j",
        ])
    assert mock.call_args[0][2] is None


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------


def test_reconcile_dispatches_with_scope():
    with patch("batch_service.executor._run_reconcile_job") as mock:
        rc = runner.main([
            "reconcile",
            "--model-dir", "/models/foo",
            "--job-id", "job-4",
            "--entity", "Repository",
            "--feature-id", "repository.commit_count",
        ])
    assert rc == 0
    args, _ = mock.call_args
    assert args[2] == "Repository"
    assert args[3] == "repository.commit_count"


def test_reconcile_without_scope_reconciles_all():
    with patch("batch_service.executor._run_reconcile_job") as mock:
        runner.main([
            "reconcile", "--model-dir", "/m", "--job-id", "j",
        ])
    args = mock.call_args[0]
    assert args[2] is None and args[3] is None


# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------


def test_unknown_subcommand_exits():
    with pytest.raises(SystemExit):
        runner.main(["bogus", "--model-dir", "/m", "--job-id", "j"])


def test_no_subcommand_exits():
    """argparse should require a subcommand — this guards against a future
    refactor accidentally dropping ``required=True`` on the subparsers."""
    with pytest.raises(SystemExit):
        runner.main([])


# ---------------------------------------------------------------------------
# External tool discovery
# ---------------------------------------------------------------------------


def test_main_calls_init_system_tools_before_dispatch():
    """The subprocess must discover VZ_TOOLS_DIR tools (iagai_metadata,
    coingecko, etc.) before running the job. If this init call is
    dropped, the external registry stays empty and every non-builtin
    extract fails with ``Unknown tool``. Regression fix — the bug shipped
    for a day after the subprocess-per-job refactor moved job execution
    out of the parent process's lifespan."""
    with patch("core.tool_service.init_system_tools") as init, \
         patch("batch_service.executor._run_job") as run:
        runner.main([
            "extract", "--model-dir", "/m", "--job-id", "j", "--tool", "git",
        ])
    init.assert_called_once()
    # And dispatch still happens after init.
    run.assert_called_once()


def test_init_tools_failure_does_not_prevent_dispatch():
    """Best-effort discovery: a broken tool.py in VZ_TOOLS_DIR mustn't
    stop the job runner from executing whatever DID load, or a single
    typo takes down the whole subprocess for every extract."""
    with patch("core.tool_service.init_system_tools", side_effect=RuntimeError("bad tool")), \
         patch("batch_service.executor._run_job") as run:
        runner.main([
            "extract", "--model-dir", "/m", "--job-id", "j", "--tool", "git",
        ])
    run.assert_called_once()
