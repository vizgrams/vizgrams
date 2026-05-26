# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for the eval framework (Epic 23 VG-260+).

Framework-only tests — no real LLM calls. Tests for actual prompt
quality live as runnable cases under ``evals/cases/`` and are run via
``python -m evals.run`` (real API keys required).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from evals.case import EvalCase, load_case, load_cases
from evals.judge import Score, judge
from evals.report import ScoredCase, build_report, to_markdown, to_scored_case
from evals.runner import CaseRun
from semantic.llm.provider import LLMResponse, ToolCall
from tests.llm.conftest import FakeLLMClient

# ---------------------------------------------------------------------------
# EvalCase YAML loader
# ---------------------------------------------------------------------------


def _write_case(tmp_path: Path, name: str, body: dict) -> Path:
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(body, sort_keys=False))
    return path


class TestLoadCase:
    def test_round_trips_minimum_fields(self, tmp_path):
        path = _write_case(tmp_path, "x", {
            "id": "x", "model": "iagai", "prompt": "show me PRs",
            "expectations": {"query": "Q", "chart": "C"},
        })
        c = load_case(path)
        assert c.id == "x"
        assert c.model == "iagai"
        assert c.prompt == "show me PRs"
        assert c.query_expectation == "Q"
        assert c.chart_expectation == "C"
        assert c.source_path == str(path)

    def test_id_defaults_to_filename_stem_when_absent(self, tmp_path):
        path = _write_case(tmp_path, "weekly_throughput", {
            "model": "iagai", "prompt": "x",
        })
        assert load_case(path).id == "weekly_throughput"

    def test_missing_model_or_prompt_is_a_clear_error(self, tmp_path):
        path = _write_case(tmp_path, "x", {"id": "x"})
        with pytest.raises(ValueError, match="model.*prompt"):
            load_case(path)

    def test_expectations_default_empty(self, tmp_path):
        path = _write_case(tmp_path, "x", {
            "id": "x", "model": "m", "prompt": "p",
        })
        c = load_case(path)
        assert c.query_expectation == ""
        assert c.chart_expectation == ""

    def test_tags_default_empty_list(self, tmp_path):
        path = _write_case(tmp_path, "x", {
            "id": "x", "model": "m", "prompt": "p",
        })
        assert load_case(path).tags == []


class TestLoadCases:
    def test_loads_directory_sorted_by_id(self, tmp_path):
        for name in ("b_case", "a_case", "c_case"):
            _write_case(tmp_path, name, {
                "id": name, "model": "m", "prompt": "p",
            })
        cases = load_cases(tmp_path)
        assert [c.id for c in cases] == ["a_case", "b_case", "c_case"]

    def test_duplicate_id_raises(self, tmp_path):
        _write_case(tmp_path, "a", {"id": "shared", "model": "m", "prompt": "p"})
        _write_case(tmp_path, "b", {"id": "shared", "model": "m", "prompt": "p"})
        with pytest.raises(ValueError, match="duplicate case id"):
            load_cases(tmp_path)


def test_shipped_case_library_is_valid():
    """Every case under evals/cases/ must load — guards against typos
    and missing fields in new cases. Also catches accidental id collisions
    if someone copies-and-modifies an existing case."""
    cases_dir = Path(__file__).resolve().parents[1] / "evals" / "cases"
    cases = load_cases(cases_dir)
    assert len(cases) >= 8, "expected the seed library of ≥ 8 cases"
    # Each shipped case should have non-empty expectations on both axes;
    # an empty expectation defeats the judge.
    for c in cases:
        assert c.query_expectation, f"case {c.id!r}: query expectation is empty"
        assert c.chart_expectation, f"case {c.id!r}: chart expectation is empty"


# ---------------------------------------------------------------------------
# Judge
# ---------------------------------------------------------------------------


def _case() -> EvalCase:
    return EvalCase(
        id="t", model="iagai", prompt="how many widgets?",
        query_expectation="root Widget, count(widget_key)",
        chart_expectation="kpi for single scalar",
    )


def _run(success: bool = True) -> CaseRun:
    """Build a CaseRun with a minimal ChatTurnResult to feed the judge."""
    from api.services.chat.service import ChatTurnResult
    return CaseRun(
        case=_case(),
        result=ChatTurnResult(
            success=success,
            iterations=2,
            title="Widget count",
            query_yaml="name: text2query\nroot: Widget\n",
            view_yaml="name: text2view\ntype: metric\n",
            sql="SELECT COUNT(*) FROM widget",
        ),
        duration_s=0.4,
        tool_sequence=["build_and_run_query", "present_view"],
    )


def _score_response(args: dict) -> LLMResponse:
    return LLMResponse(
        content=None,
        tool_calls=[ToolCall(id="c1", name="score_response", arguments=args)],
    )


class TestJudge:
    def test_happy_path_returns_parsed_score(self):
        client = FakeLLMClient()
        client.responses.append(_score_response({
            "query_score": 5, "query_notes": "spot on",
            "chart_score": 4, "chart_notes": "slight axis nit",
            "summary": "great answer",
        }))
        s = judge(_run(), llm_client=client)
        assert s.query_score == 5
        assert s.query_notes == "spot on"
        assert s.chart_score == 4
        assert s.summary == "great answer"
        assert s.overall == 4.5
        assert s.error is None

    def test_missing_tool_call_is_captured_as_error(self):
        client = FakeLLMClient()
        client.responses.append(LLMResponse(
            content="I cannot score this", tool_calls=[],
        ))
        s = judge(_run(), llm_client=client)
        assert s.error is not None
        assert s.query_score == 0
        assert s.chart_score == 0

    def test_judge_llm_exception_is_captured(self):
        class Bombing:
            def complete(self, **_):
                raise RuntimeError("network down")
        s = judge(_run(), llm_client=Bombing())
        assert s.error is not None
        assert "network down" in s.error

    def test_malformed_args_are_captured_not_raised(self):
        client = FakeLLMClient()
        client.responses.append(_score_response({
            "query_score": "five", "query_notes": "x",
            # ... missing chart_score / chart_notes / summary
        }))
        s = judge(_run(), llm_client=client)
        assert s.error is not None


# ---------------------------------------------------------------------------
# Report aggregation
# ---------------------------------------------------------------------------


def _scored(query_score: int = 5, chart_score: int = 5,
            judge_error: str | None = None) -> ScoredCase:
    return ScoredCase(
        case_id="t", prompt="p", model="m", tags=[],
        duration_s=0.1, tool_sequence=["build_and_run_query", "present_view"],
        succeeded=True,
        query_score=query_score, query_notes="q",
        chart_score=chart_score, chart_notes="c",
        summary="s", judge_error=judge_error,
    )


class TestBuildReport:
    def test_empty_report_is_well_formed(self):
        r = build_report([])
        assert r["case_count"] == 0
        assert r["cases"] == []

    def test_means_computed_only_over_judged_cases(self):
        r = build_report([
            _scored(query_score=4, chart_score=2),
            _scored(query_score=5, chart_score=5),
            _scored(judge_error="boom"),  # excluded from means
        ])
        assert r["case_count"] == 3
        assert r["judged_count"] == 2
        assert r["judge_failure_count"] == 1
        assert r["stats"]["mean_query_score"] == pytest.approx(4.5)
        assert r["stats"]["mean_chart_score"] == pytest.approx(3.5)
        assert r["stats"]["mean_overall"] == pytest.approx(4.0)


class TestToMarkdown:
    def test_empty_report(self):
        md = to_markdown(build_report([]))
        assert "No cases run" in md

    def test_lists_each_case_and_aggregate(self):
        report = build_report([_scored(query_score=4, chart_score=3)])
        md = to_markdown(report)
        assert "# Eval report" in md
        assert "Query | 4.00" in md
        assert "| `t` |" in md

    def test_weak_cases_get_detailed_notes_section(self):
        report = build_report([_scored(query_score=2, chart_score=5)])
        md = to_markdown(report)
        # Below the threshold (4) → detailed section appears.
        assert "Cases needing attention" in md
        assert "Query (2/5)" in md

    def test_judge_failures_render_dash_not_zero(self):
        report = build_report([_scored(judge_error="boom")])
        md = to_markdown(report)
        # Score columns show "—" rather than a misleading "0".
        assert "| — | — |" in md


# ---------------------------------------------------------------------------
# to_scored_case bridge
# ---------------------------------------------------------------------------


def test_to_scored_case_carries_everything_judge_and_run_produced():
    score = Score(
        query_score=4, query_notes="qn",
        chart_score=3, chart_notes="cn",
        summary="meh",
    )
    sc = to_scored_case(_run(), score)
    assert sc.case_id == "t"
    assert sc.tool_sequence == ["build_and_run_query", "present_view"]
    assert sc.query_score == 4
    assert sc.chart_notes == "cn"
    assert sc.judge_error is None
    assert sc.succeeded is True
