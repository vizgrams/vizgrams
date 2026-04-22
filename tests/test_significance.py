# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for core/significance.py"""

from core.significance import compute_significance_score

# ---------------------------------------------------------------------------
# Edge cases — no data / insufficient data
# ---------------------------------------------------------------------------


def test_none_snapshot():
    assert compute_significance_score(None) == 0.0


def test_empty_snapshot():
    assert compute_significance_score([]) == 0.0


def test_single_row():
    rows = [[100]]
    assert compute_significance_score(rows, {"columns": ["value"]}) == 0.0


def test_no_numeric_columns():
    rows = [["team-a"], ["team-b"], ["team-c"]]
    assert compute_significance_score(rows, {"columns": ["team"]}) == 0.0


# ---------------------------------------------------------------------------
# Flat time series — low significance
# ---------------------------------------------------------------------------


def test_flat_time_series_low_score():
    rows = [
        ["2024-01-01", 100],
        ["2024-01-08", 101],
        ["2024-01-15", 99],
        ["2024-01-22", 100],
        ["2024-01-29", 100],
    ]
    score = compute_significance_score(rows, {"columns": ["date", "value"]})
    assert score < 2.0, f"Expected low score for flat series, got {score}"


# ---------------------------------------------------------------------------
# Spike at end of time series — high significance
# ---------------------------------------------------------------------------


def test_spike_at_end_high_score():
    rows = [
        ["2024-01-01", 100],
        ["2024-01-08", 98],
        ["2024-01-15", 102],
        ["2024-01-22", 99],
        ["2024-01-29", 500],  # spike
    ]
    score = compute_significance_score(rows, {"columns": ["date", "value"]})
    assert score > 5.0, f"Expected high score for spike, got {score}"


# ---------------------------------------------------------------------------
# Large period-over-period change — high significance
# ---------------------------------------------------------------------------


def test_large_pop_change_high_score():
    rows = [
        ["2024-01-01", 100],
        ["2024-02-01", 105],
        ["2024-03-01", 103],
        ["2024-04-01", 200],  # 94% PoP change
    ]
    score = compute_significance_score(rows, {"columns": ["month", "revenue"]})
    assert score > 7.0, f"Expected high score for large PoP, got {score}"


# ---------------------------------------------------------------------------
# No time column — uses most extreme value z-score
# ---------------------------------------------------------------------------


def test_no_time_column_high_variance():
    rows = [
        ["team-a", 100],
        ["team-b", 95],
        ["team-c", 98],
        ["team-d", 300],  # outlier
    ]
    score = compute_significance_score(rows, {"columns": ["team", "commits"]})
    assert score > 3.0, f"Expected elevated score for outlier, got {score}"


def test_no_time_column_uniform():
    rows = [["team-a", 10], ["team-b", 10], ["team-c", 10]]
    score = compute_significance_score(rows, {"columns": ["team", "count"]})
    assert score == 0.0, f"Expected 0 for uniform data, got {score}"


# ---------------------------------------------------------------------------
# Dict rows — supported in addition to list rows
# ---------------------------------------------------------------------------


def test_dict_rows():
    rows = [
        {"date": "2024-01-01", "value": 10},
        {"date": "2024-01-08", "value": 10},
        {"date": "2024-01-15", "value": 50},
    ]
    score = compute_significance_score(rows)
    assert score > 2.0


# ---------------------------------------------------------------------------
# No columns in chart_config — synthesised column names
# ---------------------------------------------------------------------------


def test_no_column_names_synthesised():
    rows = [[10], [10], [10], [100]]
    score = compute_significance_score(rows, {})
    assert score > 0.0


# ---------------------------------------------------------------------------
# Score is always in [0, 10]
# ---------------------------------------------------------------------------


def test_score_bounded_extreme_spike():
    rows = [["2024-01", 1], ["2024-02", 1], ["2024-03", 1], ["2024-04", 1000000]]
    score = compute_significance_score(rows, {"columns": ["month", "value"]})
    assert 0.0 <= score <= 10.0


def test_score_bounded_zero_prev():
    # PoP when previous value is ~0
    rows = [["2024-01", 0], ["2024-02", 100]]
    score = compute_significance_score(rows, {"columns": ["month", "value"]})
    assert 0.0 <= score <= 10.0


# ---------------------------------------------------------------------------
# Significance is stored on publish (integration — uses in-memory DB)
# ---------------------------------------------------------------------------


def test_significance_stored_on_create(tmp_path):
    from core.vizgrams_db import create_vizgram, get_vizgram

    rows = [
        ["2024-01-01", 100],
        ["2024-01-08", 98],
        ["2024-01-15", 102],
        ["2024-01-22", 500],
    ]
    chart_config = {"columns": ["date", "value"], "type": "line"}
    score = compute_significance_score(rows, chart_config)

    vizgram_id = create_vizgram(
        dataset_ref="ds",
        query_ref="q",
        title="Test",
        author_id="alice",
        chart_config=chart_config,
        data_snapshot=rows,
        significance_score=score,
        db_path=tmp_path / "vg.db",
    )
    vg = get_vizgram(vizgram_id, db_path=tmp_path / "vg.db")
    assert vg is not None
    assert vg["significance_score"] == score
    assert score > 0.0
