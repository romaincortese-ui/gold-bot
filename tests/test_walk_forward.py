from datetime import datetime, timezone

import pytest

from goldbot.walk_forward import (
    aggregate_out_sample_pf,
    evaluate_stability,
    generate_walk_forward_splits,
)


def test_generate_splits_empty_window():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 10, tzinfo=timezone.utc)
    splits = generate_walk_forward_splits(start, end, in_sample_days=30, out_sample_days=10, step_days=10)
    assert splits == []


def test_generate_splits_basic():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, tzinfo=timezone.utc)
    splits = generate_walk_forward_splits(start, end, in_sample_days=90, out_sample_days=30, step_days=30)
    assert len(splits) >= 7
    first = splits[0]
    assert (first.in_sample_end - first.in_sample_start).days == 90
    assert (first.out_sample_end - first.out_sample_start).days == 30
    assert first.out_sample_start == first.in_sample_end


def test_generate_splits_step_advance():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, tzinfo=timezone.utc)
    splits = generate_walk_forward_splits(start, end, in_sample_days=90, out_sample_days=30, step_days=45)
    assert splits[1].out_sample_start - splits[0].out_sample_start == (
        splits[1].out_sample_start - splits[0].out_sample_start
    )
    delta = splits[1].out_sample_start - splits[0].out_sample_start
    assert delta.days == 45


def test_generate_splits_rejects_bad_config():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        generate_walk_forward_splits(start, end, in_sample_days=0, out_sample_days=30, step_days=30)


def test_evaluate_stability_passes():
    result = evaluate_stability(
        in_sample_pf=1.5,
        out_sample_pf=1.3,
        min_out_sample_pf=1.15,
        max_pf_degradation=0.5,
    )
    assert result.passed is True
    assert result.reason == "stable"


def test_evaluate_stability_rejects_below_min_pf():
    result = evaluate_stability(
        in_sample_pf=1.5,
        out_sample_pf=1.05,
        min_out_sample_pf=1.15,
        max_pf_degradation=0.5,
    )
    assert result.passed is False
    assert result.reason == "out_sample_pf_below_minimum"


def test_evaluate_stability_rejects_excessive_degradation():
    result = evaluate_stability(
        in_sample_pf=3.0,
        out_sample_pf=1.2,
        min_out_sample_pf=1.15,
        max_pf_degradation=0.5,
    )
    assert result.passed is False
    assert result.reason == "excessive_pf_degradation"
    assert result.degradation > 0.5


def test_evaluate_stability_rejects_non_positive_is_pf():
    result = evaluate_stability(
        in_sample_pf=0.0,
        out_sample_pf=1.5,
        min_out_sample_pf=1.15,
        max_pf_degradation=0.5,
    )
    assert result.passed is False
    assert result.reason == "non_positive_in_sample_pf"


def test_aggregate_out_sample_pf_weighted():
    # (pf, n_trades)
    result = aggregate_out_sample_pf([(1.5, 20), (1.0, 80)])
    expected = (1.5 * 20 + 1.0 * 80) / 100
    assert abs(result - expected) < 1e-9


def test_aggregate_out_sample_pf_zero_trades():
    assert aggregate_out_sample_pf([]) == 0.0
    assert aggregate_out_sample_pf([(1.5, 0), (1.2, 0)]) == 0.0


def test_aggregate_out_sample_pf_skips_non_positive_pf():
    result = aggregate_out_sample_pf([(1.5, 10), (0.0, 5), (-0.2, 5)])
    assert result == 1.5
