from dataclasses import replace

from goldbot.regime import (
    RegimeClassification,
    classify_from_settings,
    classify_regime,
    parse_strategy_csv,
    strategy_allowed_in_regime,
)
from tests.test_strategies import build_settings


def test_classify_regime_quiet():
    r = classify_regime(
        atr_pct=0.004,
        quiet_atr_pct_max=0.006,
        trend_atr_pct_max=0.014,
        spike_atr_pct_min=0.016,
    )
    assert r.regime == "quiet_carry"


def test_classify_regime_trend():
    r = classify_regime(
        atr_pct=0.010,
        quiet_atr_pct_max=0.006,
        trend_atr_pct_max=0.014,
        spike_atr_pct_min=0.016,
    )
    assert r.regime == "trend"


def test_classify_regime_spike_atr():
    r = classify_regime(
        atr_pct=0.020,
        quiet_atr_pct_max=0.006,
        trend_atr_pct_max=0.014,
        spike_atr_pct_min=0.016,
    )
    assert r.regime == "spike"


def test_classify_regime_news_burst_forces_spike():
    r = classify_regime(
        atr_pct=0.004,
        quiet_atr_pct_max=0.006,
        trend_atr_pct_max=0.014,
        spike_atr_pct_min=0.016,
        news_burst=True,
    )
    assert r.regime == "spike"
    assert r.news_burst is True


def test_classify_regime_between_trend_and_spike_is_trend():
    r = classify_regime(
        atr_pct=0.015,
        quiet_atr_pct_max=0.006,
        trend_atr_pct_max=0.014,
        spike_atr_pct_min=0.016,
    )
    assert r.regime == "trend"


def test_classify_regime_negative_atr_neutral():
    r = classify_regime(
        atr_pct=-0.001,
        quiet_atr_pct_max=0.006,
        trend_atr_pct_max=0.014,
        spike_atr_pct_min=0.016,
    )
    assert r.regime == "neutral"


def test_strategy_allowed_in_regime_defaults():
    assert strategy_allowed_in_regime("quiet_carry", "EXHAUSTION_REVERSAL")
    assert not strategy_allowed_in_regime("quiet_carry", "TREND_PULLBACK")
    assert strategy_allowed_in_regime("trend", "TREND_PULLBACK")
    assert strategy_allowed_in_regime("trend", "MACRO_BREAKOUT")
    assert not strategy_allowed_in_regime("trend", "EXHAUSTION_REVERSAL")
    assert strategy_allowed_in_regime("spike", "MACRO_BREAKOUT")
    assert not strategy_allowed_in_regime("spike", "EXHAUSTION_REVERSAL")
    # neutral allows everything
    assert strategy_allowed_in_regime("neutral", "TREND_PULLBACK")


def test_strategy_allowed_custom_lists():
    assert strategy_allowed_in_regime(
        "quiet_carry", "TREND_PULLBACK", quiet_strategies=["TREND_PULLBACK"]
    )


def test_parse_strategy_csv():
    assert parse_strategy_csv("A, b ,C") == ("A", "B", "C")
    assert parse_strategy_csv("") == ()


def test_classify_from_settings_respects_thresholds():
    settings = replace(
        build_settings(),
        regime_quiet_atr_pct_max=0.005,
        regime_trend_atr_pct_max=0.010,
        regime_spike_atr_pct_min=0.012,
    )
    assert classify_from_settings(settings, atr_pct=0.003).regime == "quiet_carry"
    assert classify_from_settings(settings, atr_pct=0.008).regime == "trend"
    assert classify_from_settings(settings, atr_pct=0.020).regime == "spike"
