import pandas as pd

from goldbot.impulse import body_atr_ratio, confirms_breakout


def _bar(open_, high, low, close):
    return {"open": open_, "high": high, "low": low, "close": close, "volume": 100}


def test_body_atr_ratio_zero_atr_safe() -> None:
    df = pd.DataFrame([_bar(100.0, 101.0, 99.0, 100.5)])
    sig = body_atr_ratio(df, atr=0.0)
    assert sig.body_atr_ratio == 0.0
    assert sig.direction == "FLAT"


def test_strong_up_candle_confirms_long_breakout() -> None:
    # body = 0.8, atr = 1.0 -> 0.80 >= 0.40 threshold
    df = pd.DataFrame([_bar(100.0, 100.9, 99.9, 100.8)])
    sig = body_atr_ratio(df, atr=1.0)
    assert sig.direction == "UP"
    assert sig.body_atr_ratio >= 0.40
    assert confirms_breakout(sig, required_direction="UP", body_atr_min=0.40) is True
    # Same candle does NOT confirm a short breakout.
    assert confirms_breakout(sig, required_direction="DOWN", body_atr_min=0.40) is False


def test_weak_body_fails_confirmation() -> None:
    # body = 0.1, atr = 1.0 -> 0.10 < 0.40 threshold
    df = pd.DataFrame([_bar(100.0, 100.9, 99.9, 100.1)])
    sig = body_atr_ratio(df, atr=1.0)
    assert confirms_breakout(sig, required_direction="UP", body_atr_min=0.40) is False


def test_wrong_direction_fails_confirmation() -> None:
    # Strong down candle
    df = pd.DataFrame([_bar(100.0, 100.2, 99.0, 99.2)])
    sig = body_atr_ratio(df, atr=1.0)
    assert sig.direction == "DOWN"
    assert confirms_breakout(sig, required_direction="UP", body_atr_min=0.40) is False
    assert confirms_breakout(sig, required_direction="DOWN", body_atr_min=0.40) is True
