import math

import numpy as np
import pandas as pd


def calc_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def calc_rsi(series: pd.Series, period: int = 14) -> float:
    if len(series) < period + 1:
        return 50.0
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    value = float(rsi.iloc[-1])
    return value if not math.isnan(value) else 50.0


def calc_macd(df: pd.DataFrame) -> dict[str, float | bool]:
    close = df["close"]
    ema12 = calc_ema(close, 12)
    ema26 = calc_ema(close, 26)
    macd_line = ema12 - ema26
    signal = calc_ema(macd_line, 9)
    histogram = macd_line - signal
    return {
        "macd": float(macd_line.iloc[-1]),
        "signal": float(signal.iloc[-1]),
        "histogram": float(histogram.iloc[-1]),
        "cross_up": float(macd_line.iloc[-1]) > float(signal.iloc[-1]) and float(macd_line.iloc[-2]) <= float(signal.iloc[-2]),
        "cross_down": float(macd_line.iloc[-1]) < float(signal.iloc[-1]) and float(macd_line.iloc[-2]) >= float(signal.iloc[-2]),
    }


def calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    if df is None or len(df) < period + 1:
        return 0.0
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["close"].shift(1)).abs(),
            (df["low"] - df["close"].shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    return float(atr.iloc[-1])


def consolidation_box(df: pd.DataFrame, lookback_bars: int, atr_period: int = 14) -> dict[str, float]:
    window = df.tail(lookback_bars)
    if window.empty:
        return {"high": 0.0, "low": 0.0, "width": 0.0, "atr": 0.0, "width_atr_ratio": 0.0}
    high = float(window["high"].max())
    low = float(window["low"].min())
    atr = calc_atr(df, atr_period)
    width = high - low
    return {
        "high": high,
        "low": low,
        "width": width,
        "atr": atr,
        "width_atr_ratio": width / atr if atr > 0 else 0.0,
    }


def nearest_support_resistance(df: pd.DataFrame, lookback: int = 60) -> dict[str, float]:
    window = df.tail(lookback)
    return {
        "support": float(window["low"].min()),
        "resistance": float(window["high"].max()),
    }


def detect_divergence(df: pd.DataFrame, lookback: int = 40) -> dict[str, bool]:
    window = df.tail(lookback).reset_index(drop=True)
    if len(window) < 12:
        return {"bullish": False, "bearish": False}

    close = window["close"]
    rsi_series = close.rolling(14).apply(lambda values: calc_rsi(pd.Series(values)), raw=False)

    recent_high_idx = int(close.idxmax())
    recent_low_idx = int(close.idxmin())

    prev_high_slice = close.iloc[:max(recent_high_idx - 3, 1)]
    prev_low_slice = close.iloc[:max(recent_low_idx - 3, 1)]
    if prev_high_slice.empty or prev_low_slice.empty:
        return {"bullish": False, "bearish": False}

    prev_high_idx = int(prev_high_slice.idxmax())
    prev_low_idx = int(prev_low_slice.idxmin())

    bearish = (
        float(close.iloc[recent_high_idx]) > float(close.iloc[prev_high_idx])
        and float(rsi_series.iloc[recent_high_idx]) < float(rsi_series.iloc[prev_high_idx])
    )
    bullish = (
        float(close.iloc[recent_low_idx]) < float(close.iloc[prev_low_idx])
        and float(rsi_series.iloc[recent_low_idx]) > float(rsi_series.iloc[prev_low_idx])
    )
    return {"bullish": bullish, "bearish": bearish}


def is_bullish_engulfing(df: pd.DataFrame) -> bool:
    if len(df) < 2:
        return False
    prev_candle = df.iloc[-2]
    last_candle = df.iloc[-1]
    prev_bearish = float(prev_candle["close"]) < float(prev_candle["open"])
    last_bullish = float(last_candle["close"]) > float(last_candle["open"])
    return prev_bearish and last_bullish and float(last_candle["close"]) >= float(prev_candle["open"]) and float(last_candle["open"]) <= float(prev_candle["close"])


def is_bearish_engulfing(df: pd.DataFrame) -> bool:
    if len(df) < 2:
        return False
    prev_candle = df.iloc[-2]
    last_candle = df.iloc[-1]
    prev_bullish = float(prev_candle["close"]) > float(prev_candle["open"])
    last_bearish = float(last_candle["close"]) < float(last_candle["open"])
    return prev_bullish and last_bearish and float(last_candle["open"]) >= float(prev_candle["close"]) and float(last_candle["close"]) <= float(prev_candle["open"])


def is_pin_bar(df: pd.DataFrame, direction: str) -> bool:
    if df.empty:
        return False
    candle = df.iloc[-1]
    high = float(candle["high"])
    low = float(candle["low"])
    open_price = float(candle["open"])
    close_price = float(candle["close"])
    body = abs(close_price - open_price)
    full_range = max(high - low, 1e-9)
    upper_wick = high - max(open_price, close_price)
    lower_wick = min(open_price, close_price) - low
    if direction == "LONG":
        return lower_wick >= body * 2 and lower_wick / full_range >= 0.5
    return upper_wick >= body * 2 and upper_wick / full_range >= 0.5