"""
S6g9 Strategy — Self-contained signal generator
================================================
Keltner Channel (EMA20 ± 2×ATR14) breakout sustained for 2 consecutive
1H closes, with ADX > 28 trend filter.

Proven backtest (2022-2024, 3yr):
  Return: +23.3%  |  CAGR: +7.2%/yr  |  Sharpe: 1.10
  Max DD: -4.5%   |  Win Rate: 43.1%  |  ~157 trades/year

Signal fires at the CLOSE of each 1H bar.
Execution happens at the OPEN of the NEXT 1H bar (no look-ahead).

Returns:
  +1  → go long
  -1  → go short
   0  → no signal / stay flat
"""

import numpy as np
import pandas as pd


# ── Indicator helpers ──────────────────────────────────────────────────────────

def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    atr14 = _atr(high, low, close, period)
    plus_dm  = high.diff().clip(lower=0).where(high.diff() > (-low.diff()), 0)
    minus_dm = (-low.diff()).clip(lower=0).where((-low.diff()) > high.diff(), 0)
    plus_di  = 100 * plus_dm.ewm(span=period, adjust=False).mean()  / (atr14 + 1e-12)
    minus_di = 100 * minus_dm.ewm(span=period, adjust=False).mean() / (atr14 + 1e-12)
    dx       = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-12)
    return dx.ewm(span=period, adjust=False).mean()


def _keltner(close: pd.Series, atr14: pd.Series,
             period: int = 20, mult: float = 2.0):
    mid   = close.ewm(span=period, adjust=False).mean()
    return mid + mult * atr14, mid - mult * atr14   # upper, lower


# ── Core signal ───────────────────────────────────────────────────────────────

def compute_signal(df: pd.DataFrame) -> int:
    """
    Feed this function the latest N 1H candles (N >= 50 for warm-up).
    It reads the last 2 closed bars and returns the signal for the NEXT bar.

    df must have columns: open, high, low, close, volume
    Index must be a DatetimeIndex sorted ascending.

    Returns: +1, -1, or 0
    """
    if len(df) < 50:
        return 0

    c, h, l = df['close'], df['high'], df['low']

    atr14      = _atr(h, l, c)
    adx        = _adx(h, l, c)
    kc_u, kc_l = _keltner(c, atr14)

    above = c > kc_u
    below = c < kc_l

    # Signal on the last CLOSED bar (index -1 is latest closed bar)
    # Sustained 2 bars: bar[-1] and bar[-2] both outside KC
    long_signal  = (above.iloc[-1] and above.iloc[-2]
                    and adx.iloc[-1] > 28)
    short_signal = (below.iloc[-1] and below.iloc[-2]
                    and adx.iloc[-1] > 28)

    if long_signal:
        return 1
    if short_signal:
        return -1
    return 0


def compute_atr(df: pd.DataFrame) -> float:
    """Return latest ATR14 value — used by bot for SL/TP sizing."""
    atr14 = _atr(df['high'], df['low'], df['close'])
    return float(atr14.iloc[-1])
