"""
Live 1H OHLCV data feed.
Primary:  Bybit public klines (no key, no geo-restriction)
Fallback: Kraken public OHLC
Both work from GitHub Actions (US servers).
"""

import logging
import time

import requests
import pandas as pd

log = logging.getLogger(__name__)

WARMUP_BARS = 60


def fetch_candles(symbol: str = "BTCUSDT",
                  interval: str = "1h",
                  limit: int = WARMUP_BARS) -> pd.DataFrame:
    """
    Fetch the latest `limit` closed 1H candles.
    Returns DataFrame: open, high, low, close, volume — UTC index ascending.
    Last row = most recently closed bar.
    """
    for fn in [_fetch_bybit, _fetch_kraken]:
        try:
            df = fn(limit + 1)   # +1 so we can drop the still-open bar
            df = df.iloc[:-1]    # drop current (open) bar
            log.info("Fetched %d closed 1H candles. Latest: %s  close=%.2f",
                     len(df), df.index[-1], df["close"].iloc[-1])
            return df
        except Exception as e:
            log.warning("Source %s failed: %s — trying next.", fn.__name__, e)

    raise RuntimeError("All data sources failed. Cannot fetch candles.")


# ── Bybit ──────────────────────────────────────────────────────────────────────

def _fetch_bybit(limit: int) -> pd.DataFrame:
    resp = requests.get(
        "https://api.bybit.com/v5/market/kline",
        params={
            "category": "linear",
            "symbol":   "BTCUSDT",
            "interval": "60",       # 60 minutes = 1H
            "limit":    limit,
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("retCode") != 0:
        raise ValueError(f"Bybit error: {data.get('retMsg')}")

    # Bybit returns newest-first: [[startTime, open, high, low, close, volume, ...], ...]
    rows = data["result"]["list"]
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume", "turnover"])
    df["ts"]   = pd.to_datetime(df["ts"].astype(float), unit="ms", utc=True)
    df = df.set_index("ts").sort_index()   # sort ascending
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df[["open", "high", "low", "close", "volume"]]


# ── Kraken (fallback) ──────────────────────────────────────────────────────────

def _fetch_kraken(limit: int) -> pd.DataFrame:
    resp = requests.get(
        "https://api.kraken.com/0/public/OHLC",
        params={"pair": "XBTUSDT", "interval": 60},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise ValueError(f"Kraken error: {data['error']}")

    rows = data["result"]["XBTUSDT"]
    df = pd.DataFrame(rows, columns=[
        "ts", "open", "high", "low", "close", "vwap", "volume", "count"
    ])
    df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="s", utc=True)
    df = df.set_index("ts").sort_index().tail(limit)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df[["open", "high", "low", "close", "volume"]]
