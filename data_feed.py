"""
Live 1H OHLCV data feed.
Fetches from Aster DEX public klines endpoint (no API key needed).
Falls back to Binance if Aster is unreachable.
"""

import logging
import time

import requests
import pandas as pd

log = logging.getLogger(__name__)

SYMBOL   = "BTCUSDT"
INTERVAL = "1h"
WARMUP_BARS = 60

ASTER_URL   = "https://sapi.asterdex.com/fapi/v1/klines"
BINANCE_URL = "https://api.binance.com/api/v3/klines"


def fetch_candles(symbol: str = SYMBOL,
                  interval: str = INTERVAL,
                  limit: int = WARMUP_BARS) -> pd.DataFrame:
    """
    Fetch the latest `limit` closed 1H candles.
    Tries Aster DEX first, falls back to Binance.
    Returns DataFrame with columns: open, high, low, close, volume.
    Index: UTC DatetimeIndex sorted ascending. Last row = most recent closed bar.
    """
    for source, url in [("Aster", ASTER_URL), ("Binance", BINANCE_URL)]:
        try:
            resp = requests.get(
                url,
                params={"symbol": symbol, "interval": interval, "limit": limit + 1},
                timeout=10,
            )
            resp.raise_for_status()
            raw = resp.json()
            df  = _parse(raw)
            log.info("[%s] Fetched %d closed 1H candles. Latest: %s close=%.2f",
                     source, len(df), df.index[-1], df["close"].iloc[-1])
            return df
        except Exception as e:
            log.warning("[%s] fetch failed: %s — trying next source.", source, e)

    raise RuntimeError("All data sources failed. Cannot fetch candles.")


def _parse(raw: list) -> pd.DataFrame:
    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore",
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df.set_index("open_time", inplace=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    # Drop the still-open current bar
    return df.iloc[:-1][["open", "high", "low", "close", "volume"]]
