"""
Live 1H OHLCV data feed.
Fetches from Binance public API (no key needed) as the primary source —
Binance has the most reliable 1H BTC/USDT data and zero rate-limit issues
for 60 candles/hour. Falls back to a second attempt on timeout.
"""

import logging
import time

import requests
import pandas as pd

log = logging.getLogger(__name__)

BINANCE_URL = "https://api.binance.com/api/v3/klines"
SYMBOL      = "BTCUSDT"
INTERVAL    = "1h"
WARMUP_BARS = 60   # enough for ATR14, ADX14, EMA20, EMA200


def fetch_candles(symbol: str = SYMBOL,
                  interval: str = INTERVAL,
                  limit: int = WARMUP_BARS) -> pd.DataFrame:
    """
    Fetch the latest `limit` closed 1H candles from Binance.
    Returns a DataFrame with columns: open, high, low, close, volume
    Index: UTC DatetimeIndex (bar open time), sorted ascending.
    The LAST row is the most recently CLOSED bar (current bar excluded).
    """
    for attempt in range(3):
        try:
            resp = requests.get(
                BINANCE_URL,
                params={"symbol": symbol, "interval": interval, "limit": limit + 1},
                timeout=10,
            )
            resp.raise_for_status()
            raw = resp.json()
            break
        except requests.RequestException as e:
            log.warning("Binance fetch attempt %d failed: %s", attempt + 1, e)
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)

    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore",
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df.set_index("open_time", inplace=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    # Drop the current (still-open) bar — only trade on closed bars
    df = df.iloc[:-1]

    log.info("Fetched %d closed 1H candles. Latest: %s close=%.2f",
             len(df), df.index[-1], df['close'].iloc[-1])
    return df[["open", "high", "low", "close", "volume"]]
