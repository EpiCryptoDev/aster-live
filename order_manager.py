"""
Aster DEX order manager.
=========================
Wraps the Aster perpetual futures REST API.
All entries use GTX (post-only maker) limit orders.
SL uses STOP_MARKET (taker). TP uses TAKE_PROFIT (limit maker).

Fee model:
  Entry GTX:      0.025% maker
  TP limit:       0.025% maker
  SL stop-market: 0.075% taker
"""

import hashlib
import hmac
import logging
import os
import time
from urllib.parse import urlencode

import requests

log = logging.getLogger(__name__)

BASE_URL   = os.getenv("ASTER_BASE_URL", "https://sapi.asterdex.com")
API_KEY    = os.getenv("ASTER_API_KEY", "")
API_SECRET = os.getenv("ASTER_API_SECRET", "")
PAPER      = os.getenv("PAPER_TRADE", "true").lower() == "true"


# ── Auth ───────────────────────────────────────────────────────────────────────

def _sign(params: dict) -> str:
    query = urlencode(params)
    return hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()


def _headers() -> dict:
    return {"X-MBX-APIKEY": API_KEY, "Content-Type": "application/x-www-form-urlencoded"}


def _request(method: str, path: str, params: dict) -> dict:
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    url = f"{BASE_URL}{path}"
    if PAPER:
        log.info("[PAPER] %s %s %s", method, path, params)
        return {"orderId": "PAPER", "status": "NEW", "paper": True}
    resp = requests.request(method, url, headers=_headers(), data=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


# ── Account info ───────────────────────────────────────────────────────────────

def get_balance() -> float:
    """Return USDT available balance."""
    if PAPER:
        return float(os.getenv("INITIAL_CAPITAL", "1000"))
    data = _request("GET", "/fapi/v2/account", {})
    for asset in data.get("assets", []):
        if asset["asset"] == "USDT":
            return float(asset["availableBalance"])
    return 0.0


def get_position(symbol: str) -> dict:
    """Return current position info for symbol."""
    if PAPER:
        return {}
    data = _request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
    for p in data:
        if p["symbol"] == symbol:
            return p
    return {}


def set_leverage(symbol: str, leverage: int):
    """Set leverage for symbol (call once on startup)."""
    _request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})


# ── Order placement ────────────────────────────────────────────────────────────

def place_gtx_entry(symbol: str, side: str, qty: float, price: float) -> dict:
    """
    Place a GTX (post-only maker) limit entry order.
    side: "BUY" or "SELL"
    qty:  BTC quantity
    price: limit price (entry)
    """
    params = {
        "symbol":           symbol,
        "side":             side,
        "type":             "LIMIT",
        "timeInForce":      "GTX",      # Good-Till-Crossing = post-only maker
        "quantity":         f"{qty:.4f}",
        "price":            f"{price:.2f}",
        "reduceOnly":       "false",
    }
    result = _request("POST", "/fapi/v1/order", params)
    log.info("GTX entry placed: side=%s qty=%.4f price=%.2f → %s",
             side, qty, price, result.get("orderId"))
    return result


def place_stop_market(symbol: str, side: str, qty: float, stop_price: float) -> dict:
    """
    Place a STOP_MARKET SL order.
    side: closing side — "SELL" for long SL, "BUY" for short SL
    """
    params = {
        "symbol":       symbol,
        "side":         side,
        "type":         "STOP_MARKET",
        "quantity":     f"{qty:.4f}",
        "stopPrice":    f"{stop_price:.2f}",
        "reduceOnly":   "true",
    }
    result = _request("POST", "/fapi/v1/order", params)
    log.info("SL placed: side=%s qty=%.4f stopPrice=%.2f → %s",
             side, qty, stop_price, result.get("orderId"))
    return result


def place_take_profit(symbol: str, side: str, qty: float, tp_price: float) -> dict:
    """
    Place a TAKE_PROFIT limit (maker) TP order.
    side: closing side — "SELL" for long TP, "BUY" for short TP
    """
    params = {
        "symbol":       symbol,
        "side":         side,
        "type":         "TAKE_PROFIT",
        "timeInForce":  "GTX",
        "quantity":     f"{qty:.4f}",
        "price":        f"{tp_price:.2f}",
        "stopPrice":    f"{tp_price:.2f}",
        "reduceOnly":   "true",
    }
    result = _request("POST", "/fapi/v1/order", params)
    log.info("TP placed: side=%s qty=%.4f tp=%.2f → %s",
             side, qty, tp_price, result.get("orderId"))
    return result


def cancel_all_orders(symbol: str):
    """Cancel all open orders for symbol (e.g. orphaned SL/TP after timeout)."""
    _request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})
    log.info("All open orders cancelled for %s", symbol)


def get_open_orders(symbol: str) -> list:
    """Return list of open orders for symbol."""
    if PAPER:
        return []
    data = _request("GET", "/fapi/v1/openOrders", {"symbol": symbol})
    return data if isinstance(data, list) else []


def close_position_market(symbol: str, side: str, qty: float) -> dict:
    """
    Close position immediately at market price (taker).
    Used for timeout exits and emergency closes.
    side: "SELL" to close long, "BUY" to close short
    """
    params = {
        "symbol":       symbol,
        "side":         side,
        "type":         "MARKET",
        "quantity":     f"{qty:.4f}",
        "reduceOnly":   "true",
    }
    result = _request("POST", "/fapi/v1/order", params)
    log.info("Market close: side=%s qty=%.4f → %s", side, qty, result.get("orderId"))
    return result
