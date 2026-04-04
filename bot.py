"""
bot.py — Hourly cron entrypoint
================================
Triggered by GitHub Actions every hour at :05 UTC.
Runs as a plain Python script — no cloud functions framework needed.

Flow:
  1. Fetch latest 60 closed 1H candles from Binance
  2. Compute S6g9 signal
  3. Check existing position for SL / TP / timeout
  4. If flat and signal → risk checks → place GTX entry + SL + TP bracket
  5. Snapshot equity to state/equity.json
  6. Exit (GitHub Actions commits state/ back to repo)

Local test (paper mode):
  python bot.py
"""

import logging
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SYMBOL          = os.getenv("SYMBOL", "BTCUSDT")
LEVERAGE        = int(os.getenv("LEVERAGE", "5"))
INITIAL_CAPITAL = float(os.getenv("INITIAL_CAPITAL", "1000"))
MAX_HOLD_BARS   = 48   # 48 × 1H = 2 days

from data_feed      import fetch_candles
from strategy       import compute_signal, compute_atr
from order_manager  import (
    get_balance, set_leverage, cancel_all_orders,
    place_gtx_entry, place_stop_market, place_take_profit,
    close_position_market, PAPER,
)
from position_store import (
    get_position, save_position, clear_position,
    log_trade, snapshot_equity,
)
from risk_guard import check_all, size_order, RiskVeto


def main():
    now_utc = datetime.now(timezone.utc)
    log.info("=== Bot cycle start: %s  PAPER=%s ===", now_utc.isoformat(), PAPER)

    # ── 1. Fetch data ──────────────────────────────────────────────────────────
    df            = fetch_candles(SYMBOL, "1h", limit=60)
    current_price = float(df["close"].iloc[-1])
    atr           = compute_atr(df)
    log.info("Price=%.2f  ATR=%.2f", current_price, atr)

    # ── 2. Load state ──────────────────────────────────────────────────────────
    position  = get_position()
    equity    = get_balance() if not PAPER else _paper_equity()
    daily_pnl = _daily_pnl()

    # ── 3. Manage existing position ────────────────────────────────────────────
    if position:
        position = _manage_position(position, current_price, atr, now_utc, equity)

    # ── 4. Compute signal ──────────────────────────────────────────────────────
    signal = compute_signal(df)
    log.info("Signal: %s", {1: "LONG", -1: "SHORT", 0: "FLAT"}.get(signal, "?"))

    # ── 5. Open new position ───────────────────────────────────────────────────
    if signal != 0 and position is None:
        try:
            check_all(equity, daily_pnl, has_open_position=False)
            _open_position(signal, current_price, atr, equity, now_utc)
        except RiskVeto as e:
            log.warning("Risk veto: %s", e)

    # ── 6. Snapshot equity ─────────────────────────────────────────────────────
    snapshot_equity(equity)
    log.info("=== Cycle complete. Equity=%.2f ===", equity)


# ── Position helpers ───────────────────────────────────────────────────────────

def _open_position(signal, price, atr, equity, now_utc):
    direction = "BUY"  if signal ==  1 else "SELL"
    close_dir = "SELL" if signal ==  1 else "BUY"
    sizing    = size_order(equity, atr, price, confidence=1.0)
    qty       = sizing["qty"]
    sl        = round(price - sizing["sl_dist"] * signal, 2)
    tp        = round(price + sizing["tp_dist"] * signal, 2)

    log.info("Opening %s: qty=%.4f entry=%.2f sl=%.2f tp=%.2f margin=%.1f%%",
             direction, qty, price, sl, tp, sizing["margin_pct"])

    set_leverage(SYMBOL, LEVERAGE)
    place_gtx_entry(SYMBOL, direction, qty, price)
    place_stop_market(SYMBOL, close_dir, qty, sl)
    place_take_profit(SYMBOL, close_dir, qty, tp)

    save_position({
        "direction":   signal,
        "entry_price": price,
        "qty":         qty,
        "notional":    sizing["notional"],
        "sl":          sl,
        "tp":          tp,
        "atr":         atr,
        "margin_pct":  sizing["margin_pct"],
        "bars_held":   0,
        "entry_time":  now_utc.isoformat(),
    })


def _manage_position(position, price, atr, now_utc, equity):
    """Check SL / TP / timeout. Returns None if position closed, else updated position."""
    direction = position["direction"]
    entry     = position["entry_price"]
    sl        = position["sl"]
    tp        = position["tp"]
    qty       = position["qty"]
    bars_held = position.get("bars_held", 0) + 1
    close_dir = "SELL" if direction == 1 else "BUY"

    exit_reason = None
    exit_price  = None

    if direction == 1:
        if price <= sl: exit_reason, exit_price = "SL", sl
        if price >= tp: exit_reason, exit_price = "TP", tp
    else:
        if price >= sl: exit_reason, exit_price = "SL", sl
        if price <= tp: exit_reason, exit_price = "TP", tp

    if bars_held >= MAX_HOLD_BARS and not exit_reason:
        exit_reason, exit_price = "TIMEOUT", price

    if exit_reason:
        log.info("Closing: reason=%s exit=%.2f bars=%d", exit_reason, exit_price, bars_held)
        cancel_all_orders(SYMBOL)
        close_position_market(SYMBOL, close_dir, qty)

        maker_fee = 0.00025
        taker_fee = 0.00075
        fee_rate  = maker_fee if exit_reason == "TP" else taker_fee
        raw_pnl   = (exit_price - entry) * direction * qty
        net_pnl   = raw_pnl - abs(qty * exit_price) * fee_rate

        log_trade({
            "entry_time":  position["entry_time"],
            "exit_time":   now_utc.isoformat(),
            "direction":   "LONG" if direction == 1 else "SHORT",
            "entry_price": entry,
            "exit_price":  exit_price,
            "qty":         qty,
            "notional":    position["notional"],
            "sl":          sl,
            "tp":          tp,
            "pnl":         round(net_pnl, 4),
            "pnl_pct":     round(net_pnl / position["notional"] * 100, 3),
            "exit_reason": exit_reason,
            "bars_held":   bars_held,
        })
        clear_position()
        return None

    position["bars_held"] = bars_held
    save_position(position)
    log.info("Position still open: %s entry=%.2f bars=%d/%d",
             "LONG" if direction == 1 else "SHORT", entry, bars_held, MAX_HOLD_BARS)
    return position


# ── Paper mode helpers ─────────────────────────────────────────────────────────

def _paper_equity() -> float:
    """Derive paper equity from initial capital + sum of logged trade PnLs."""
    from position_store import get_trades
    trades = get_trades()
    return INITIAL_CAPITAL + sum(t.get("pnl", 0) for t in trades)


def _daily_pnl() -> float:
    """Sum of today's (UTC) closed trade PnLs."""
    from position_store import get_trades
    today = datetime.now(timezone.utc).date().isoformat()
    return sum(
        t.get("pnl", 0) for t in get_trades()
        if str(t.get("exit_time", "")).startswith(today)
    )


if __name__ == "__main__":
    main()
