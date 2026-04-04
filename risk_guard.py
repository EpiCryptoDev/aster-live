"""
Risk guard — pre-trade safety checks.
All checks must pass before any order is placed.
"""

import logging
import os
from datetime import datetime, timezone

log = logging.getLogger(__name__)

INITIAL_CAPITAL  = float(os.getenv("INITIAL_CAPITAL", "1000"))
DAILY_LOSS_LIMIT = 0.03   # halt if daily P&L < -3%
EQUITY_FLOOR     = 0.70   # halt if equity < 70% of initial capital
MAX_MARGIN_PCT   = 0.05   # 5% max margin per trade
LEVERAGE         = int(os.getenv("LEVERAGE", "5"))
ATR_SL_MULT      = 1.5
ATR_TP_MULT      = 3.0


class RiskVeto(Exception):
    """Raised when a risk check blocks trading."""
    pass


def check_all(equity: float, daily_pnl: float, has_open_position: bool):
    """
    Run all pre-trade risk checks. Raises RiskVeto if any fail.
    Call this before opening any new position.
    """
    if has_open_position:
        raise RiskVeto("Already have an open position — max 1 at a time.")

    pct_loss = daily_pnl / INITIAL_CAPITAL
    if pct_loss < -DAILY_LOSS_LIMIT:
        raise RiskVeto(
            f"Daily loss limit hit: {pct_loss:.1%} < -{DAILY_LOSS_LIMIT:.1%}. "
            "Halting until next UTC day."
        )

    equity_pct = equity / INITIAL_CAPITAL
    if equity_pct < EQUITY_FLOOR:
        raise RiskVeto(
            f"Equity floor hit: ${equity:.2f} ({equity_pct:.1%} of initial). "
            "Manual review required before resuming."
        )

    log.info("Risk checks passed. Equity=%.2f DailyPnL=%.4f", equity, daily_pnl)


def size_order(equity: float, atr: float, entry_price: float,
               confidence: float = 1.0) -> dict:
    """
    Calculate order sizing from equity, ATR, and confidence score.

    margin = clamp(2% + 3% × confidence, 2%, 5%) of equity
    notional = margin × leverage
    qty = notional / entry_price
    sl  = entry ± ATR_SL_MULT × ATR
    tp  = entry ± ATR_TP_MULT × ATR

    Returns dict with qty, notional, margin_used, sl_dist, tp_dist.
    """
    min_pct, max_pct = 0.02, MAX_MARGIN_PCT
    margin_pct  = min(max(min_pct + (max_pct - min_pct) * confidence, min_pct), max_pct)
    margin_used = equity * margin_pct
    notional    = margin_used * LEVERAGE
    qty         = notional / entry_price
    sl_dist     = ATR_SL_MULT * atr
    tp_dist     = ATR_TP_MULT * atr

    return {
        "qty":          round(qty, 4),
        "notional":     round(notional, 2),
        "margin_used":  round(margin_used, 2),
        "margin_pct":   round(margin_pct * 100, 2),
        "sl_dist":      round(sl_dist, 2),
        "tp_dist":      round(tp_dist, 2),
    }
