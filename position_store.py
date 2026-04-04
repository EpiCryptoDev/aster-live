"""
Position state store — JSON files in the repo.
================================================
No external database. State is read/written to:
  state/position.json  — current open position (or empty)
  state/trades.json    — completed trade log (array)
  state/equity.json    — hourly equity snapshots (array)

GitHub Actions commits these files back to the repo after each run,
which also keeps the repo active (prevents the 60-day inactivity disable).
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# Resolve paths relative to this file so it works both locally and in CI
_ROOT       = Path(__file__).resolve().parent
STATE_DIR   = _ROOT / "state"
POS_FILE    = STATE_DIR / "position.json"
TRADES_FILE = STATE_DIR / "trades.json"
EQUITY_FILE = STATE_DIR / "equity.json"


def _read(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("Failed to read %s: %s — using default", path.name, e)
        return default


def _write(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


# ── Position state ─────────────────────────────────────────────────────────────

def get_position() -> Optional[dict]:
    data = _read(POS_FILE, {})
    return data if data.get("open") else None


def save_position(position: dict):
    _write(POS_FILE, {**position, "open": True,
                      "updated_at": datetime.now(timezone.utc).isoformat()})
    log.info("Position saved: dir=%s entry=%.2f",
             "LONG" if position.get("direction") == 1 else "SHORT",
             position.get("entry_price", 0))


def clear_position():
    _write(POS_FILE, {"open": False,
                      "cleared_at": datetime.now(timezone.utc).isoformat()})
    log.info("Position cleared.")


# ── Trade log ──────────────────────────────────────────────────────────────────

def log_trade(trade: dict):
    trades = _read(TRADES_FILE, [])
    trade["logged_at"] = datetime.now(timezone.utc).isoformat()
    trades.append(trade)
    _write(TRADES_FILE, trades)
    log.info("Trade logged: pnl=%.4f reason=%s",
             trade.get("pnl", 0), trade.get("exit_reason"))


def get_trades(limit: int = 100) -> list:
    trades = _read(TRADES_FILE, [])
    return trades[-limit:]


# ── Equity snapshots ───────────────────────────────────────────────────────────

def snapshot_equity(equity: float):
    history = _read(EQUITY_FILE, [])
    history.append({
        "equity": round(equity, 4),
        "ts":     datetime.now(timezone.utc).isoformat(),
    })
    # Keep last 2160 points (90 days × 24h)
    _write(EQUITY_FILE, history[-2160:])


def get_equity_history() -> list:
    return _read(EQUITY_FILE, [])
