"""Tick -> M1 candle engine with SQLite storage (pure stdlib, no Quotex dependency)."""
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

PERIOD = 60


class CandleEngine:
    def __init__(self, db_path: str = "quotex_data.db", retention_minutes: int = 10,
                 save_ticks: bool = True):
        self.retention = retention_minutes * 60
        self.save_ticks = save_ticks
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS candles(
            symbol TEXT, ts INTEGER, open REAL, high REAL, low REAL, close REAL,
            ticks INTEGER, payout INTEGER, PRIMARY KEY(symbol, ts))""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS ticks(
            symbol TEXT, ts REAL, price REAL)""")
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_ticks ON ticks(symbol, ts)")
        self.db.commit()
        self.running: Dict[str, Dict[str, Any]] = {}
        self.last_tick_ts: Dict[str, float] = {}
        self.last_price: Dict[str, float] = {}
        self.payouts: Dict[str, Optional[int]] = {}
        self._tick_buf: List[Tuple[str, float, float]] = []

    # ------------------------------------------------------------ ticks
    def on_tick(self, sym: str, t: float, price: float) -> None:
        if t > 1e11:  # milliseconds -> seconds
            t /= 1000.0
        minute = int(t // PERIOD * PERIOD)
        r = self.running.get(sym)
        if r is not None and minute > r["ts"]:
            self._close(sym, r)
            r = None
        if r is None:
            self.running[sym] = {"ts": minute, "open": price, "high": price,
                                 "low": price, "close": price, "ticks": 1}
        elif minute == r["ts"]:
            r["high"] = max(r["high"], price)
            r["low"] = min(r["low"], price)
            r["close"] = price
            r["ticks"] += 1
        else:
            return  # late tick from an older minute
        self.last_price[sym] = price
        self.last_tick_ts[sym] = max(t, self.last_tick_ts.get(sym, 0))
        if self.save_ticks:
            self._tick_buf.append((sym, t, price))

    def roll(self, now: Optional[float] = None) -> None:
        """Close candles whose minute ended even if no new tick arrived."""
        now = now or time.time()
        for sym, r in list(self.running.items()):
            if r["ts"] + PERIOD <= now:
                self._close(sym, r)
                del self.running[sym]

    def _close(self, sym: str, r: Dict[str, Any]) -> None:
        self.db.execute("INSERT OR REPLACE INTO candles VALUES(?,?,?,?,?,?,?,?)",
                        (sym, r["ts"], r["open"], r["high"], r["low"], r["close"],
                         r["ticks"], self.payouts.get(sym)))
        self.db.commit()

    def flush_ticks(self) -> None:
        if self._tick_buf:
            buf, self._tick_buf = self._tick_buf, []
            self.db.executemany("INSERT INTO ticks VALUES(?,?,?)", buf)
            self.db.commit()

    def purge(self, now: Optional[float] = None) -> None:
        cutoff = (now or time.time()) - self.retention
        self.db.execute("DELETE FROM candles WHERE ts < ?", (int(cutoff // PERIOD * PERIOD),))
        self.db.execute("DELETE FROM ticks WHERE ts < ?", (cutoff,))
        self.db.commit()

    # ---------------------------------------------------------- history
    def save_history(self, sym: str, candles: List[Dict[str, Any]], now: Optional[float] = None) -> None:
        """Backfill from broker history. Closed candles fill gaps only; current minute seeds the running candle."""
        now = now or time.time()
        for c in candles:
            if c["ts"] + PERIOD <= now:
                self.db.execute("INSERT OR IGNORE INTO candles VALUES(?,?,?,?,?,?,?,?)",
                                (sym, c["ts"], c["open"], c["high"], c["low"], c["close"],
                                 0, self.payouts.get(sym)))
            elif sym not in self.running:
                self.running[sym] = {"ts": c["ts"], "open": c["open"], "high": c["high"],
                                     "low": c["low"], "close": c["close"], "ticks": 0}
        self.db.commit()

    # ------------------------------------------------------------ reads
    def get_closed(self, sym: str, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.db.execute(
            "SELECT ts,open,high,low,close,ticks,payout FROM candles WHERE symbol=? "
            "ORDER BY ts DESC LIMIT ?", (sym, limit)).fetchall()
        keys = ("ts", "open", "high", "low", "close", "ticks", "payout")
        return [dict(zip(keys, r)) for r in reversed(rows)]

    def get_running(self, sym: str) -> Optional[Dict[str, Any]]:
        r = self.running.get(sym)
        if r and r["ts"] + PERIOD > time.time():
            return dict(r, payout=self.payouts.get(sym))
        return None

    def get_ticks(self, sym: str, seconds: int = 60, limit: int = 5000) -> List[Dict[str, float]]:
        rows = self.db.execute(
            "SELECT ts,price FROM ticks WHERE symbol=? AND ts>=? ORDER BY ts DESC LIMIT ?",
            (sym, time.time() - seconds, limit)).fetchall()
        return [{"ts": a, "price": b} for a, b in reversed(rows)]
