"""
Quotex Live API v2  (FastAPI + pyquotex)
 - Live tick stream -> real-time running M1 candle (open/high/low/close)
 - Closed candles + ticks saved in SQLite (RETENTION_MINUTES, default 10)
 - Live payouts for all markets, REST + WebSocket
Run: python -m uvicorn app:app --host 127.0.0.1 --port 8000
"""
import asyncio
import inspect
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from starlette.requests import HTTPConnection

from engine import PERIOD, CandleEngine

load_dotenv()

# --- compat patch: pyquotex expects httpx-style `reason_phrase` on responses ---
def _patch_reason_phrase() -> None:
    for mod, name in (("curl_cffi.requests", "Response"), ("requests", "Response"),
                      ("requests.models", "Response")):
        try:
            cls = getattr(__import__(mod, fromlist=[name]), name)
            if not hasattr(cls, "reason_phrase"):
                cls.reason_phrase = property(lambda self: getattr(self, "reason", "") or "")
        except Exception:  # noqa: BLE001
            pass


_patch_reason_phrase()

EMAIL = os.getenv("email", "")
PASSWORD = os.getenv("password", "")
TZ_SETTING = os.getenv("timezone", "UTC")
RETENTION_MIN = int(os.getenv("RETENTION_MINUTES", "10"))
SAVE_TICKS = os.getenv("SAVE_TICKS", "true").lower() == "true"
STREAM_SYMBOLS = os.getenv("STREAM_SYMBOLS", "ALL")  # ALL or EURUSD_otc,GBPUSD_otc
MAX_STREAMS = int(os.getenv("MAX_STREAMS", "30"))
PAYOUT_REFRESH_SEC = int(os.getenv("PAYOUT_REFRESH_SEC", "20"))
API_KEY = os.getenv("API_KEY", "")  # set korle shob request-e key lagbe

engine = CandleEngine("quotex_data.db", RETENTION_MIN, SAVE_TICKS)
client: Any = None
lock = asyncio.Lock()  # serialises calls that talk to the broker (connect/history)
subscribed: Set[str] = set()
state: Dict[str, Any] = {"connected": False, "payouts": {}, "updated": None}


# ---------------------------------------------------------------- helpers
async def maybe_await(x):
    return await x if inspect.isawaitable(x) else x


def parse_tz(name: Optional[str]) -> timezone:
    m = re.search(r"([+-]\d+(?:\.\d+)?)", name or "")
    return timezone(timedelta(hours=float(m.group(1)))) if m else timezone.utc


def out_candle(c: Dict[str, Any], tz: timezone, running: bool = False) -> Dict[str, Any]:
    return {
        "time": datetime.fromtimestamp(c["ts"], tz).strftime("%Y-%m-%d %H:%M:%S"),
        "timestamp": c["ts"],
        "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"],
        "direction": "UP" if c["close"] > c["open"] else "DOWN" if c["close"] < c["open"] else "FLAT",
        "ticks": c.get("ticks", 0),
        "payout": c.get("payout"),
        "is_running": running,
    }


def norm_history_candle(c: Any) -> Optional[Dict[str, Any]]:
    try:
        if isinstance(c, dict):
            t, o, h, l, cl = c["time"], c["open"], c["high"], c["low"], c["close"]
        else:
            t, o, cl, h, l = c[0], c[1], c[2], c[3], c[4]
        t = float(t)
        if t > 1e11:
            t /= 1000.0
        return {"ts": int(t // PERIOD * PERIOD), "open": float(o), "high": float(h),
                "low": float(l), "close": float(cl)}
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def parse_tick(it: Any):
    """Returns (time, price) from dict or list tick formats."""
    try:
        if isinstance(it, dict):
            return float(it["time"]), float(it.get("price", it.get("close")))
        if isinstance(it, (list, tuple)):
            if len(it) >= 3 and isinstance(it[0], str):
                return float(it[1]), float(it[2])
            return float(it[0]), float(it[1])
    except (KeyError, TypeError, ValueError):
        pass
    return None, None


def norm_payouts(raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out = {}
    for sym, v in (raw or {}).items():
        if not isinstance(v, dict):
            continue
        profit = v.get("profit") or {}
        payout = profit.get("1M") or v.get("payment") or v.get("turbo_payment")
        out[sym] = {"symbol": sym, "payout_percent": payout,
                    "is_open": bool(v.get("open", False)),
                    "is_otc": sym.lower().endswith("_otc")}
    return out


# ------------------------------------------------------------ broker side
async def connect() -> None:
    global client
    from pyquotex.stable_api import Quotex  # imported lazily
    client = Quotex(email=EMAIL, password=PASSWORD, lang="en")
    ok, reason = await client.connect()
    state["connected"] = bool(ok)
    if not ok:
        raise RuntimeError(f"Connect failed: {reason}")
    subscribed.clear()  # new session -> must resubscribe


async def backfill(sym: str) -> None:
    """Load last RETENTION minutes from broker so history exists immediately / after gaps."""
    minutes = RETENTION_MIN + 2
    async with lock:
        raw = await client.get_candles(sym, time.time(), minutes * PERIOD, PERIOD) or []
    candles = [c for c in (norm_history_candle(x) for x in raw) if c]
    engine.save_history(sym, candles)


async def subscribe(sym: str) -> None:
    if sym in subscribed:
        return
    await maybe_await(client.start_realtime_price(sym, PERIOD))
    subscribed.add(sym)
    try:
        await backfill(sym)
    except Exception as e:  # noqa: BLE001
        print(f"[backfill] {sym}: {e}")


def wanted_symbols() -> list:
    pay = state["payouts"]
    if STREAM_SYMBOLS.strip().upper() == "ALL":
        opens = [s for s, v in pay.items() if v["is_open"]]
        opens.sort(key=lambda s: pay[s]["payout_percent"] or 0, reverse=True)
        return opens[:MAX_STREAMS]
    return [s.strip() for s in STREAM_SYMBOLS.split(",") if s.strip()]


async def keeper() -> None:
    """Connection + payouts + subscriptions + auto-reconnect."""
    while True:
        try:
            if not state["connected"] or not await client.check_connect():
                await connect()
            state["payouts"] = norm_payouts(client.get_payment())
            state["updated"] = int(time.time())
            for s, v in state["payouts"].items():
                engine.payouts[s] = v["payout_percent"]
            for sym in wanted_symbols():
                await subscribe(sym)
        except Exception as e:  # noqa: BLE001
            state["connected"] = False
            import traceback
            print(f"[keeper] {type(e).__name__}: {e} -> retry in 10s")
            traceback.print_exc()
            await asyncio.sleep(10)
            continue
        await asyncio.sleep(PAYOUT_REFRESH_SEC)


async def tick_loop() -> None:
    """Pull new ticks of every subscribed symbol and feed the candle engine."""
    last_purge = 0.0
    while True:
        if state["connected"]:
            for sym in list(subscribed):
                try:
                    items = await maybe_await(client.get_realtime_price(sym))
                    last = engine.last_tick_ts.get(sym, 0.0)
                    fresh = []
                    for it in reversed(items or []):
                        t, p = parse_tick(it)
                        if t is None:
                            continue
                        if (t / 1000.0 if t > 1e11 else t) <= last:
                            break
                        fresh.append((t, p))
                    for t, p in reversed(fresh):
                        engine.on_tick(sym, t, p)
                except Exception as e:  # noqa: BLE001
                    print(f"[tick] {sym}: {e}")
        engine.roll()
        engine.flush_ticks()
        if time.time() - last_purge > 30:
            engine.purge()
            last_purge = time.time()
        await asyncio.sleep(0.25)


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not EMAIL or not PASSWORD:
        print("!! .env te email/password dao")
    tasks = [asyncio.create_task(keeper()), asyncio.create_task(tick_loop())]
    yield
    for t in tasks:
        t.cancel()


async def check_key(conn: HTTPConnection) -> None:
    """If API_KEY is set: require ?key=... or header X-API-Key (REST + WebSocket)."""
    if not API_KEY or conn.url.path == "/health":
        return
    given = conn.query_params.get("key") or conn.headers.get("x-api-key")
    if given != API_KEY:
        raise HTTPException(401, "Invalid or missing API key")


app = FastAPI(title="Quotex Live API", lifespan=lifespan, dependencies=[Depends(check_key)])


@app.get("/health")
async def health():
    return {"ok": True, "connected": state["connected"]}


def need_conn():
    if not state["connected"]:
        raise HTTPException(503, "Not connected to Quotex yet, retry in few seconds")


async def ensure_symbol(symbol: str) -> None:
    if symbol not in subscribed:
        if symbol not in state["payouts"]:
            raise HTTPException(404, f"Unknown symbol {symbol}")
        await subscribe(symbol)


# --------------------------------------------------------------- endpoints
@app.get("/")
async def root():
    return {"status": "ok", "connected": state["connected"], "streaming": sorted(subscribed),
            "retention_minutes": RETENTION_MIN,
            "endpoints": ["/api/v1/markets", "/api/v1/candles", "/api/v1/candles/all",
                          "/api/v1/live", "/api/v1/ticks", "ws:/ws/live?symbol="]}


@app.get("/api/v1/markets")
async def markets(only_open: bool = False, otc: Optional[bool] = None,
                  min_payout: int = Query(0, ge=0, le=100)):
    if not state["payouts"]:
        raise HTTPException(503, "Payouts not ready yet")
    rows = list(state["payouts"].values())
    if only_open:
        rows = [r for r in rows if r["is_open"]]
    if otc is not None:
        rows = [r for r in rows if r["is_otc"] == otc]
    rows = [r for r in rows if (r["payout_percent"] or 0) >= min_payout]
    rows.sort(key=lambda r: r["payout_percent"] or 0, reverse=True)
    return {"updated": state["updated"], "total_assets": len(rows), "data": rows}


@app.get("/api/v1/candles")
async def candles(symbol: str, limit: int = Query(100, ge=1, le=1000),
                  include_running: bool = True, tz: Optional[str] = Query(None, alias="timezone")):
    """Saved closed candles (last RETENTION_MINUTES) + live running candle."""
    need_conn()
    await ensure_symbol(symbol)
    z = parse_tz(tz or TZ_SETTING)
    data = [out_candle(c, z) for c in engine.get_closed(symbol, limit)]
    run = engine.get_running(symbol) if include_running else None
    if run:
        data.append(out_candle(run, z, running=True))
    return {"symbol": symbol, "timeframe": "1m", "retention_minutes": RETENTION_MIN,
            "candle_count": len(data), "candles": data}


@app.get("/api/v1/candles/all")
async def candles_all(otc: Optional[bool] = None, limit: int = Query(10, ge=1, le=1000),
                      tz: Optional[str] = Query(None, alias="timezone")):
    """Saved + running candles for every streamed market (served from memory/DB, fast)."""
    need_conn()
    z = parse_tz(tz or TZ_SETTING)
    out = {}
    for sym in sorted(subscribed):
        if otc is not None and sym.lower().endswith("_otc") != otc:
            continue
        data = [out_candle(c, z) for c in engine.get_closed(sym, limit)]
        run = engine.get_running(sym)
        if run:
            data.append(out_candle(run, z, running=True))
        out[sym] = {"payout_percent": state["payouts"].get(sym, {}).get("payout_percent"),
                    "candles": data}
    return {"market_count": len(out), "data": out}


@app.get("/api/v1/live")
async def live(symbol: str, tz: Optional[str] = Query(None, alias="timezone")):
    need_conn()
    await ensure_symbol(symbol)
    z = parse_tz(tz or TZ_SETTING)
    run = engine.get_running(symbol)
    closed = engine.get_closed(symbol, 1)
    return {"symbol": symbol,
            "payout_percent": state["payouts"].get(symbol, {}).get("payout_percent"),
            "last_price": engine.last_price.get(symbol),
            "last_tick_age_sec": round(time.time() - engine.last_tick_ts[symbol], 2)
            if symbol in engine.last_tick_ts else None,
            "running_candle": out_candle(run, z, True) if run else None,
            "last_closed_candle": out_candle(closed[-1], z) if closed else None}


@app.get("/api/v1/ticks")
async def ticks(symbol: str, seconds: int = Query(60, ge=1, le=RETENTION_MIN * 60),
                limit: int = Query(5000, ge=1, le=50000)):
    """Raw saved ticks of the last N seconds."""
    if not SAVE_TICKS:
        raise HTTPException(400, "SAVE_TICKS=false in .env")
    rows = engine.get_ticks(symbol, seconds, limit)
    return {"symbol": symbol, "count": len(rows), "ticks": rows}


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket, symbol: str, tz: Optional[str] = Query(None, alias="timezone")):
    """Pushes the running candle every time it changes."""
    await ws.accept()
    z = parse_tz(tz or TZ_SETTING)
    try:
        await ensure_symbol(symbol)
        last = None
        while True:
            run = engine.get_running(symbol)
            snap = run and (run["ts"], run["high"], run["low"], run["close"])
            if run and snap != last:
                await ws.send_json({"symbol": symbol, "running_candle": out_candle(run, z, True)})
                last = snap
            await asyncio.sleep(0.2)
    except (WebSocketDisconnect, HTTPException):
        pass
