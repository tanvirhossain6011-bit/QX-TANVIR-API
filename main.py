# tanvirxqx.botpro - Quotex live data API
# Developer: TANVIR | Telegram: @T4nviRhossain
import asyncio, os, time, sqlite3, logging
from datetime import datetime, timezone, timedelta

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query

try:
    from pyquotex.stable_api import Quotex
    IMPORT_ERR = None
except Exception as _e:
    Quotex = None
    IMPORT_ERR = repr(_e)

load_dotenv()
EMAIL = os.getenv("QUOTEX_EMAIL")
PASSWORD = os.getenv("QUOTEX_PASSWORD")
PORT = int(os.getenv("PORT", "8000"))
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "30"))
ONLY_ASSETS = [a.strip() for a in os.getenv("ASSETS", "").split(",") if a.strip()]
HOSTS = [h.strip() for h in os.getenv("QUOTEX_HOST", "qxbroker.com,quotex.com,qxbroker.io,quotex.io").split(",") if h.strip()]

BD = timezone(timedelta(hours=6))
META = {"api": "tanvirxqx.botpro", "developer": "TANVIR", "telegram": "@T4nviRhossain"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("tanvirxqx")

db = sqlite3.connect("data.db", check_same_thread=False)
db.execute("PRAGMA journal_mode=WAL")
db.execute("CREATE TABLE IF NOT EXISTS candles(asset TEXT, ts INTEGER, open REAL, high REAL, low REAL, close REAL, PRIMARY KEY(asset, ts))")
db.execute("CREATE TABLE IF NOT EXISTS payouts(ts INTEGER, asset TEXT, payout REAL, is_open INTEGER)")
db.execute("CREATE INDEX IF NOT EXISTS ix_pay ON payouts(asset, ts)")
db.commit()


def fmt(ts):
    return datetime.fromtimestamp(ts, BD).strftime("%Y-%m-%d %H:%M:%S")


def color(o, c):
    if c > o:
        return "green"
    if c < o:
        return "red"
    return "doji"


def save_candle(asset, ts, o, h, l, c):
    db.execute("INSERT OR REPLACE INTO candles VALUES(?,?,?,?,?,?)", (asset, ts, o, h, l, c))


LIVE = {}
PAYOUT = {}
BUCKET = {}
client = None
STATUS = {"state": "starting", "error": None}


async def connect():
    global client
    i = 0
    while True:
        host = HOSTS[i % len(HOSTS)]
        i += 1
        try:
            try:
                client = Quotex(email=EMAIL, password=PASSWORD, lang="en", host=host)
            except TypeError:
                client = Quotex(email=EMAIL, password=PASSWORD, lang="en")
            log.info("Trying host: %s", host)
            ok, reason = await client.connect()
            if ok:
                log.info("Connected to Quotex via %s", host)
                STATUS.update(state="connected", error=None, host=host)
                return
            log.warning("Connect failed (%s): %s", host, reason)
            STATUS.update(state="connect_failed", error=str(reason), host=host)
        except Exception as e:
            log.warning("Connect error (%s): %s", host, e)
            STATUS.update(state="connect_error", error=repr(e), host=host)
        await asyncio.sleep(3)


def parse_payouts(raw):
    out = {}
    for name, v in (raw or {}).items():
        try:
            p = v.get("profit", {}).get("1M") or v.get("payment") or v.get("turbo_payment")
            out[name] = {"payout": float(p) if p is not None else None, "open": bool(v.get("open", True))}
        except Exception:
            continue
    return out


async def payout_loop():
    while True:
        try:
            raw = await client.get_payment()
            data = parse_payouts(raw)
            now = time.time()
            changed = []
            for a, d in data.items():
                old = PAYOUT.get(a)
                if old is None or old["payout"] != d["payout"] or old["open"] != d["open"]:
                    PAYOUT[a] = {**d, "updated": fmt(now)}
                    changed.append((int(now), a, d["payout"], int(d["open"])))
            if changed:
                db.executemany("INSERT INTO payouts VALUES(?,?,?,?)", changed)
                db.commit()
        except Exception as e:
            log.warning("payout error: %s", e)
            raise
        await asyncio.sleep(0.5)


def on_tick(asset, ts, price):
    LIVE[asset] = {"price": price, "ts": ts}
    m = ts - ts % 60
    b = BUCKET.get(asset)
    if b is None or b[0] != m:
        if b is not None:
            save_candle(asset, *b)
        BUCKET[asset] = [m, price, price, price, price]
    else:
        b[2] = max(b[2], price)
        b[3] = min(b[3], price)
        b[4] = price


async def tick_loop(asset):
    last = 0
    await client.start_realtime_price(asset, 60)
    while True:
        prices = await client.get_realtime_price(asset)
        for t in prices or []:
            ts = float(t["time"])
            if ts > last:
                last = ts
                on_tick(asset, int(ts), float(t["price"]))
        await asyncio.sleep(0.05)


async def flush_loop():
    while True:
        await asyncio.sleep(2)
        for a, b in list(BUCKET.items()):
            save_candle(a, *b)
        db.commit()


async def backfill(asset, since_ts):
    end = int(time.time())
    step = 4 * 3600
    t = end
    while t > since_ts:
        try:
            rows = await client.get_candles(asset, t, step, 60)
            for r in rows or []:
                save_candle(asset, int(r["time"]), r["open"], r.get("high", r["open"]),
                            r.get("low", r["open"]), r["close"])
            db.commit()
        except Exception as e:
            log.warning("backfill %s: %s", asset, e)
        t -= step
        await asyncio.sleep(0.3)


async def history_loop(assets):
    since = int(time.time()) - HISTORY_DAYS * 86400
    for a in assets:
        lo, hi = db.execute("SELECT MIN(ts), MAX(ts) FROM candles WHERE asset=?", (a,)).fetchone()
        if hi:
            await backfill(a, hi - 60)
        if not lo or lo > since + 86400:
            await backfill(a, since)
    log.info("History done")


async def collector():
    while True:
        if IMPORT_ERR or not EMAIL or not PASSWORD:
            msg = IMPORT_ERR or "QUOTEX_EMAIL / QUOTEX_PASSWORD not set in Environment"
            STATUS.update(state="config_error", error=msg)
            log.warning("Config error: %s", msg)
            await asyncio.sleep(10)
            continue
        await connect()
        tasks = []
        try:
            names = list(parse_payouts(await client.get_payment()).keys())
            assets = ONLY_ASSETS or names
            log.info("Assets: %d", len(assets))
            tasks = [asyncio.create_task(payout_loop()),
                     asyncio.create_task(flush_loop()),
                     asyncio.create_task(history_loop(assets))]
            tasks += [asyncio.create_task(tick_loop(a)) for a in assets]
            await asyncio.gather(*tasks)
        except Exception as e:
            log.warning("Collector crashed, reconnecting: %s", e)
            STATUS.update(state="reconnecting", error=repr(e))
        finally:
            for t in tasks:
                t.cancel()
        await asyncio.sleep(3)


app = FastAPI(title="tanvirxqx.botpro")


@app.on_event("startup")
async def _start():
    asyncio.create_task(collector())


@app.get("/")
def root():
    return {**META, "status": STATUS["state"], "error": STATUS["error"],
            "assets_live": len(LIVE), "time_utc6": fmt(time.time())}


@app.get("/assets")
def assets():
    return {**META, "assets": [{"asset": a, **d} for a, d in PAYOUT.items()]}


@app.get("/tick/{asset}")
def tick(asset: str):
    d = LIVE.get(asset)
    return {**META, "asset": asset, **(d or {}), "time": fmt(d["ts"]) if d else None}


@app.get("/payouts")
def payouts():
    return {**META, "time": fmt(time.time()), "payouts": PAYOUT}


@app.get("/payouts/history")
def payouts_history(asset: str, hours: int = 24):
    since = int(time.time()) - hours * 3600
    rows = db.execute("SELECT ts, payout FROM payouts WHERE asset=? AND ts>=? ORDER BY ts",
                      (asset, since)).fetchall()
    return {**META, "asset": asset, "data": [{"time": fmt(t), "payout": p} for t, p in rows]}


@app.get("/candles")
def candles(asset: str, tf: int = Query(1, description="minutes: 1,2,5,10,15,30,60,120,240"),
            limit: int = 200):
    size = tf * 60
    rows = db.execute("SELECT ts, open, high, low, close FROM candles WHERE asset=? ORDER BY ts",
                      (asset,)).fetchall()
    b = BUCKET.get(asset)
    if b:
        rows = [r for r in rows if r[0] != b[0]] + [tuple(b)]
    out, cur = [], None
    for ts, o, h, l, c in rows:
        k = ts - ts % size
        if cur is None or cur[0] != k:
            if cur:
                out.append(cur)
            cur = [k, o, h, l, c]
        else:
            cur[2] = max(cur[2], h)
            cur[3] = min(cur[3], l)
            cur[4] = c
    if cur:
        out.append(cur)
    out = out[-limit:]
    return {**META, "asset": asset, "tf": tf, "candles": [
        {"ts": k, "time": fmt(k), "open": o, "high": h, "low": l, "close": c, "color": color(o, c)}
        for k, o, h, l, c in out]}


@app.websocket("/live")
async def live(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json({**META, "time": fmt(time.time()), "ticks": LIVE, "payouts": PAYOUT})
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
