"""
Personal OTC API + Quotex Collector (single file)
Run: python main.py
"""
import os, sys, subprocess, importlib

# ---------- auto install ----------
def need(mod, pip_name):
    try:
        importlib.import_module(mod)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pip_name])

need("fastapi", "fastapi")
need("uvicorn", "uvicorn")
need("pyquotex", "pyquotex")

import sqlite3, time, asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ---------- .env loader (optional) ----------
# Same folder-e ".env" file banao:
# ADMIN_KEY=...
# QX_EMAIL=...
# QX_PASS=...
if os.path.exists(".env"):
    for line in open(".env", encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# ===== DEFAULT CONFIG (.env / environment diye override kora jay) =====
KEY = os.environ.get("ADMIN_KEY", "ADMINXTANVIR")
QX_EMAIL = os.environ.get("QX_EMAIL", "ldrbot46@gmail.com")
QX_PASS = os.environ.get("QX_PASS", "Ta11223344")
TUNNEL = os.environ.get("TUNNEL", "0") == "1"   # Cloudflare public link
PORT = int(os.environ.get("SERVER_PORT") or os.environ.get("PORT") or 8000)
LOOP_DELAY = float(os.environ.get("LOOP_DELAY", "0.2"))
BACKFILL = os.environ.get("BACKFILL", "0") == "1"   # historical candles ekbar load
KEEP_DAYS = int(os.environ.get("KEEP_DAYS", "7"))   # purono tick delete

DB = "data/market.db"
clients = set()


# ---------- database ----------
def db():
    os.makedirs("data", exist_ok=True)
    c = sqlite3.connect(DB, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def setup():
    with db() as c:
        c.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS markets(
          symbol TEXT PRIMARY KEY, payout REAL, is_open INTEGER, updated REAL);
        CREATE TABLE IF NOT EXISTS ticks(
          id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, price REAL, ts REAL);
        CREATE INDEX IF NOT EXISTS ix_ticks ON ticks(symbol, id);
        CREATE TABLE IF NOT EXISTS candles(
          symbol TEXT, epoch INTEGER, open REAL, high REAL, low REAL, close REAL,
          PRIMARY KEY(symbol, epoch));
        CREATE TABLE IF NOT EXISTS payouts(
          id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, payout REAL, ts REAL);
        CREATE TABLE IF NOT EXISTS history(
          id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, is_open INTEGER, ts REAL);
        """)


def rows(q, args=()):
    with db() as c:
        return [dict(x) for x in c.execute(q, args)]


def save(data):
    now = time.time()
    with db() as c:
        for m in data.get("markets", []):
            old = c.execute("SELECT * FROM markets WHERE symbol=?", (m["symbol"],)).fetchone()
            op = int(m["is_open"])
            c.execute("""INSERT INTO markets VALUES(?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET payout=excluded.payout,
                is_open=excluded.is_open, updated=excluded.updated""",
                      (m["symbol"], m.get("payout"), op, now))
            if old is None or old["is_open"] != op:
                c.execute("INSERT INTO history(symbol,is_open,ts) VALUES(?,?,?)",
                          (m["symbol"], op, now))
            if m.get("payout") is not None and (old is None or old["payout"] != m["payout"]):
                c.execute("INSERT INTO payouts(symbol,payout,ts) VALUES(?,?,?)",
                          (m["symbol"], m["payout"], now))
        for t in data.get("ticks", []):
            s, p = t["symbol"], float(t["price"])
            ts = float(t.get("ts", now))
            ep = int(ts // 60) * 60
            c.execute("INSERT INTO ticks(symbol,price,ts) VALUES(?,?,?)", (s, p, ts))
            k = c.execute("SELECT * FROM candles WHERE symbol=? AND epoch=?", (s, ep)).fetchone()
            if k:
                c.execute("UPDATE candles SET high=?,low=?,close=? WHERE symbol=? AND epoch=?",
                          (max(k["high"], p), min(k["low"], p), p, s, ep))
            else:
                c.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", (s, ep, p, p, p, p))
        for k in data.get("candles", []):
            c.execute("INSERT OR REPLACE INTO candles VALUES(?,?,?,?,?,?)",
                      (k["symbol"], int(k["epoch"]), k["open"], k["high"], k["low"], k["close"]))


def prune():
    cutoff = time.time() - KEEP_DAYS * 86400
    with db() as c:
        c.execute("DELETE FROM ticks WHERE ts<?", (cutoff,))


# ---------- Quotex collector ----------
async def collector():
    if not (QX_EMAIL and QX_PASS):
        print("[collector] QX_EMAIL / QX_PASS set nai, collector off.")
        return
    from pyquotex.stable_api import Quotex

    while True:
        try:
            qx = Quotex(email=QX_EMAIL, password=QX_PASS, lang="en")
            ok, why = await qx.connect()
            if not ok:
                print("[collector] Login fail:", why)
                await asyncio.sleep(30)
                continue
            print("[collector] Connected")

            pay = qx.get_payment()
            names = list(pay.keys())
            print(f"[collector] {len(names)} markets")

            for n in names:
                try:
                    await qx.start_realtime_price(n, 60)
                except Exception as e:
                    print("[collector] start_realtime", n, e)

            if BACKFILL:
                print("[collector] Backfill starting...")
                for n in names:
                    try:
                        cs = await qx.get_candles(n, time.time(), 3600 * 24, 60)
                        out = [{"symbol": n, "epoch": c["time"], "open": c["open"],
                                "high": c["high"], "low": c["low"], "close": c["close"]}
                               for c in cs or []]
                        await asyncio.to_thread(save, {"candles": out})
                    except Exception as e:
                        print("[collector] backfill", n, e)
                print("[collector] Backfill done")

            last_ts = {}
            last_prune = time.time()
            while True:
                pay = qx.get_payment()
                markets, ticks = [], []
                for name, v in pay.items():
                    markets.append({"symbol": name,
                                    "payout": v.get("payment"),
                                    "is_open": bool(v.get("open"))})
                    try:
                        p = await qx.get_realtime_price(name)
                        if p:
                            last = p[-1]
                            if last_ts.get(name) != last["time"]:
                                last_ts[name] = last["time"]
                                ticks.append({"symbol": name,
                                              "price": last["price"],
                                              "ts": last["time"]})
                    except Exception:
                        pass
                await asyncio.to_thread(save, {"markets": markets, "ticks": ticks})
                await broadcast()
                if time.time() - last_prune > 3600:
                    await asyncio.to_thread(prune)
                    last_prune = time.time()
                await asyncio.sleep(LOOP_DELAY)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("[collector] error, reconnecting in 10s:", e)
            await asyncio.sleep(10)


# ---------- Cloudflare tunnel (optional) ----------
async def tunnel():
    if not TUNNEL:
        return
    import platform, urllib.request
    arch = "arm64" if platform.machine().lower() in ("aarch64", "arm64") else "amd64"
    path = "./cloudflared"
    try:
        if not os.path.exists(path):
            url = ("https://github.com/cloudflare/cloudflared/releases/latest/download/"
                   f"cloudflared-linux-{arch}")
            await asyncio.to_thread(urllib.request.urlretrieve, url, path)
            os.chmod(path, 0o755)
        proc = await asyncio.create_subprocess_exec(
            path, "tunnel", "--url", f"http://127.0.0.1:{PORT}",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        async for line in proc.stdout:
            t = line.decode(errors="ignore")
            if "trycloudflare.com" in t:
                print("[tunnel] PUBLIC LINK:", t.strip())
    except Exception as e:
        print("[tunnel] error:", e)


# ---------- web app ----------
@asynccontextmanager
async def lifespan(app):
    setup()
    tasks = [asyncio.create_task(collector()), asyncio.create_task(tunnel())]
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(title="Personal OTC API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["GET"], allow_headers=["*"])


@app.get("/")
def home():
    return HTMLResponse("""
    <html><meta name="viewport" content="width=device-width">
    <body style="background:#101827;color:white;font-family:Arial;padding:20px">
    <h2>📊 Personal OTC Dashboard</h2>
    <h3 id="status">Connecting...</h3>
    <div id="data"></div>
    <script>
    let w=new WebSocket((location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws/live");
    w.onopen=()=>status.innerText="● Connected";
    w.onmessage=e=>{
      let d=JSON.parse(e.data);
      data.innerHTML="<h3>Markets ("+d.markets.length+")</h3>"+d.markets.map(x=>
      "<p>"+x.symbol+" | "+(x.payout??"N/A")+"% | "+
      (x.is_open?"OPEN":"CLOSED")+"</p>").join("")+
      "<h3>Ticks: "+d.ticks+"</h3>";
    };
    w.onclose=()=>status.innerText="Disconnected";
    </script></body></html>
    """)


@app.post("/admin/update")
async def update(data: dict, x_admin_key: str = Header(default="")):
    if not KEY or x_admin_key != KEY:
        raise HTTPException(401, "Unauthorized")
    await asyncio.to_thread(save, data)
    await broadcast()
    return {"saved": True}


async def broadcast():
    if not clients:
        return
    d = {"markets": rows("SELECT * FROM markets"),
         "ticks": rows("SELECT COUNT(*) n FROM ticks")[0]["n"]}
    for w in list(clients):
        try:
            await w.send_json(d)
        except Exception:
            clients.discard(w)


@app.get("/api/markets")
def markets():
    return rows("SELECT * FROM markets")


@app.get("/api/markets/live")
def live():
    return rows("SELECT * FROM markets WHERE is_open=1")


@app.get("/api/payouts")
def payouts():
    return rows("SELECT * FROM payouts ORDER BY id DESC LIMIT 500")


@app.get("/api/ticks/{symbol}")
def ticks(symbol: str, limit: int = 100):
    return rows("SELECT * FROM ticks WHERE symbol=? ORDER BY id DESC LIMIT ?",
                (symbol, min(max(limit, 1), 5000)))


@app.get("/api/candles/{symbol}")
def candles(symbol: str, limit: int = 500):
    return rows("SELECT * FROM candles WHERE symbol=? ORDER BY epoch DESC LIMIT ?",
                (symbol, min(max(limit, 1), 5000)))


@app.get("/api/history/{symbol}")
def history(symbol: str):
    return rows("SELECT * FROM history WHERE symbol=? ORDER BY id DESC LIMIT 500", (symbol,))


@app.get("/api/stats")
def stats():
    return {t: rows("SELECT COUNT(*) n FROM " + t)[0]["n"]
            for t in ["markets", "ticks", "candles", "payouts", "history"]}


@app.websocket("/ws/live")
async def ws(w: WebSocket):
    await w.accept()
    clients.add(w)
    try:
        while True:
            await w.receive_text()   # client ping; push hoy broadcast() theke
    except Exception:
        pass
    finally:
        clients.discard(w)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
