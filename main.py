import os
import time
import asyncio
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pyquotex.stable_api import Quotex

# ==========================================
# QX TANVIR API v3.12 (Robust Edition)
# ==========================================

OWNER = "TANVIR"
TELEGRAM = "@T4nviRhossain"
BROKER = "Quotex"
VERSION = "3.12"

EMAIL = os.getenv("QX_EMAIL", "")
PASSWORD = os.getenv("QX_PASS", "")

client = None
connected = False
last_error = None
connection_lock = asyncio.Lock()


# ==========================================
# TIMEFRAME SETTINGS
# ==========================================

TIMEFRAMES = {
    "M1": 60,
    "M2": 120,
    "M3": 180,
    "M5": 300,
    "M10": 600,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H4": 14400
}


# ==========================================
# CONNECTION MANAGER
# ==========================================

async def connect_quotex():
    global client, connected, last_error

    async with connection_lock:
        if connected and client:
            return True

        if not EMAIL or not PASSWORD:
            last_error = "QX_EMAIL and QX_PASS environment variables are missing."
            raise RuntimeError(last_error)

        try:
            client = Quotex(
                email=EMAIL,
                password=PASSWORD
            )

            result = await client.connect()

            if isinstance(result, tuple):
                success, reason = result
            else:
                success, reason = bool(result), None

            if not success:
                connected = False
                last_error = f"Quotex connection rejected: {reason or 'Cloud/IP restriction or invalid credentials'}"
                raise RuntimeError(last_error)

            connected = True
            last_error = None
            return True

        except Exception as e:
            connected = False
            last_error = str(e)
            raise RuntimeError(f"Connection exception: {last_error}")


async def ensure_connection():
    global connected
    if connected and client:
        return
    await connect_quotex()


# ==========================================
# CLEANUP
# ==========================================

async def close_quotex():
    global connected, client
    if client:
        try:
            result = client.close()
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            pass

    connected = False
    client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await connect_quotex()
    except Exception as error:
        print("Initial background connection warning:", error)

    yield

    await close_quotex()


app = FastAPI(
    title="QX TANVIR API",
    version=VERSION,
    lifespan=lifespan
)


# ==========================================
# RESPONSE METADATA
# ==========================================

def metadata(start, **kwargs):
    return {
        "Owner_Developer": OWNER,
        "Telegram": TELEGRAM,
        "Broker": BROKER,
        "Timeframe": kwargs.get("timeframe", "M1"),
        "Version": VERSION,
        "Execution_time": f"{time.perf_counter() - start:.4f} second"
    }


# ==========================================
# ASSET HELPERS
# ==========================================

def normalize_assets(assets):
    result = []
    if isinstance(assets, dict):
        assets = list(assets.keys())

    if not assets:
        return result

    for asset in assets:
        if isinstance(asset, (list, tuple)):
            symbol = str(asset[0])
        elif isinstance(asset, dict):
            symbol = asset.get("symbol") or asset.get("name")
        else:
            symbol = str(asset)

        if not symbol:
            continue

        symbol = symbol.strip()
        otc = "_otc" in symbol.lower()

        result.append({
            "pair": symbol,
            "display": symbol.replace("_otc", "").upper() + (" (OTC)" if otc else ""),
            "otc": otc,
            "category": detect_category(symbol),
            "payout": None,
            "price": None,
            "last_epoch": None,
            "is_open": None
        })

    return result


def detect_category(symbol):
    name = symbol.upper()
    if any(x in name for x in ["USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF"]):
        return "currencies"
    if any(x in name for x in ["BTC", "ETH", "LTC", "XRP"]):
        return "crypto"
    if any(x in name for x in ["GOLD", "SILVER", "XAU", "XAG"]):
        return "commodities"
    return "exotics"


# ==========================================
# CANDLE NORMALIZER
# ==========================================

def normalize_candle(candle, pair, payout=None, timeframe=60):
    epoch = int(
        candle.get("time")
        or candle.get("from")
        or candle.get("timestamp")
        or 0
    )

    if epoch > 10_000_000_000:
        epoch //= 1000

    if not epoch:
        return None

    opening = float(candle.get("open", 0))
    high = float(candle.get("high", 0))
    low = float(candle.get("low", 0))
    closing = float(candle.get("close", 0))

    if closing > opening:
        colour = "green"
    elif closing < opening:
        colour = "red"
    else:
        colour = "doji"

    now = int(time.time())
    current_bucket = (now // timeframe) * timeframe
    is_open = epoch >= current_bucket

    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    volume = candle.get("volume", candle.get("ticks"))

    return {
        "pair": pair,
        "time": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "epoch": epoch,
        "open": opening,
        "high": high,
        "low": low,
        "close": closing,
        "colour": colour,
        "payout": payout,
        "chart_open": True,
        "volume": volume
    }


# ==========================================
# ROOT & STATUS
# ==========================================

@app.get("/")
async def root():
    return {
        "Owner_Developer": OWNER,
        "Telegram": TELEGRAM,
        "Broker": BROKER,
        "Version": VERSION,
        "status": "online",
        "message": "QX TANVIR API is running!"
    }


@app.get("/api/status")
async def status():
    return {
        "success": True,
        "connected": connected,
        "last_error": last_error,
        "broker": BROKER,
        "version": VERSION
    }


# ==========================================
# MARKETS
# ==========================================

@app.get("/api/markets")
async def get_markets_data():
    start = time.perf_counter()
    try:
        await ensure_connection()
        assets = client.get_all_asset_name()
        market_list = normalize_assets(assets)

        return {
            **metadata(start),
            "success": True,
            "count": len(market_list),
            "data": market_list
        }
    except Exception as error:
        raise HTTPException(
            status_code=502,
            detail=str(error)
        )


# ==========================================
# CANDLES
# ==========================================

@app.get("/api/candles")
async def get_candles(
    pair: str = Query(...),
    timeframe: str = Query("M1"),
    limit: int = Query(100, ge=1, le=1000)
):
    start = time.perf_counter()

    if timeframe not in TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail="Unsupported timeframe"
        )

    period = TIMEFRAMES[timeframe]

    try:
        await ensure_connection()

        raw = await client.get_candles(
            pair,
            period,
            limit * period
        )

        if not raw:
            raw = []

        if isinstance(raw, dict):
            raw = raw.get("data", [])

        candles = []
        for candle in raw:
            item = normalize_candle(candle, pair, payout=None, timeframe=period)
            if item:
                candles.append(item)

        candles.sort(key=lambda x: x["epoch"], reverse=True)
        candles = candles[:limit]

        now = int(time.time())
        current_bucket = (now // period) * period

        open_count = sum(1 for x in candles if x["epoch"] >= current_bucket)
        closed_count = len(candles) - open_count

        return {
            **metadata(start, timeframe=timeframe),
            "success": True,
            "pair": pair,
            "chart_open": len(candles) > 0,
            "open_candles": open_count,
            "closed_candles": closed_count,
            "count": len(candles),
            "data": candles
        }

    except Exception as error:
        raise HTTPException(
            status_code=502,
            detail=str(error)
        )


@app.get("/api/history")
async def history(
    pair: str,
    timeframe: str = "M1",
    limit: int = Query(100, ge=1, le=1000)
):
    return await get_candles(pair, timeframe, limit)


@app.get("/api/timeframes")
async def timeframes():
    return {
        "success": True,
        "data": TIMEFRAMES
    }


@app.exception_handler(Exception)
async def general_error(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": str(exc)
        }
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
