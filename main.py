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

BD = timezone(timedelta(hours=6))
META = {"api": "tanvirxqx.botpro", "developer": "TANVIR", "telegram": "@T4nviRhossain"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("tanvirxqx")

db = sqlite3.connect("data.db", check_same_thread=False)
db.execute("PRAGMA journal_mode=WAL")
db.execute("CREATE TABLE IF NOT EXISTS candles(asset TEXT, ts INTEGER, open REAL, high REAL, low REAL, close REAL, PRIMARY KEY(asset, ts))")
db.execute("CREATE TABLE IF NOT EXISTS payouts(ts INTEGER, asset TEXT, payout REAL, is_open INTEGER)")
db.execute("CREATE INDEX IF NOT EXISTS ix_pay ON payouts(asset, ts)")
