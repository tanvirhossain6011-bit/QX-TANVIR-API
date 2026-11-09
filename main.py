import os
import asyncio
from fastapi import FastAPI, HTTPException
from pyquotex.stable_api import Quotex

app = FastAPI(title="QX Tanvir API", version="1.0")

# Render-এর Environment Variables থেকে ইমেইল ও পাসওয়ার্ড নেওয়া
EMAIL = os.getenv("QX_EMAIL", "")
PASSWORD = os.getenv("QX_PASS", "")

@app.get("/")
async def root():
    return {"message": "QX Tanvir API is running successfully!"}

@app.get("/api/markets")
async def get_markets_data():
    try:
        # Quotex ক্লায়েন্ট ইনিশিয়ালাইজ করা
        client = Quotex(email=EMAIL, password=PASSWORD, lang="en")
        
        # কানেক্ট বা লগইন করা
        connected = await client.connect()
        if not connected:
            raise HTTPException(status_code=500, detail="Failed to connect to Quotex API.")

        # সব অ্যাসেট এবং পেআউট ডাটা ফেচ করা
        # (pyquotex লাইব্রেরির মেথড অনুযায়ী ডেটা প্রসেসিং)
        all_assets = client.get_all_assets()
        payouts = client.get_payouts()
        
        market_list = []
        
        # ডাটা ফরম্যাট সাজানো
        if isinstance(all_assets, dict):
            for symbol, info in all_assets.items():
                payout_value = payouts.get(symbol, 80) if isinstance(payouts, dict) else 80
                
                market_list.append({
                    "pair": symbol,
                    "display": info.get("name", symbol),
                    "otc": "_otc" in symbol.lower(),
                    "category": info.get("category", "currencies"),
                    "payout": payout_value,
                    "price": info.get("price", 0.0),
                    "last_epoch": info.get("time", 0),
                    "is_open": info.get("enabled", True)
                })

        # কানেকশন বন্ধ করা
        await client.close()

        return {
            "success": True,
            "count": len(market_list),
            "data": market_list
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
