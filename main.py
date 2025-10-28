# main.py
from fastapi import FastAPI, Query, HTTPException
from typing import Optional, Dict
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx

from pf_gear import fetch_gear_for_date, debug_meetings, debug_form_csv

app = FastAPI(title="PF Gear Changes", version="1.0.0")

# --- Timezone helpers ---
MEL = ZoneInfo("Australia/Melbourne")

def today_mel_str() -> str:
    try:
        return datetime.now(MEL).strftime("%Y-%m-%d")
    except Exception:
        # Fallback if zoneinfo not available
        return datetime.utcnow().strftime("%Y-%m-%d")

# --- Simple in-memory cache keyed by 'YYYY-MM-DD' ---
GEAR_CACHE: Dict[str, dict] = {}

@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "service": "pf-gear-changes",
        "now_mel": datetime.now(MEL).isoformat(),
        "cache_keys": list(GEAR_CACHE.keys()),
    }

@app.get("/gear/daily")
async def gear_daily(
    date: Optional[str] = Query(
        None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="YYYY-MM-DD (defaults to today's date in Australia/Melbourne)"
    ),
    warm: bool = Query(
        False,
        description="Force refresh and update cache for the resolved date"
    ),
):
    """
    Return gear changes for a date.
    - If `date` is omitted, uses today's date in Australia/Melbourne.
    - If `warm=1`, forces refresh and stores in cache.
    """
    d = date or today_mel_str()

    if not warm and d in GEAR_CACHE:
        return GEAR_CACHE[d]

    try:
        data = await fetch_gear_for_date(d)
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"PF call failed: {e}") from e

    GEAR_CACHE[d] = data
    return data

@app.get("/gear/today")
async def gear_today(
    warm: bool = Query(
        False,
        description="Force refresh and update cache for today's Melbourne date"
    )
):
    """
    Convenience route: always resolves to today's Melbourne date.
    """
    return await gear_daily(date=today_mel_str(), warm=warm)

# --- Debug helpers ---

@app.get("/gear/debug/meetings")
async def gear_debug_meetings(
    date: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$")
):
    d = date or today_mel_str()
    return await debug_meetings(d)

@app.get("/gear/debug/formcsv")
async def gear_debug_formcsv(meeting_id: int):
    return await debug_form_csv(meeting_id)
