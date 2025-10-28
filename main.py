# main.py
from fastapi import FastAPI, Query, HTTPException
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx

from pf_gear import fetch_gear_for_date, debug_meetings, debug_form_csv

app = FastAPI(title="PF Gear Changes", version="1.0.0")

def today_mel_str() -> str:
    try:
        return datetime.now(ZoneInfo("Australia/Melbourne")).strftime("%Y-%m-%d")
    except Exception:
        # Fallback if zoneinfo not available
        return datetime.utcnow().strftime("%Y-%m-%d")

@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "service": "pf-gear-changes",
        "now_mel": datetime.now(ZoneInfo("Australia/Melbourne")).isoformat()
    }

@app.get("/gear/daily")
async def gear_daily(
    date: Optional[str] = Query(
        None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="YYYY-MM-DD (defaults to today's date in Australia/Melbourne)"
    )
):
    # default to today (Melbourne) if not provided
    date = date or today_mel_str()
    try:
        return await fetch_gear_for_date(date)
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"PF call failed: {e}") from e

@app.get("/gear/today")
async def gear_today():
    # explicit helper that always returns today (Melbourne)
    date = today_mel_str()
    return await fetch_gear_for_date(date)

# --- Debug helpers ---

@app.get("/gear/debug/meetings")
async def gear_debug_meetings(
    date: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$")
):
    date = date or today_mel_str()
    return await debug_meetings(date)

@app.get("/gear/debug/formcsv")
async def gear_debug_formcsv(meeting_id: int):
    return await debug_form_csv(meeting_id)
