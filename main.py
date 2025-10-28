# main.py — FastAPI wrapper for Gear Changes
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from datetime import datetime
from dateutil import tz
from datetime import timedelta


from pf_gear import fetch_gear_for_date, debug_meetings, debug_form_csv

MAX_AGE = timedelta(hours=26)  # safe > 24h, survives DST/cron drift

app = FastAPI(title="PF Gear Changes", version="1.1")
MEL_TZ = tz.gettz("Australia/Melbourne")

# ---- models ----
class GearRunner(BaseModel):
    runner_number: Optional[int] = None
    horse_name: Optional[str] = None
    runner_id: Optional[int] = None
    gear_change: Optional[str] = None

class RaceGearOut(BaseModel):
    race_number: int
    runners: List[GearRunner]

class MeetingGearOut(BaseModel):
    meeting_id: Optional[int] = None
    meeting: Optional[str] = None
    races: List[RaceGearOut]

class GearOut(BaseModel):
    date: str
    meetings: List[MeetingGearOut]

# ---- routes ----
@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "service": "pf-gear-changes",
        "now_mel": datetime.now(MEL_TZ).isoformat(),
    }

@app.get("/gear/daily", response_model=GearOut)
async def gear_daily(date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$")):
    try:
        return await fetch_gear_for_date(date)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"gear fetch failed: {e}")

@app.get("/gear/today", response_model=GearOut)
async def gear_today():
    date = datetime.now(MEL_TZ).strftime("%Y-%m-%d")
    return await gear_daily(date=date)

# ---------- debug ----------
@app.get("/gear/debug/meetings")
async def gear_debug_meetings(date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$")):
    return await debug_meetings(date)

@app.get("/gear/debug/formcsv")
async def gear_debug_formcsv(meeting_id: int):
    """Shows PF status/preview for /form/form/csv on a single meetingId."""
    return await debug_form_csv(meeting_id)
