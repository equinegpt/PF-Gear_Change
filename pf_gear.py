# pf_gear.py — Gear Changes per meeting/race/runner for a given date (AUS/MEL)
import os
import re
import csv
import io
import unicodedata
from typing import Any, Dict, List, Optional, Set, Tuple
import httpx
from datetime import datetime

PF_API_KEY = os.getenv("PF_API_KEY")

# PF endpoints
PF_FORM_CSV_URL     = "https://api.puntingform.com.au/v2/form/form/csv"
PF_MEETING_CSV_URL  = "https://api.puntingform.com.au/v2/form/meeting/csv"
PF_UPD_SCR_URL      = "https://api.puntingform.com.au/v2/Updates/Scratchings"
PF_UPD_COND_URL     = "https://api.puntingform.com.au/v2/Updates/Conditions"

# ---------------- helpers ----------------

def _snakify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.strip().lower())

def _parse_int(x: Any) -> Optional[int]:
    try:
        v = int(x)
        return v if v != 0 else None
    except Exception:
        return None

def _norm_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s or None

def _canonise(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    lower = {_snakify(k): v for k, v in d.items()}
    out.update(lower)
    return out

def _yyyy_mm_dd(date_str: str) -> Optional[str]:
    """Accept 'YYYY-MM-DD', 'DD-MM-YYYY', or ISO 'YYYY-MM-DDTHH:MM:SS' → 'YYYY-MM-DD'."""
    if not date_str:
        return None
    s = str(date_str).strip()
    m = re.match(r"^(\d{4}-\d{2}-\d{2})[T ]", s)
    if m:
        return m.group(1)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    m = re.fullmatch(r"(\d{2})-(\d{2})-(\d{4})", s)
    if m:
        d, mth, y = m.groups()
        return f"{y}-{mth}-{d}"
    return None

RUNNER_NUM_KEYS = {
    "runner_number", "runnernumber", "no", "number",
    "saddle_number", "saddlenumber", "saddle_no",
    "tab_no", "tabno", "cloth", "cloth_number", "program_number",
}
HORSE_NAME_KEYS = {"horse_name", "runnername", "name", "horse"}
RACE_NO_KEYS    = {"race_number", "racenumber", "race_no", "raceno", "race"}
GEAR_KEYS       = {"gearchanges", "gear_changes", "gear_change", "gear", "gear_desc", "gear_description"}
SCRATCH_KEYS    = {"scratched", "is_scratched", "scratch"}

def _get_first(d: Dict[str, Any], keys: Set[str]) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in ("", None):
            return d[k]
    return None

def _runner_number(d: Dict[str, Any]) -> Optional[int]:
    v = _get_first(d, RUNNER_NUM_KEYS)
    try:
        if v is None:
            return None
        n = int(str(v).strip())
        return n if n != 0 else None
    except Exception:
        return None

def _horse_name(d: Dict[str, Any]) -> Optional[str]:
    v = _get_first(d, HORSE_NAME_KEYS)
    return v if isinstance(v, str) and v.strip() else None

def _race_number(d: Dict[str, Any]) -> Optional[int]:
    v = _get_first(d, RACE_NO_KEYS)
    return _parse_int(v)

def _gear_text(d: Dict[str, Any]) -> Optional[str]:
    v = _get_first(d, GEAR_KEYS)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None

def _is_scratched(d: Dict[str, Any]) -> bool:
    v = _get_first(d, SCRATCH_KEYS)
    if isinstance(v, bool):
        return v
    if isinstance(v, str) and v.strip():
        return v.strip().lower() in {"1", "true", "y", "yes", "t"}
    return False

def _first_track_name(rows: List[Dict[str, Any]]) -> Optional[str]:
    # Canonical key will be "track_name"
    for raw in rows or []:
        c = _canonise(raw)
        t = c.get("track_name")
        if isinstance(t, str) and t.strip():
            return t.strip()
    return None

# ---------------- HTTP (header+param fallbacks) ----------------

async def _get_json(url: str, params: Dict[str, Any]) -> Any:
    key = PF_API_KEY
    if not key:
        raise RuntimeError("PF_API_KEY not set")
    attempts = [
        ({"accept": "application/json", "X-Api-Key": key}, params),
        ({"accept": "application/json"}, {**params, "apiKey": key}),
    ]
    async with httpx.AsyncClient(timeout=30.0) as client:
        last_err = None
        for headers, q in attempts:
            try:
                r = await client.get(url, headers=headers, params=q)
                if r.status_code == 200:
                    j = r.json()
                    if isinstance(j, dict) and "payLoad" in j:
                        return j["payLoad"]
                    return j
                if r.status_code in (401, 403):
                    last_err = f"{r.status_code} {r.text[:220]}"
                    continue
                r.raise_for_status()
            except Exception as e:
                last_err = str(e)
        raise httpx.HTTPStatusError(f"PF JSON failed for {url}: {last_err}", request=None, response=None)

async def _get_csv(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    key = PF_API_KEY
    if not key:
        raise RuntimeError("PF_API_KEY not set")
    attempts = [
        ({"accept": "text/csv", "X-Api-Key": key}, params),
        ({"accept": "text/csv"}, {**params, "apiKey": key}),
    ]
    async with httpx.AsyncClient(timeout=30.0) as client:
        last_err = None
        for headers, q in attempts:
            try:
                r = await client.get(url, headers=headers, params=q)
                if r.status_code == 200:
                    text = r.text or ""
                    if not text.strip():
                        return []
                    buff = io.StringIO(text.strip("\ufeff\r\n"))
                    return [dict(row) for row in csv.DictReader(buff)]
                if r.status_code in (401, 403):
                    # return [] silently, but record error for debug path
                    last_err = f"{r.status_code} {r.text[:220]}"
                    continue
                r.raise_for_status()
            except Exception as e:
                last_err = str(e)
        # CSV paths: return empty list (normal flow). Debug route uses raw fetch below.
        return []

async def _get_csv_raw(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Raw fetch for debug: expose status/body so we can see auth errors or headers."""
    key = PF_API_KEY
    if not key:
        return {"ok": False, "error": "PF_API_KEY not set"}
    attempts = [
        ({"accept": "text/csv", "X-Api-Key": key}, params),
        ({"accept": "text/csv"}, {**params, "apiKey": key}),
    ]
    async with httpx.AsyncClient(timeout=30.0) as client:
        results = []
        for headers, q in attempts:
            r = await client.get(url, headers=headers, params=q)
            results.append({
                "attempt_headers": list(headers.keys()),
                "status_code": r.status_code,
                "preview": (r.text[:400] if r.text else ""),
            })
            if r.status_code == 200:
                # show first columns from header row
                try:
                    buff = io.StringIO(r.text.strip("\ufeff\r\n"))
                    reader = csv.DictReader(buff)
                    first = next(reader, None)
                    cols = reader.fieldnames or []
                    return {"ok": True, "status_code": 200, "columns": cols, "first_row": first}
                except Exception as e:
                    return {"ok": False, "error": f"CSV parse error: {e}", "status_code": 200}
        return {"ok": False, "tries": results}

# ---------------- meeting discovery ----------------

async def _meetings_from_meeting_csv(date_str: str) -> List[Dict[str, Any]]:
    """Try multiple param names/formats to get meetings for the date."""
    ymd = date_str  # 'YYYY-MM-DD'
    dmy = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")

    tries = [
        {"meetingDate": ymd},
        {"date": ymd},
        {"meeting_date": ymd},
        {"meetingDate": dmy},
        {"date": dmy},
    ]
    seen = set()
    out: List[Dict[str, Any]] = []
    for q in tries:
        rows = await _get_csv(PF_MEETING_CSV_URL, q)
        for raw in rows or []:
            c = _canonise(raw)
            mid = _parse_int(c.get("meeting_id"))
            venue = c.get("venue") or c.get("track") or c.get("course") or c.get("meeting")
            key = f"{mid or 0}|{(venue or '').lower()}"
            if key in seen:
                continue
            seen.add(key)
            out.append({"meeting_id": mid, "meeting": venue})
    return out

async def _meetings_from_updates(date_str: str) -> List[Dict[str, Any]]:
    """Use Updates endpoints (scratchings + conditions) to harvest meetingIds for that date."""
    ymd = date_str
    dmy = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")

    out: Dict[int, Dict[str, Any]] = {}

    # Scratchings
    scr = await _get_json(PF_UPD_SCR_URL, {})
    for item in scr or []:
        c = _canonise(item)
        mdate = _yyyy_mm_dd(c.get("meeting_date") or c.get("meetingdate") or c.get("meetingdateutc") or c.get("timestamp"))
        if not mdate:
            mdate = _yyyy_mm_dd(c.get("meeting_date") or dmy)
        if mdate != ymd:
            continue
        mid = _parse_int(c.get("meeting_id")) or _parse_int(c.get("meetingid"))
        venue = c.get("track") or c.get("venue")
        if mid:
            out.setdefault(mid, {"meeting_id": mid, "meeting": venue})

    # Conditions
    cond = await _get_json(PF_UPD_COND_URL, {})
    for item in cond or []:
        c = _canonise(item)
        mdate = _yyyy_mm_dd(c.get("meeting_date") or c.get("meetingdate") or c.get("last_update") or c.get("timestamp"))
        if not mdate:
            mdate = _yyyy_mm_dd(c.get("meeting_date") or dmy)
        if mdate != ymd:
            continue
        mid = _parse_int(c.get("meeting_id")) or _parse_int(c.get("meetingid"))
        venue = c.get("track") or c.get("venue")
        if mid:
            out.setdefault(mid, {"meeting_id": mid, "meeting": venue})

    return list(out.values())

async def _meetings_for_date(date_str: str) -> List[Dict[str, Any]]:
    """Union of meetings from meeting CSV and Updates (deduped)."""
    a = await _meetings_from_meeting_csv(date_str)
    b = await _meetings_from_updates(date_str)
    seen = set()
    out: List[Dict[str, Any]] = []
    for m in (a + b):
        mid = m.get("meeting_id")
        venue = m.get("meeting")
        key = f"{mid or 0}|{(venue or '').lower()}"
        if key in seen:
            continue
        seen.add(key)
        out.append(m)
    return out

# ---------------- gear extraction ----------------

async def _gear_from_meeting_csv(meeting_id: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Query PF form/form/csv per raceNumber so we (a) know the race_number,
    (b) only extract current-race GearChanges (NOT Forms[n].GearChanges).
    Returns (rows, meeting_name)
    """
    all_rows: List[Dict[str, Any]] = []
    meeting_name: Optional[str] = None

    # Reasonable upper bound; we break when we hit consecutive empty races.
    consecutive_empty = 0
    for rn in range(1, 16):
        rows = await _get_csv(PF_FORM_CSV_URL, {"meetingId": meeting_id, "raceNumber": rn})
        if not rows:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
            continue
        consecutive_empty = 0

        # Cache meeting name if we can
        if meeting_name is None:
            meeting_name = _first_track_name(rows) or meeting_name

        for raw in rows:
            c = _canonise(raw)

            # Only take top-level GearChanges for this race (ignore historical Forms[n].GearChanges)
            gear = c.get("gearchanges")
            if not isinstance(gear, str) or not gear.strip():
                continue

            # Skip scratched
            if _is_scratched(c):
                continue

            all_rows.append({
                "race_number": rn,
                "runner_number": _runner_number(c),
                "horse_name": _horse_name(c),
                "runner_id": _parse_int(c.get("runner_id")),
                "gear_change": gear.strip(),
            })

    # Keep only entries with some identity
    all_rows = [r for r in all_rows if r.get("horse_name") or r.get("runner_id")]
    return all_rows, meeting_name

# ---------------- debug helpers (exported) ----------------

async def debug_meetings(date_str: str) -> Dict[str, Any]:
    a = await _meetings_from_meeting_csv(date_str)
    b = await _meetings_from_updates(date_str)
    return {"date": date_str, "from_meeting_csv": a, "from_updates": b}

async def debug_form_csv(meeting_id: int) -> Dict[str, Any]:
    return await _get_csv_raw(PF_FORM_CSV_URL, {"meetingId": meeting_id, "raceNumber": 0})

# ---------------- public ----------------

async def fetch_gear_for_date(date_str: str) -> Dict[str, Any]:
    meetings = await _meetings_for_date(date_str)
    out_meetings: List[Dict[str, Any]] = []

    for m in meetings:
        mid = m.get("meeting_id")
        venue = m.get("meeting")
        if not mid:
            out_meetings.append({"meeting_id": None, "meeting": venue, "races": []})
            continue

        rows, track_name = await _gear_from_meeting_csv(mid)
        # Prefer CSV track name if present
        meeting_label = track_name or venue

        # Group rows by race_number
        races_map: Dict[int, List[Dict[str, Any]]] = {}
        for r in rows:
            rno = r.get("race_number")
            if rno is None:
                continue
            races_map.setdefault(rno, []).append({
                "runner_number": r.get("runner_number"),
                "horse_name": r.get("horse_name"),
                "runner_id": r.get("runner_id"),
                "gear_change": r.get("gear_change"),
            })

        races_out = [
            {
                "race_number": rno,
                "runners": sorted(rows, key=lambda x: (x["runner_number"] is None, x["runner_number"] or 0, (x["horse_name"] or "").lower()))
            }
            for rno, rows in sorted(races_map.items(), key=lambda kv: kv[0])
        ]

        out_meetings.append({
            "meeting_id": mid,
            "meeting": meeting_label,
            "races": races_out
        })

    return {"date": date_str, "meetings": out_meetings}
