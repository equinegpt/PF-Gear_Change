#!/usr/bin/env bash
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1

mkdir -p logs

# venv python (handles spaces in path)
PY="$ROOT/.venv/bin/python"
if [ ! -x "$PY" ]; then
  echo "ERROR: venv Python not found at $PY" | tee -a logs/gear_cron.error.log
  exit 1
fi

# Today's date in Melbourne (DST-safe)
DATE="$("$PY" - <<'PY'
from datetime import datetime
from zoneinfo import ZoneInfo
print(datetime.now(ZoneInfo("Australia/Melbourne")).strftime("%Y-%m-%d"))
PY
)"

LOG="logs/gear_cron.${DATE}.log"

{
  echo "[UTC $(date -u +%FT%TZ)] start gear for $DATE"
  "$PY" - <<PY
import asyncio, inspect
from pf_gear import fetch_gear_for_date

d = "$DATE"
print(f"Fetching gear for {d} …")

if inspect.iscoroutinefunction(fetch_gear_for_date):
    asyncio.run(fetch_gear_for_date(d))
else:
    fetch_gear_for_date(d)

print("Done.")
PY
  echo "[UTC $(date -u +%FT%TZ)] finished OK"
} | tee -a "$LOG"
