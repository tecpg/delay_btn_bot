from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
import json
import httpx
from datetime import date, datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import os

import kbt_load_env

# ────────────────────────────────────────────────
# INIT
# ────────────────────────────────────────────────
app = FastAPI(title="Match Fixtures API")

redis_client = redis.from_url(kbt_load_env.redis_url, decode_responses=True)

BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": kbt_load_env.api_football_key}

CACHE_TTL = 7 * 24 * 60 * 60
CACHE_TTL_SHORT = 3600
CACHE_TTL_LONG = 86400 * 3

# ────────────────────────────────────────────────
# DB CONNECTION
# ────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        kbt_load_env.supabase_url,
        cursor_factory=RealDictCursor
    )

# ────────────────────────────────────────────────
# MODEL
# ────────────────────────────────────────────────
class FixtureOut(BaseModel):
    fixture_id: int
    league: str
    league_logo: Optional[str] = None
    home_team: str
    home_logo: Optional[str] = None
    away_team: str
    away_logo: Optional[str] = None
    match_time: Optional[str]
    date: str
    prediction: Optional[str] = None
    odd: Optional[str] = None
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    status: Optional[str] = None
    source: Optional[str] = None
    last_updated: Optional[str] = None

# ────────────────────────────────────────────────
# CACHE HELPERS
# ────────────────────────────────────────────────
def get_cache(key):
    data = redis_client.get(key)
    return json.loads(data) if data else None

def set_cache(key, value, ttl):
    redis_client.setex(key, ttl, json.dumps(value))

# ────────────────────────────────────────────────
# FIXTURES
# ────────────────────────────────────────────────
@app.get("/fixtures/{fixture_date}", response_model=List[FixtureOut])
def get_fixtures(fixture_date: str):

    cache_key = f"fixtures:{fixture_date}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT *
            FROM pro_tips
            WHERE date = %s
            ORDER BY match_time DESC
        """, (fixture_date,))

        rows = cursor.fetchall()

        for r in rows:
            if r.get("last_updated"):
                r["last_updated"] = r["last_updated"].isoformat()

        set_cache(cache_key, rows, CACHE_TTL)
        return rows

    finally:
        cursor.close()
        conn.close()

@app.get("/fixtures/today")
def get_today():
    return get_fixtures(str(date.today()))

# ────────────────────────────────────────────────
# FIXTURE DETAILS
# ────────────────────────────────────────────────
@app.get("/fixture-details/{fixture_id}")
async def fixture_details(fixture_id: int):

    cache_key = f"fixture:{fixture_id}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT full_json, status_short
            FROM fixture_details
            WHERE fixture_id = %s
        """, (fixture_id,))
        row = cursor.fetchone()

        if row:
            data = json.loads(row["full_json"])
            ttl = CACHE_TTL_LONG if row["status_short"] in ["FT","AET","PEN"] else CACHE_TTL_SHORT
            set_cache(cache_key, data, ttl)
            return data

    finally:
        cursor.close()
        conn.close()

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(f"{BASE_URL}/fixtures?id={fixture_id}", headers=HEADERS)
            resp.raise_for_status()
            fixture = resp.json()["response"][0]

            result = {
                "fixture": {
                    "fixture_id": fixture_id,
                    "home_team": fixture["teams"]["home"]["name"],
                    "away_team": fixture["teams"]["away"]["name"],
                    "date": fixture["fixture"]["date"],
                    "status": fixture["fixture"]["status"]["short"],
                    "score": fixture["goals"],
                }
            }

            # save
            conn = get_db()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO fixture_details (fixture_id, full_json, status_short)
                VALUES (%s, %s, %s)
                ON CONFLICT (fixture_id) DO UPDATE SET
                    full_json = EXCLUDED.full_json,
                    status_short = EXCLUDED.status_short,
                    last_updated = CURRENT_TIMESTAMP
            """, (fixture_id, json.dumps(result), result["fixture"]["status"]))

            conn.commit()
            cursor.close()
            conn.close()

            ttl = CACHE_TTL_SHORT
            set_cache(cache_key, result, ttl)

            return result

        except Exception as e:
            raise HTTPException(500, str(e))

# ────────────────────────────────────────────────
# CACHE CLEAR
# ────────────────────────────────────────────────
@app.post("/admin/clear-cache/{fixture_date}")
def clear_cache(fixture_date: str):
    redis_client.delete(f"fixtures:{fixture_date}")
    return {"message": "cleared"}

# ────────────────────────────────────────────────
# SCHEDULER
# ────────────────────────────────────────────────
scheduler = BackgroundScheduler()

def refresh_live_predictions():
    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT fixture_id, date
            FROM pro_tips
            WHERE date = CURRENT_DATE
              AND (
                  last_updated IS NULL
                  OR last_updated < NOW() - INTERVAL '30 seconds'
              )
              AND (
                  status IN ('1H','HT','2H','LIVE')
                  OR (
                      status = 'NS'
                      AND match_time <= CURRENT_TIME + INTERVAL '1 hour 45 minutes'
                  )
              )
            LIMIT 10
        """)

        rows = cursor.fetchall()
        if not rows:
            return

        deleted_dates = set()

        with httpx.Client(timeout=10) as client:
            for row in rows:
                fid = row["fixture_id"]

                try:
                    r = client.get(f"{BASE_URL}/fixtures?id={fid}", headers=HEADERS)
                    data = r.json()["response"][0]

                    cursor.execute("""
                        UPDATE pro_tips
                        SET home_score=%s,
                            away_score=%s,
                            status=%s,
                            last_updated=NOW()
                        WHERE fixture_id=%s
                    """, (
                        data["goals"]["home"] or 0,
                        data["goals"]["away"] or 0,
                        data["fixture"]["status"]["short"],
                        fid
                    ))

                    if row["date"] not in deleted_dates:
                        redis_client.delete(f"fixtures:{row['date']}")
                        deleted_dates.add(row["date"])

                except Exception as e:
                    print("Error:", e)

        conn.commit()

    finally:
        cursor.close()
        conn.close()

scheduler.add_job(
    refresh_live_predictions,
    trigger=IntervalTrigger(seconds=90),
    id="live_refresh",
    replace_existing=True
)

@app.on_event("startup")
async def startup():
    scheduler.start()

@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown()

# ────────────────────────────────────────────────
# HEALTH
# ────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": datetime.utcnow().isoformat()
    }