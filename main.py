import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from typing import List, Optional
import mysql.connector
import redis
import json
from datetime import date
import kbt_funtions  # your existing DB connection helper
import kbt_load_env

app = FastAPI(title="Match Fixtures API")

# -----------------------------
# Connect to Redis
# -----------------------------
REDIS_URL = kbt_load_env.redis_url
redis_client = redis.from_url(REDIS_URL, decode_responses=True)  # decode_responses=True returns strings

CACHE_TTL = 24 * 60 * 60  # 24 hours cache

# ---------------------------
# Pydantic Model
# ---------------------------
from pydantic import BaseModel

class FixtureOut(BaseModel):
    fixture_id: int
    league: str
    league_logo: Optional[str]
    home_team: str
    home_logo: Optional[str]
    away_team: str
    away_logo: Optional[str]
    match_time: str  # "HH:MM:SS"
    date: str        # "YYYY-MM-DD"
    prediction: Optional[str]
    odd: str
    source: Optional[str]
    last_updated: Optional[str]

# ---------------------------
# DB Connection Helper
# ---------------------------
def get_db():
    return kbt_funtions.db_connection()

# ---------------------------
# Redis Helpers
# ---------------------------
def get_fixtures_from_cache():
    cache_key = f"fixtures:{date.today()}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    return None

def set_fixtures_to_cache(fixtures):
    cache_key = f"fixtures:{date.today()}"
    redis_client.setex(cache_key, CACHE_TTL, json.dumps(fixtures))

# ---------------------------
# API Endpoint
# ---------------------------
@app.get("/fixtures/today", response_model=List[FixtureOut])
def get_fixtures_today():
    # 1️⃣ Try Redis cache first
    cached = get_fixtures_from_cache()
    if cached:
        return cached

    # 2️⃣ Cache miss → fetch from MySQL
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT fixture_id, league, league_logo, home_team, home_logo,
                   away_team, away_logo, match_time, date, prediction, odd, source, last_updated
            FROM pro_tips
            WHERE date = CURDATE()
            ORDER BY match_time ASC
        """)
        fixtures = cursor.fetchall()

        # 3️⃣ Ensure match_time is string
        for f in fixtures:
            if f.get("match_time") is not None:
                # Already stored as string in DB → keep as-is
                f["match_time"] = str(f["match_time"])
            if f.get("date") is not None:
                f["date"] = str(f["date"])
            if f.get("last_updated") is not None:
                f["last_updated"] = f["last_updated"].strftime("%Y-%m-%dT%H:%M:%S")

        # 4️⃣ Save to Redis
        set_fixtures_to_cache(fixtures)

        return fixtures

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


@app.get("/fixtures/{fixture_date}", response_model=List[FixtureOut])
def get_fixtures_by_date(fixture_date: str):

    cache_key = f"fixtures:{fixture_date}"

    # 1️⃣ Check Redis cache
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT fixture_id, league, league_logo, home_team, home_logo,
                   away_team, away_logo, match_time, date, prediction, odd, source, last_updated
            FROM pro_tips
            WHERE date = %s
            ORDER BY match_time ASC
        """, (fixture_date,))

        fixtures = cursor.fetchall()

        # Format fields
        for f in fixtures:
            if f.get("match_time") is not None:
                f["match_time"] = str(f["match_time"])

            if f.get("date") is not None:
                f["date"] = str(f["date"])

            if f.get("last_updated") is not None:
                f["last_updated"] = f["last_updated"].strftime("%Y-%m-%dT%H:%M:%S")

        # 2️⃣ Save to Redis cache
        redis_client.setex(cache_key, CACHE_TTL, json.dumps(fixtures))

        return fixtures

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()