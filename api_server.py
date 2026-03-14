from fastapi import FastAPI
from fastapi.responses import JSONResponse
from typing import List, Optional
import mysql.connector
import redis
import json
from datetime import date, timedelta
import kbt_funtions
import kbt_load_env
from pydantic import BaseModel

app = FastAPI(title="Match Fixtures API")

# -----------------------------
# Redis setup
# -----------------------------
REDIS_URL = kbt_load_env.redis_url
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
CACHE_TTL = 7 * 24 * 60 * 60  # 7 days

# -----------------------------
# Pydantic model
# -----------------------------
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
    home_score: Optional[str]
    away_score: Optional[str]
    status: Optional[str]
    source: Optional[str]
    last_updated: Optional[str]

# -----------------------------
# DB helper
# -----------------------------
def get_db():
    return kbt_funtions.db_connection()

# -----------------------------
# Redis helper
# -----------------------------
def get_fixtures_from_cache(fixture_date: str):
    cache_key = f"fixtures:{fixture_date}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    return None

def set_fixtures_to_cache(fixture_date: str, fixtures: list):
    cache_key = f"fixtures:{fixture_date}"
    redis_client.setex(cache_key, CACHE_TTL, json.dumps(fixtures))

# -----------------------------
# API endpoints
# -----------------------------
@app.get("/fixtures/today", response_model=List[FixtureOut])
def get_fixtures_today():
    today_str = str(date.today())
    cached = get_fixtures_from_cache(today_str)
    if cached:
        return cached
    return get_fixtures_by_date(today_str)

@app.get("/fixtures/{fixture_date}", response_model=List[FixtureOut])
def get_fixtures_by_date(fixture_date: str):
    # Check Redis cache
    cached = get_fixtures_from_cache(fixture_date)
    if cached:
        return cached

    conn = None
    cursor = None

    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT fixture_id, league, league_logo, home_team, home_logo,
                   away_team, away_logo, match_time, date, prediction, odd,
                   home_score, away_score, status, source, last_updated
            FROM pro_tips
            WHERE `date` = %s
            ORDER BY match_time DESC
        """, (fixture_date,))
        fixtures = cursor.fetchall()

        # Safely handle NULL/empty values
        for f in fixtures:

            f["home_score"] = str(f.get("home_score") or "")
            f["away_score"] = str(f.get("away_score") or "")
            f["status"] = str(f.get("status") or "")
            f["match_time"] = str(f.get("match_time") or "")
            f["date"] = str(f.get("date") or "")
            f["league_logo"] = f.get("league_logo") or ""
            f["home_logo"] = f.get("home_logo") or ""
            f["away_logo"] = f.get("away_logo") or ""
            f["prediction"] = f.get("prediction") or ""
            f["odd"] = f.get("odd") or ""
            f["source"] = f.get("source") or ""
            f["last_updated"] = f["last_updated"].strftime("%Y-%m-%dT%H:%M:%S") if f.get("last_updated") else ""
                # Save to Redis
        set_fixtures_to_cache(fixture_date, fixtures)

        return fixtures

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
    # Check Redis cache
    cached = get_fixtures_from_cache(fixture_date)
    if cached:
        return cached

    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT fixture_id, league, league_logo, home_team, home_logo,
                   away_team, away_logo, match_time, date, prediction, odd, home_score,
                       away_score, status, source,
                        last_updated
            FROM pro_tips
            WHERE date = %s
            ORDER BY match_time ASC
        """, (fixture_date,))
        fixtures = cursor.fetchall()

        # Ensure string formatting
        for f in fixtures:
            
            f["home_score"] = f.get("home_score") or ""
            f["away_score"] = f.get("away_score") or ""
            f["status"] = f.get("status") or ""
            if f.get("match_time"):
                f["match_time"] = str(f["match_time"])
            if f.get("date"):
                f["date"] = str(f["date"])
            if f.get("last_updated"):
                f["last_updated"] = f["last_updated"].strftime("%Y-%m-%dT%H:%M:%S")

        # Save to Redis (7 days)
        set_fixtures_to_cache(fixture_date, fixtures)

        return fixtures

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()