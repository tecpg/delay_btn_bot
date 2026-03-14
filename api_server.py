from fastapi import FastAPI
from fastapi.responses import JSONResponse
from typing import List, Optional
import mysql.connector
import redis
import json
from datetime import date
import kbt_funtions
import kbt_load_env
from pydantic import BaseModel, parse_obj_as

app = FastAPI(title="Match Fixtures API")

REDIS_URL = kbt_load_env.redis_url
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
CACHE_TTL = 7 * 24 * 60 * 60  # 7 days

class FixtureOut(BaseModel):
    fixture_id: int
    league: str
    league_logo: Optional[str] = None
    home_team: str
    home_logo: Optional[str] = None
    away_team: str
    away_logo: Optional[str] = None
    match_time: str
    date: str
    prediction: Optional[str] = None
    odd: str
    home_score: Optional[str] = None      # ← add = None
    away_score: Optional[str] = None      # ← add = None
    status: Optional[str] = None          # ← add = None
    source: Optional[str] = None
    last_updated: Optional[str] = None

def get_db():
    return kbt_funtions.db_connection()

def get_fixtures_from_cache(fixture_date: str):
    cached = redis_client.get(f"fixtures:{fixture_date}")
    if cached:
        return json.loads(cached)
    return None
from datetime import date, timedelta

def set_fixtures_to_cache(fixture_date: str, fixtures: list):
    cache_key = f"fixtures:{fixture_date}"
    
    try:
        match_date = date.fromisoformat(fixture_date)
        today = date.today()
        days_old = (today - match_date).days
        
        if days_old > 7:          # very old matches – rarely change
            ttl = 24 * 60 * 60          # 1 day
        elif days_old >= 0:       # today or past
            ttl = 60 * 60               # 1 hour (scores usually final after FT)
        else:                     # future matches
            ttl = 7 * 24 * 60 * 60      # your original 7 days
    except ValueError:
        ttl = CACHE_TTL  # fallback if date parsing fails
    
    redis_client.setex(cache_key, ttl, json.dumps(fixtures))

@app.get("/fixtures/today", response_model=List[FixtureOut])
def get_fixtures_today():
    return get_fixtures_by_date(str(date.today()))

@app.get("/fixtures/{fixture_date}", response_model=List[FixtureOut])
def get_fixtures_by_date(fixture_date: str):
    cached = get_fixtures_from_cache(fixture_date)
    if cached:
        return parse_obj_as(List[FixtureOut], cached)

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
            ORDER BY match_time ASC
        """, (fixture_date,))
        fixtures = cursor.fetchall()

        # Normalize types for Pydantic
        for f in fixtures:
            for key in ["home_score", "away_score", "status", "prediction", "source",
                        "league_logo", "home_logo", "away_logo", "odd", "match_time", "date"]:
                if f.get(key) is not None:
                    f[key] = str(f[key])
            if f.get("last_updated") and hasattr(f["last_updated"], "strftime"):
                f["last_updated"] = f["last_updated"].strftime("%Y-%m-%dT%H:%M:%S")

        # Validate with Pydantic (ensures response matches model)
        fixtures_out = parse_obj_as(List[FixtureOut], fixtures)

        # Cache in Redis
        set_fixtures_to_cache(fixture_date, [f.dict() for f in fixtures_out])

        return fixtures_out

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


# Add a manual cache-clear endpoint (useful for admin/debug)
# Call it with POST /admin/clear-cache/2026-03-11 when you know results have been updated.
@app.post("/admin/clear-cache/{fixture_date}")
def clear_cache(fixture_date: str):
    cache_key = f"fixtures:{fixture_date}"
    deleted = redis_client.delete(cache_key)
    return {"message": f"Cache for {fixture_date} cleared", "deleted": deleted > 0}