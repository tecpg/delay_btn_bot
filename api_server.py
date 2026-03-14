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

def set_fixtures_to_cache(fixture_date: str, fixtures: list):
    redis_client.setex(f"fixtures:{fixture_date}", CACHE_TTL, json.dumps(fixtures))

@app.get("/fixtures/today", response_model=List[FixtureOut])
def get_fixtures_today():
    return get_fixtures_by_date(str(date.today()))

@app.get("/fixtures/{fixture_date}")
def get_fixtures_by_date(fixture_date: str):
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

        return {"raw": fixtures}   # skip validation

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()