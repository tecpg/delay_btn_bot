from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
import mysql.connector
import redis
import json
import httpx
import asyncio
from datetime import date, datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

import kbt_funtions
import kbt_load_env

# ────────────────────────────────────────────────
# APP INIT
# ────────────────────────────────────────────────
app = FastAPI(title="Match Fixtures API")

redis_client = redis.from_url(kbt_load_env.redis_url, decode_responses=True)

BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": kbt_load_env.api_football_key}

CACHE_TTL = 7 * 24 * 60 * 60
CACHE_TTL_SHORT = 3600
CACHE_TTL_LONG = 86400 * 3

# ────────────────────────────────────────────────
# MODELS
# ────────────────────────────────────────────────
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
    home_score: Optional[str] = None
    away_score: Optional[str] = None
    status: Optional[str] = None
    source: Optional[str] = None
    last_updated: Optional[str] = None

# ────────────────────────────────────────────────
# DB
# ────────────────────────────────────────────────
def get_db():
    return kbt_funtions.db_connection()

# ────────────────────────────────────────────────
# CACHE HELPERS
# ────────────────────────────────────────────────
def get_fixtures_from_cache(fixture_date: str):
    cached = redis_client.get(f"fixtures:{fixture_date}")
    return json.loads(cached) if cached else None

def set_fixtures_to_cache(fixture_date: str, fixtures: list):
    try:
        match_date = date.fromisoformat(fixture_date)
        days_old = (date.today() - match_date).days

        if days_old > 7:
            ttl = 86400
        elif days_old >= 0:
            ttl = 3600
        else:
            ttl = CACHE_TTL
    except:
        ttl = CACHE_TTL

    redis_client.setex(f"fixtures:{fixture_date}", ttl, json.dumps(fixtures))

# ────────────────────────────────────────────────
# FIXTURES ENDPOINT
# ────────────────────────────────────────────────
@app.get("/fixtures/{fixture_date}", response_model=List[FixtureOut])
def get_fixtures_by_date(fixture_date: str):
    cached = get_fixtures_from_cache(fixture_date)
    if cached:
        return [FixtureOut(**f) for f in cached]

    conn, cursor = None, None
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT * FROM pro_tips
            WHERE `date` = %s
            ORDER BY match_time DESC
        """, (fixture_date,))

        fixtures = cursor.fetchall()

        for f in fixtures:
            for key in f:
                if f[key] is not None:
                    f[key] = str(f[key])

        fixtures_out = [FixtureOut(**f) for f in fixtures]

        set_fixtures_to_cache(fixture_date, [f.dict() for f in fixtures_out])
        return fixtures_out

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

    finally:
        if cursor: cursor.close()
        if conn and conn.is_connected(): conn.close()

# ────────────────────────────────────────────────
# FIXTURE DETAILS (ASYNC + PARALLEL)
# ────────────────────────────────────────────────
@app.get("/fixture-details/{fixture_id}")
async def get_fixture_details(fixture_id: int):
    cache_key = f"fixture_full:{fixture_id}"

    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    # Check DB
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT full_json, status_short FROM fixture_details WHERE fixture_id = %s", (fixture_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row:
        data = json.loads(row["full_json"])
        ttl = CACHE_TTL_LONG if row["status_short"] in ["FT","AET","PEN"] else CACHE_TTL_SHORT
        redis_client.setex(cache_key, ttl, json.dumps(data))
        return data

    # Fetch API
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            fixture_resp = await client.get(f"{BASE_URL}/fixtures?id={fixture_id}", headers=HEADERS)
            fixture_resp.raise_for_status()

            fixture_data = fixture_resp.json()["response"][0]

            league_id = fixture_data["league"]["id"]
            season = fixture_data["league"]["season"]
            home_id = fixture_data["teams"]["home"]["id"]
            away_id = fixture_data["teams"]["away"]["id"]

            # PARALLEL CALLS
            lineup_task = client.get(f"{BASE_URL}/fixtures/lineups?fixture={fixture_id}", headers=HEADERS)
            stats_task = client.get(f"{BASE_URL}/fixtures/statistics?fixture={fixture_id}", headers=HEADERS)
            odds_task = client.get(f"{BASE_URL}/odds?fixture={fixture_id}&bookmaker=1", headers=HEADERS)

            lineup_resp, stats_resp, odds_resp = await asyncio.gather(
                lineup_task, stats_task, odds_task
            )

            result = {
                "fixture": {
                    "fixture_id": fixture_id,
                    "home_team": fixture_data["teams"]["home"]["name"],
                    "away_team": fixture_data["teams"]["away"]["name"],
                    "date": fixture_data["fixture"]["date"],
                    "status": fixture_data["fixture"]["status"]["short"],
                    "score": fixture_data["goals"],
                }
            }

            # Lineups
            result["lineups"] = [
                {
                    "team": t["team"]["name"],
                    "formation": t.get("formation"),
                }
                for t in lineup_resp.json().get("response", [])
            ]

            # Stats
            result["statistics"] = {
                t["team"]["name"]: {s["type"]: s["value"] for s in t["statistics"]}
                for t in stats_resp.json().get("response", [])
            }

            # Odds
            odds = {"home": None, "draw": None, "away": None}
            try:
                vals = odds_resp.json()["response"][0]["bookmakers"][0]["bets"][0]["values"]
                odds = {v["value"].lower(): v["odd"] for v in vals}
            except:
                pass

            result["odds"] = odds

            # Save DB
            conn = get_db()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO fixture_details (fixture_id, league_id, season, home_team_id, away_team_id, full_json, status_short)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE full_json=VALUES(full_json), status_short=VALUES(status_short)
            """, (
                fixture_id, league_id, season, home_id, away_id,
                json.dumps(result), result["fixture"]["status"]
            ))

            conn.commit()
            cursor.close()
            conn.close()

            ttl = CACHE_TTL_SHORT if result["fixture"]["status"] not in ["FT","AET","PEN"] else CACHE_TTL_LONG
            redis_client.setex(cache_key, ttl, json.dumps(result))

            return result

        except Exception as e:
            raise HTTPException(500, str(e))

# ────────────────────────────────────────────────
# CACHE CLEAR
# ────────────────────────────────────────────────
@app.post("/admin/clear-cache/{fixture_date}")
def clear_cache(fixture_date: str):
    deleted = redis_client.delete(f"fixtures:{fixture_date}")
    return {"deleted": bool(deleted)}

# ────────────────────────────────────────────────
# SCHEDULER
# ────────────────────────────────────────────────
scheduler = BackgroundScheduler()

def refresh_live_predictions():
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT fixture_id, `date`
        FROM pro_tips
        WHERE `date` = CURDATE()
        AND last_updated < NOW() - INTERVAL 30 SECOND
        AND status IN ('1H','HT','2H','LIVE')
        LIMIT 10
    """)

    rows = cursor.fetchall()
    if not rows:
        return

    deleted_dates = set()

    with httpx.Client(timeout=10) as client:
        for row in rows:
            try:
                r = client.get(f"{BASE_URL}/fixtures?id={row['fixture_id']}", headers=HEADERS)
                data = r.json()["response"][0]

                cursor.execute("""
                    UPDATE pro_tips
                    SET home_score=%s, away_score=%s, status=%s, last_updated=NOW()
                    WHERE fixture_id=%s
                """, (
                    data["goals"]["home"] or 0,
                    data["goals"]["away"] or 0,
                    data["fixture"]["status"]["short"],
                    row["fixture_id"]
                ))

                if row['date'] not in deleted_dates:
                    redis_client.delete(f"fixtures:{row['date']}")
                    deleted_dates.add(row['date'])

            except Exception as e:
                print("Error:", e)

    conn.commit()
    cursor.close()
    conn.close()

scheduler.add_job(
    refresh_live_predictions,
    trigger=IntervalTrigger(seconds=90),
    id='live_refresh',
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