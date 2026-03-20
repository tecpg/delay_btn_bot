from fastapi import FastAPI, HTTPException
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import redis
import json
import httpx
from datetime import date, datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import kbt_load_env

# ────────────────────────────────────────────────
# INIT
# ────────────────────────────────────────────────
app = FastAPI(title="Match Fixtures API")

redis_client = redis.from_url(kbt_load_env.redis_url, decode_responses=True)

BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": kbt_load_env.api_football_key}

# ────────────────────────────────────────────────
# DB POOL (🔥 PERFORMANCE BOOST)
# ────────────────────────────────────────────────
db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=kbt_load_env.supabase_url
)

def get_db():
    return db_pool.getconn()

def release_db(conn):
    db_pool.putconn(conn)

# ────────────────────────────────────────────────
# SERIALIZER (🔥 AUTO FIX JSON)
# ────────────────────────────────────────────────
def json_serializer(obj):
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)

def set_cache(key, value, ttl):
    redis_client.setex(key, ttl, json.dumps(value, default=json_serializer))

def get_cache(key):
    data = redis_client.get(key)
    return json.loads(data) if data else None

# ────────────────────────────────────────────────
# SMART TTL
# ────────────────────────────────────────────────
def get_ttl(match_date: date):
    today = date.today()
    if match_date < today:
        return 3600      # 1h
    elif match_date == today:
        return 300       # 5 min
    else:
        return 86400     # 1 day
    
    

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

    # 🔥 NEW (optional but powerful)
    match_datetime: Optional[str] = None

    prediction: Optional[str] = None
    odd: Optional[str] = None

    home_score: Optional[int] = None
    away_score: Optional[int] = None
    status: Optional[str] = None

    source: Optional[str] = None
    last_updated: Optional[str] = None


# ────────────────────────────────────────────────
# FIXTURES
# ────────────────────────────────────────────────

from datetime import date

from zoneinfo import ZoneInfo

@app.get("/fixtures/{fixture_date}", response_model=List[FixtureOut])
def get_fixtures(fixture_date: str):

    if fixture_date == "today":
        fixture_date = str(date.today())

    cache_key = f"fixtures:{fixture_date}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT *
            FROM pro_tips
            WHERE date = %s
            ORDER BY match_time DESC
        """, (fixture_date,))

        rows = cursor.fetchall()

        result = []

        for r in rows:
            row = dict(r)

            # ✅ HANDLE match_datetime (UTC ONLY)
            if row.get("match_datetime"):
                dt = row["match_datetime"]

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))

                # 🔥 KEEP UTC ONLY (client will convert)
                row["match_datetime"] = dt.isoformat()

                # optional fallback display
                row["match_time"] = dt.strftime("%H:%M")
                row["date"] = dt.strftime("%Y-%m-%d")

            else:
                row["match_time"] = None
                row["date"] = fixture_date

            # ✅ serialize last_updated
            if row.get("last_updated"):
                row["last_updated"] = row["last_updated"].isoformat()

            result.append(row)

        ttl = get_ttl(date.fromisoformat(fixture_date))

        # ✅ cache CORRECT data
        set_cache(cache_key, result, ttl)

        return result

    finally:
        cursor.close()
        release_db(conn)

# ────────────────────────────────────────────────
# FIXTURE DETAILS
# ────────────────────────────────────────────────
@app.get("/fixture-details/{fixture_id}")
async def fixture_details(fixture_id: int):

    cache_key = f"fixture:{fixture_id}"
    # cached = get_cache(cache_key)
    # if cached:
    #     return cached

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(f"{BASE_URL}/fixtures?id={fixture_id}", headers=HEADERS)
            resp.raise_for_status()

            fixture = resp.json()["response"][0]

            result = {
                "fixture_id": fixture_id,
                "home": fixture["teams"]["home"]["name"],
                "away": fixture["teams"]["away"]["name"],
                "score": fixture["goals"],
                "status": fixture["fixture"]["status"]["short"],
            }

            set_cache(cache_key, result, 300)
            return result

        except Exception as e:
            raise HTTPException(500, str(e))


# ────────────────────────────────────────────────
# HEALTH
# ────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}