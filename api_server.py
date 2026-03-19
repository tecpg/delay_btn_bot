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

# ────────────────────────────────────────────────
# MODEL
# ────────────────────────────────────────────────
class FixtureOut(BaseModel):
    fixture_id: int
    league: str
    home_team: str
    away_team: str
    match_time: Optional[str]
    date: str
    home_score: Optional[int]
    away_score: Optional[int]
    status: Optional[str]

# ────────────────────────────────────────────────
# FIXTURES
# ────────────────────────────────────────────────

from datetime import date



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

        def normalize_row(r):
            if r.get("match_time"):
                r["match_time"] = str(r["match_time"])
            if r.get("date"):
                r["date"] = str(r["date"])
            if r.get("last_updated"):
                r["last_updated"] = r["last_updated"].isoformat()
            return r

        rows = [normalize_row(r) for r in rows]

        ttl = get_ttl(date.fromisoformat(fixture_date))
        set_cache(cache_key, rows, ttl)

        return rows

    finally:
        cursor.close()
        release_db(conn)



# ────────────────────────────────────────────────
# FIXTURE DETAILS
# ────────────────────────────────────────────────
@app.get("/fixture-details/{fixture_id}")
async def fixture_details(fixture_id: int):

    cache_key = f"fixture:{fixture_id}"
    cached = get_cache(cache_key)
    if cached:
        return cached

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
# SCHEDULER (🔥 OPTIMIZED)
# ────────────────────────────────────────────────
scheduler = BackgroundScheduler()

def refresh_live_predictions():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT fixture_id, date
            FROM pro_tips
            WHERE date = CURRENT_DATE
              AND (
                  last_updated IS NULL
                  OR last_updated < NOW() - INTERVAL '60 seconds'
              )
              AND (
                  status IN ('1H','HT','2H','LIVE')
                  OR (
                      status = 'NS'
                      AND match_time BETWEEN CURRENT_TIME
                      AND CURRENT_TIME + INTERVAL '1 hour'
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
                try:
                    fid = row["fixture_id"]

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
                    print("Scheduler error:", e)

        conn.commit()

    finally:
        cursor.close()
        release_db(conn)

scheduler.add_job(
    refresh_live_predictions,
    trigger=IntervalTrigger(seconds=90),
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
    return {"status": "ok", "time": datetime.utcnow().isoformat()}