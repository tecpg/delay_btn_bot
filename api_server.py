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
async def get_fixture_details(fixture_id: int):
    cache_key = f"fixture_full:{fixture_id}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    conn = get_db()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT full_json, status_short
            FROM fixture_details
            WHERE fixture_id = %s
        """, (fixture_id,))
        row = cursor.fetchone()

        if row:
            data = json.loads(row["full_json"])
            # Optional: check freshness based on status
            redis_client.setex(cache_key, CACHE_TTL_LONG if row["status_short"] in ["FT", "AET", "PEN"] else CACHE_TTL_SHORT, json.dumps(data))
            return data

    finally:
        cursor.close()
        conn.close()

    # Miss → fetch from API-Football
    async with httpx.AsyncClient() as client:
        try:
            # 1. Fixture main data
            fixture_resp = await client.get(f"{BASE_URL}/fixtures?id={fixture_id}", headers=HEADERS, timeout=12)
            fixture_resp.raise_for_status()
            fixture_data = fixture_resp.json()
            if not fixture_data.get("response"):
                raise HTTPException(404, "Fixture not found")
            fixture = fixture_data["response"][0]

            league_id = fixture["league"]["id"]
            season = fixture["league"]["season"]
            home_id = fixture["teams"]["home"]["id"]
            away_id = fixture["teams"]["away"]["id"]

            result: Dict[str, Any] = {
                "fixture": {
                    "fixture_id": fixture_id,
                    "home_team": fixture["teams"]["home"]["name"],
                    "away_team": fixture["teams"]["away"]["name"],
                    "date": fixture["fixture"]["date"],
                    "status": fixture["fixture"]["status"]["short"],
                    "score": fixture["goals"],
                }
            }

            # 2. Lineups
            lineup_resp = await client.get(f"{BASE_URL}/fixtures/lineups?fixture={fixture_id}", headers=HEADERS)
            lineups = []
            for team in lineup_resp.json().get("response", []):
                lineups.append({
                    "team_name": team["team"]["name"],
                    "formation": team.get("formation"),
                    "coach": team.get("coach", {}).get("name"),
                    "starters": [p["player"]["name"] for p in team.get("startXI", [])],
                    "substitutes": [p["player"]["name"] for p in team.get("substitutes", [])],
                })
            result["lineups"] = lineups

            # 3. Standings
            stand_resp = await client.get(f"{BASE_URL}/standings?league={league_id}&season={season}", headers=HEADERS)
            standings_data = stand_resp.json().get("response", [])
            home_standing = away_standing = None
            if standings_data:
                table = standings_data[0]["league"]["standings"][0]
                home_standing = next((t for t in table if t["team"]["id"] == home_id), None)
                away_standing = next((t for t in table if t["team"]["id"] == away_id), None)
            result["standings"] = {
                "home_team": {"rank": home_standing["rank"] if home_standing else None, "points": home_standing["points"] if home_standing else None},
                "away_team": {"rank": away_standing["rank"] if away_standing else None, "points": away_standing["points"] if away_standing else None},
            }

            # 4. Head-to-Head (last 5)
            h2h_resp = await client.get(f"{BASE_URL}/fixtures/headtohead?h2h={home_id}-{away_id}", headers=HEADERS)
            h2h = []
            for m in h2h_resp.json().get("response", [])[:5]:
                h2h.append({
                    "date": m["fixture"]["date"],
                    "home_team": m["teams"]["home"]["name"],
                    "away_team": m["teams"]["away"]["name"],
                    "home_goals": m["goals"]["home"],
                    "away_goals": m["goals"]["away"],
                })
            result["h2h"] = h2h

            # 5. Odds (1X2 from bookmaker 1)
            odds_resp = await client.get(f"{BASE_URL}/odds?fixture={fixture_id}&bookmaker=1", headers=HEADERS)
            odds = {"home": None, "draw": None, "away": None}
            if odds_resp.json().get("response"):
                try:
                    vals = odds_resp.json()["response"][0]["bookmakers"][0]["bets"][0]["values"]
                    odds = {v["value"].lower(): v["odd"] for v in vals if v["value"] in ["Home", "Draw", "Away"]}
                except:
                    pass
            result["odds"] = odds

            # 6. Statistics
            stats_resp = await client.get(f"{BASE_URL}/fixtures/statistics?fixture={fixture_id}", headers=HEADERS)
            stats = {}
            for team in stats_resp.json().get("response", []):
                team_name = team["team"]["name"]
                stats[team_name] = {s["type"]: s["value"] for s in team.get("statistics", [])}
            result["statistics"] = stats

            # Store in DB
            conn = get_db()
            cursor = conn.cursor()
            json_str = json.dumps(result)
            cursor.execute("""
                INSERT INTO fixture_details (fixture_id, league_id, season, home_team_id, away_team_id, full_json, status_short)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    full_json = VALUES(full_json),
                    status_short = VALUES(status_short),
                    last_updated = CURRENT_TIMESTAMP
            """, (fixture_id, league_id, season, home_id, away_id, json_str, result["fixture"]["status"]))
            conn.commit()
            cursor.close()
            conn.close()

            # Cache
            ttl = CACHE_TTL_SHORT if result["fixture"]["status"] not in ["FT", "AET", "PEN"] else CACHE_TTL_LONG
            redis_client.setex(cache_key, ttl, json.dumps(result))

            return result

        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal error: {str(exc)}")
        


# ────────────────────────────────────────────────
# HEALTH
# ────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}