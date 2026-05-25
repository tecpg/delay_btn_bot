from fastapi import FastAPI, HTTPException
from typing import List, Optional, Dict, Any
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import redis
import json
import httpx
from datetime import date, datetime
import kbt_load_env
import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any

import httpx
from zoneinfo import ZoneInfo
from psycopg2.extras import RealDictCursor

from notification_service import MatchNotificationService

CACHE_TTL_SHORT = 30      # live
CACHE_TTL_MEDIUM = 600    # upcoming
CACHE_TTL_LONG = 86400 * 3  # finished


# ────────────────────────────────────────────────
# INIT
# ────────────────────────────────────────────────
app = FastAPI(title="Match Fixtures API")

redis_client = redis.from_url(kbt_load_env.redis_url, decode_responses=True)

BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": kbt_load_env.api_football_key}



# Initialize notification service
notification_service = MatchNotificationService()


from datetime import date

from zoneinfo import ZoneInfo

# main.py - Add these endpoints

from pydantic import BaseModel
from typing import Optional

class DeviceRegistration(BaseModel):
    onesignal_player_id: str
    device_model: Optional[str] = None
    app_version: Optional[str] = None

from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime


from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime, date


class FixtureOut(BaseModel):
    fixture_id: int
    league: str
    league_logo: Optional[str] = None
    league_country: Optional[str] = None

    home_team: str
    home_logo: Optional[str] = None

    away_team: str
    away_logo: Optional[str] = None

    match_time: Optional[str] = None
    date: Optional[str] = None
    match_datetime: Optional[str] = None

    prediction: Optional[str] = None
    odd: Optional[str] = None

    home_score: Optional[int] = None
    away_score: Optional[int] = None
    status: Optional[str] = None
    elapsed: Optional[str] = None
    extra: Optional[str] = None

    source: Optional[str] = None
    last_updated: Optional[str] = None
    result_notification_sent: bool = False

    # 🔥 FIX 1: convert date (date → string)
    @field_validator('date', mode='before')
    @classmethod
    def convert_date(cls, v):
        if isinstance(v, date):
            return v.strftime("%Y-%m-%d")
        return v

    # 🔥 FIX 2: convert match_datetime (datetime → ISO string)
    @field_validator('match_datetime', mode='before')
    @classmethod
    def convert_match_datetime(cls, v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    # 🔥 FIX 3: convert last_updated (datetime → ISO string)
    @field_validator('last_updated', mode='before')
    @classmethod
    def convert_last_updated(cls, v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    # 🔥 FIX 4: normalize odd values
    @field_validator('odd', mode='before')
    @classmethod
    def convert_odd(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return f"{float(v):.2f}"
        return str(v)

    # 🔥 FIX 5: prevent None for required strings
    @field_validator('league', 'home_team', 'away_team', mode='before')
    @classmethod
    def prevent_none_strings(cls, v):
        return v or ""

    # 🔥 FIX 6: safe score conversion
    @field_validator('home_score', 'away_score', mode='before')
    @classmethod
    def convert_scores(cls, v):
        if v in ("", None):
            return None
        try:
            return int(v)
        except:
            return None    

class NotificationPreference(BaseModel):
    user_id: str
    fixture_id: int
    enabled: bool

class DeviceRegistration(BaseModel):
    user_id: str
    device_model: str
    app_version: str
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
    


def get_fixture_ttl(status: str):
    if status in ["FT", "AET", "PEN"]:
        return CACHE_TTL_LONG
    elif status in ["1H", "2H", "LIVE", "HT"]:
        return CACHE_TTL_SHORT
    else:
        return CACHE_TTL_MEDIUM


def process_form_data(fixtures_data, current_team_name):
    """Process form data for a specific team - returns complete match details"""
    form_results = []
    
    for match in fixtures_data[:5]:
        home_team = match.get("teams", {}).get("home", {}).get("name", "")
        away_team = match.get("teams", {}).get("away", {}).get("name", "")
        home_team_logo = match.get("teams", {}).get("home", {}).get("logo", "")
        away_team_logo = match.get("teams", {}).get("away", {}).get("logo", "")
        home_goals = match.get("goals", {}).get("home") or 0
        away_goals = match.get("goals", {}).get("away") or 0
        
        # Determine if current team is home or away in this fixture
        if home_team == current_team_name:
            # Current team played at HOME
            our_score = home_goals
            their_score = away_goals
            opponent = away_team
            opponent_logo = away_team_logo
            location = "home"
            was_home = True
        elif away_team == current_team_name:
            # Current team played AWAY
            our_score = away_goals
            their_score = home_goals
            opponent = home_team
            opponent_logo = home_team_logo
            location = "away"
            was_home = False
        else:
            # Current team not found in this fixture (skip)
            continue
        
        # Calculate result
        if our_score > their_score:
            result = "W"
        elif our_score == their_score:
            result = "D"
        else:
            result = "L"
        
        score_display = f"{our_score}-{their_score}"
        league_name = match.get("league", {}).get("name", "Unknown League")
        league_logo = match.get("league", {}).get("logo", "")
        league_country = match.get("league", {}).get("country", "")
        
        # Extract match date
        match_date = match.get("fixture", {}).get("date")
        formatted_date = None
        formatted_datetime = None
        
        if match_date:
            try:
                dt = datetime.fromisoformat(match_date.replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                formatted_date = dt.strftime("%Y-%m-%d")
                formatted_datetime = dt.isoformat()
                match_time = dt.strftime("%H:%M")
            except:
                formatted_date = match_date[:10] if len(match_date) >= 10 else match_date
                formatted_datetime = match_date
                match_time = match_date[11:16] if len(match_date) >= 16 else None
        
        # Get fixture status
        fixture_status = match.get("fixture", {}).get("status", {}).get("short", "")
        elapsed = match.get("fixture", {}).get("status", {}).get("elapsed", None)
        
        form_results.append({
            # Core match info
            "fixture_id": match.get("fixture", {}).get("id"),
            "match_datetime": formatted_datetime,
            "date": formatted_date,
            "match_time": match_time,
            "status": fixture_status,
            "elapsed": elapsed,
            
            # League info
            "league": league_name,
            "league_logo": league_logo,
            "league_country": league_country,
            
            # Home team details
            "home_team": home_team,
            "home_logo": home_team_logo,
            "home_score": home_goals,
            
            # Away team details
            "away_team": away_team,
            "away_logo": away_team_logo,
            "away_score": away_goals,
            
            # Current team perspective
            "current_team": current_team_name,
            "current_team_score": our_score,
            "opponent": opponent,
            "opponent_logo": opponent_logo,
            "opponent_score": their_score,
            "result": result,
            "score": score_display,
            "location": location,  # "home" or "away"
            "was_home": was_home,
            
            # Additional details
            "round": match.get("league", {}).get("round", ""),
            "season": match.get("league", {}).get("season", ""),
        })
    
    return form_results


# ────────────────────────────────────────────────
# NOTIFICATION
# ────────────────────────────────────────────────

@app.post("/notifications/enable-fixture")
async def enable_fixture_notification(pref: NotificationPreference):
    conn = get_db()
    cursor = conn.cursor()

    try:
        if pref.enabled:
            cursor.execute("""
                INSERT INTO device_fixture_notifications (user_id, fixture_id, enabled, created_at)
                VALUES (%s, %s, TRUE, NOW())
                ON CONFLICT (user_id, fixture_id)
                DO UPDATE SET enabled = TRUE, updated_at = NOW()
            """, (pref.user_id, pref.fixture_id))
        else:
            cursor.execute("""
                UPDATE device_fixture_notifications
                SET enabled = FALSE, updated_at = NOW()
                WHERE user_id = %s AND fixture_id = %s
            """, (pref.user_id, pref.fixture_id))

        conn.commit()
        return {"status": "success"}

    finally:
        cursor.close()
        release_db(conn)

@app.get("/notifications/fixture-status/{user_id}/{fixture_id}")
async def get_fixture_notification_status(user_id: str, fixture_id: int):
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT enabled FROM device_fixture_notifications
        WHERE user_id = %s AND fixture_id = %s
    """, (user_id, fixture_id))

    result = cursor.fetchone()

    cursor.close()
    release_db(conn)

    return {"enabled": result[0] if result else False}


@app.post("/device/register")
async def register_device(registration: DeviceRegistration):
    await notification_service.register_user(
        registration.user_id,
        {
            'device_model': registration.device_model,
            'app_version': registration.app_version
        }
    )
    return {"status": "registered"}


@app.get("/device/status")
async def get_device_status(user_id: str):
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT last_active
        FROM users
        WHERE user_id = %s
    """, (user_id,))

    result = cursor.fetchone()

    cursor.close()
    release_db(conn)

    return {"registered": bool(result)}
# Manual trigger endpoints for testing
@app.post("/notifications/test-reminder/{fixture_id}")
async def test_reminder(fixture_id: int):
    """Manually trigger a test reminder (for debugging)"""
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("SELECT * FROM pro_tips WHERE fixture_id = %s", (fixture_id,))
    fixture = cursor.fetchone()
    cursor.close()
    release_db(conn)
    
    if fixture:
        await notification_service.send_match_reminder(fixture)
        return {"status": "test_reminder_sent", "fixture_id": fixture_id}
    return {"error": "Fixture not found"}

# Add to your main.py or api_server.py

@app.get("/notifications/debug/fixture/{fixture_id}")
async def debug_fixture_notifications(fixture_id: int):
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("""
        SELECT fixture_id, home_team, away_team, match_datetime, status
        FROM pro_tips WHERE fixture_id = %s
    """, (fixture_id,))

    fixture = cursor.fetchone()

    cursor.execute("""
        SELECT user_id, enabled, created_at, updated_at
        FROM device_fixture_notifications
        WHERE fixture_id = %s AND enabled = TRUE
    """, (fixture_id,))

    users = cursor.fetchall()

    cursor.close()
    release_db(conn)

    return {
        "fixture": fixture,
        "users_enabled": len(users),
        "users": users
    }
# ────────────────────────────────────────────────
# FIXTURES
# ────────────────────────────────────────────────
from fastapi import HTTPException
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo


@app.get("/fixtures/vip", response_model=List[FixtureOut])
def get_vip():

    cache_key = "vip_today"
    cached = get_cache(cache_key)
    if cached:
        return cached

    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # 🔥 FETCH VIP PICKS (READ-ONLY)
        cursor.execute("""
            SELECT p.*
            FROM pro_tips p
            JOIN vip_tips v ON p.fixture_id = v.fixture_id
            WHERE v.vip_date = CURRENT_DATE
            ORDER BY p.id DESC
        """)

        rows = cursor.fetchall()

        result = []

        # ✅ LOOP MUST BE INSIDE TRY
        for r in rows:
            row = dict(r)

            # ✅ HANDLE match_datetime
            if row.get("match_datetime"):
                dt = row["match_datetime"]

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))

                row["match_datetime"] = dt.isoformat()
                row["match_time"] = dt.strftime("%H:%M")
                row["date"] = dt.strftime("%Y-%m-%d")

            else:
                row["match_time"] = None
                row["date"] = str(row.get("date"))

            # ✅ serialize last_updated
            if row.get("last_updated"):
                row["last_updated"] = row["last_updated"].isoformat()

            # ✅ filter bad rows
            if not row.get("fixture_id"):
                continue
            if not row.get("home_team") or not row.get("away_team"):
                continue

            result.append(row)

        # ✅ AFTER LOOP (not inside it)
        if not result:
            return []

        set_cache(cache_key, result, 300)
        return result

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, "Failed to fetch VIP fixtures")

    finally:
        cursor.close()
        release_db(conn)
    # ====================== VIP HISTORY (Grouped by Date) ======================


@app.get("/fixtures/vip-history")
def get_vip_history():

    cache_key = "vip_history"
    cached = get_cache(cache_key)
    if cached:
        return cached

    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:

        cursor.execute("""
    SELECT p.*, v.vip_date
    FROM vip_tips v
    JOIN pro_tips p ON p.fixture_id = v.fixture_id
    WHERE v.vip_date < CURRENT_DATE
      AND v.vip_date >= CURRENT_DATE - INTERVAL '14 days'
    ORDER BY v.vip_date DESC, p.id DESC
""")

        rows = cursor.fetchall()
        grouped = {}

        for r in rows:
            row = dict(r)

            # ✅ HANDLE match_datetime (same pattern everywhere)
            if row.get("match_datetime"):
                dt = row["match_datetime"]

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))

                row["match_datetime"] = dt.isoformat()
                row["match_time"] = dt.strftime("%H:%M")
                row["date"] = dt.strftime("%Y-%m-%d")

            else:
                row["match_time"] = None
                row["date"] = str(row.get("date"))

            # ✅ serialize last_updated
            if row.get("last_updated"):
                row["last_updated"] = row["last_updated"].isoformat()

            # 🔥 normalize vip_date (IMPORTANT)
            group_date = str(row.get("vip_date"))

            # ✅ filter bad rows
            if not row.get("fixture_id"):
                continue
            if not row.get("home_team") or not row.get("away_team"):
                continue

            try:
                item = FixtureOut(**row)
            except Exception as e:
                print("❌ VIP HISTORY BAD ROW:", row.get("fixture_id"), e)
                continue

            if group_date not in grouped:
                grouped[group_date] = []

            grouped[group_date].append(item)

        # ✅ safe empty
        if not grouped:
            return []

        # ✅ format response
        result = [
            {
                "date": d,
                "fixtures": grouped[d]
            }
            for d in sorted(grouped.keys(), reverse=True)
        ]

        set_cache(cache_key, result, 1800)
        return result

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, "Failed to fetch VIP history")

    finally:
        cursor.close()
        release_db(conn) 


@app.post("/fixtures/vip-updates")
def get_vip_updates(fixture_ids: List[int]):

    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT 
                fixture_id,
                home_score,
                away_score,
                status,
                elapsed
            FROM pro_tips
            WHERE fixture_id = ANY(%s)
        """, (fixture_ids,))

        rows = cursor.fetchall()

        return rows

    finally:
        cursor.close()
        release_db(conn)



from fastapi.responses import JSONResponse
import json

@app.get("/fixtures/{fixture_date}", response_model=List[FixtureOut])
def get_fixtures(fixture_date: str):

    if fixture_date == "today":
        fixture_date = str(date.today())

    cache_key = f"fixtures:{fixture_date}"
    cached = get_cache(cache_key)

    if cached:
        if isinstance(cached, str):
            cached = json.loads(cached)

        return JSONResponse(
            content=cached,
            media_type="application/json; charset=utf-8"
        )

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

            if row.get("match_datetime"):
                dt = row["match_datetime"]

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))

                row["match_datetime"] = dt.isoformat()
                row["match_time"] = dt.strftime("%H:%M")
                row["date"] = dt.strftime("%Y-%m-%d")
            else:
                row["match_time"] = None
                row["date"] = fixture_date

            if row.get("last_updated"):
                row["last_updated"] = row["last_updated"].isoformat()

            result.append(row)

        ttl = get_ttl(date.fromisoformat(fixture_date))
        set_cache(cache_key, result, ttl)

        return JSONResponse(
            content=result,
            media_type="application/json; charset=utf-8"
        )

    finally:
        cursor.close()
        release_db(conn)

# ────────────────────────────────────────────────
# FIXTURE DETAILS
# ────────────────────────────────────────────────


@app.get("/fixture-details/{fixture_id}")
async def get_fixture_details(fixture_id: int):

    cache_key = f"fixture_full:{fixture_id}"

    # ─────────────────────────────
    # 1. REDIS CACHE
    # ─────────────────────────────
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    # ─────────────────────────────
    # 2. DATABASE CHECK (FRESHNESS)
    # ─────────────────────────────
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT full_json, status_short, last_updated
            FROM fixture_details
            WHERE fixture_id = %s
        """, (fixture_id,))

        row = cursor.fetchone()

        if row:
            data = row["full_json"] if isinstance(row["full_json"], dict) else json.loads(row["full_json"])

            status = row["status_short"]
            last_updated = row["last_updated"]

            # 🧠 freshness logic
            age = (datetime.now(timezone.utc) - last_updated).total_seconds()

            if status in ["FT", "AET", "PEN"]:
                redis_client.setex(cache_key, CACHE_TTL_LONG, json.dumps(data))
                return data

            if age < 60:  # still fresh
                redis_client.setex(cache_key, CACHE_TTL_SHORT, json.dumps(data))
                return data

    finally:
        cursor.close()
        release_db(conn)

    # ─────────────────────────────
    # 3. FETCH FROM API (SMART)
    # ─────────────────────────────
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            # 🔥 ALWAYS FETCH FIXTURE
            fixture_resp = await client.get(
                f"{BASE_URL}/fixtures?id={fixture_id}",
                headers=HEADERS
            )
            fixture_resp.raise_for_status()

            fixture_data = fixture_resp.json()
            if not fixture_data.get("response"):
                raise HTTPException(404, "Fixture not found")

            fixture = fixture_data["response"][0]

            league_id = fixture["league"]["id"]
            season = fixture["league"]["season"]
            home_id = fixture["teams"]["home"]["id"]
            away_id = fixture["teams"]["away"]["id"]

            status = fixture["fixture"]["status"]["short"]

            # ✅ TIME (UTC)
            dt = datetime.fromisoformat(fixture["fixture"]["date"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("UTC"))

            result: Dict[str, Any] = {
                "fixture": {
                    "fixture_id": fixture_id,
                    "home_team": fixture["teams"]["home"]["name"],
                    "away_team": fixture["teams"]["away"]["name"],
                    "match_datetime": dt.isoformat(),
                    "status": status,
                    "score": fixture["goals"],
                }
            }

            # ─────────────────────────────
            # 4. CONDITIONAL PARALLEL CALLS
            # ─────────────────────────────
            tasks = []

            # 0. Lineups
            if status != "NS":
                tasks.append(client.get(f"{BASE_URL}/fixtures/lineups?fixture={fixture_id}", headers=HEADERS))
            else:
                tasks.append(None)

            # 1. Standings
            tasks.append(client.get(f"{BASE_URL}/standings?league={league_id}&season={season}", headers=HEADERS))

            # 2. H2H
            tasks.append(client.get(f"{BASE_URL}/fixtures/headtohead?h2h={home_id}-{away_id}", headers=HEADERS))

            # 3. Odds
            if status not in ["FT", "AET", "PEN"]:
                tasks.append(client.get(f"{BASE_URL}/odds?fixture={fixture_id}&bookmaker=1", headers=HEADERS))
            else:
                tasks.append(None)

            # 4. Stats
            if status in ["1H", "2H", "LIVE", "HT"]:
                tasks.append(client.get(f"{BASE_URL}/fixtures/statistics?fixture={fixture_id}", headers=HEADERS))
            else:
                tasks.append(None)

            # 5. Events
            tasks.append(client.get(f"{BASE_URL}/fixtures/events?fixture={fixture_id}", headers=HEADERS))

            # 6. Home Form
            tasks.append(client.get(f"{BASE_URL}/fixtures?team={home_id}&last=5", headers=HEADERS))

            # 7. Away Form
            tasks.append(client.get(f"{BASE_URL}/fixtures?team={away_id}&last=5", headers=HEADERS))

            responses = await asyncio.gather(*[t for t in tasks if t is not None], return_exceptions=True)

            idx = 0

            # ───────── LINEUPS ─────────
      
            # ───────── LINEUPS ─────────
            if tasks[0]:
                lineup_resp = responses[idx]; idx += 1
                
                result["lineups"] = []
                for t in lineup_resp.json().get("response", []):
                    lineup_data = {
                        "team": t.get("team", {}).get("name", ""),
                        "team_id": t.get("team", {}).get("id"),
                        "team_logo": t.get("team", {}).get("logo", ""),
                        "formation": t.get("formation", ""),
                        "coach": t.get("coach", {}).get("name", ""),
                        "players": [],
                        "substitutes": []
                    }
                    
                    # STARTING XI
                    for p in t.get("startXI", []):
                        player = p.get("player", {})
                        lineup_data["players"].append({
                            "name": player.get("name", ""),
                            "number": player.get("number"),
                            "pos": player.get("pos", ""),
                            "grid": player.get("grid")  # Position on pitch (e.g., "1:1")
                        })
                    
                    # SUBSTITUTES
                    for p in t.get("substitutes", []):
                        player = p.get("player", {})
                        lineup_data["substitutes"].append({
                            "name": player.get("name", ""),
                            "number": player.get("number"),
                            "pos": player.get("pos", "")
                        })
                    
                    result["lineups"].append(lineup_data) 
                    
             # ───────── STANDINGS ─────────
            stand_resp = responses[idx]; idx += 1
            result["standings"] = stand_resp.json().get("response", [])

            # ───────── H2H ─────────
            h2h_resp = responses[idx]; idx += 1
            result["h2h"] = h2h_resp.json().get("response", [])[:5]

            # ───────── ODDS ─────────
            if tasks[3]:
                odds_resp = responses[idx]; idx += 1
                result["odds"] = odds_resp.json().get("response", [])
            else:
                result["odds"] = None

            # ───────── STATS ─────────
            if tasks[4]:
                stats_resp = responses[idx]; idx += 1
                result["statistics"] = stats_resp.json().get("response", [])
            else:
                result["statistics"] = None

            # ───────── EVENTS ─────────
            events_resp = responses[idx]; idx += 1

            result["events"] = [
                {
                    "time": e["time"]["elapsed"],
                    "team": e["team"]["name"],
                    "type": e["type"],        # Goal, Card, subst
                    "detail": e["detail"],    # Yellow Card, Substitution
                    "player": e["player"]["name"] if e.get("player") else None,
                    "assist": e["assist"]["name"] if e.get("assist") else None
                }
                for e in events_resp.json().get("response", [])
            ]

         # In your fixture details endpoint:

            # Get current team names
            home_team_name = fixture["teams"]["home"]["name"]  # "Barcelona"
            away_team_name = fixture["teams"]["away"]["name"]  # "Atletico Madrid"

            # ───────── HOME FORM (for Barcelona) ─────────
            home_form_resp = responses[idx]; idx += 1
            home_form_data = home_form_resp.json().get("response", []) if not isinstance(home_form_resp, Exception) else []
            result["home_form"] = process_form_data(home_form_data, home_team_name)

            # ───────── AWAY FORM (for Atletico Madrid) ─────────
            away_form_resp = responses[idx]; idx += 1
            away_form_data = away_form_resp.json().get("response", []) if not isinstance(away_form_resp, Exception) else []
            result["away_form"] = process_form_data(away_form_data, away_team_name)             # 5. SAVE TO DATABASE
                        # ─────────────────────────────
            conn = get_db()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO fixture_details (
                    fixture_id, league_id, season,
                    home_team_id, away_team_id,
                    full_json, status_short, last_updated
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (fixture_id)
                DO UPDATE SET
                    full_json = EXCLUDED.full_json,
                    status_short = EXCLUDED.status_short,
                    last_updated = NOW()
            """, (
                fixture_id,
                league_id,
                season,
                home_id,
                away_id,
                json.dumps(result),
                status
            ))

            conn.commit()
            cursor.close()
            release_db(conn)

            # ─────────────────────────────
            # 6. CACHE FINAL RESULT
            # ─────────────────────────────
            ttl = get_fixture_ttl(status)
            redis_client.setex(cache_key, ttl, json.dumps(result))

            return result

        except httpx.HTTPStatusError as exc:
            raise HTTPException(exc.response.status_code, str(exc))

        except Exception as exc:
            raise HTTPException(500, f"Internal error: {str(exc)}")
  
  



# Add this import at the top if not already present
from fastapi import HTTPException, Depends
from typing import Optional

# Add this endpoint after your existing fixture endpoints
@app.get("/fixture/{fixture_id}")
async def get_single_fixture(fixture_id: int):
    """
    Fetch fresh fixture data for notification clicks.
    Used when users click on notifications to get the latest match data.
    Only fetches from pro_tips table.
    """
    cache_key = f"api_fixture:{fixture_id}"
    
    # 🔥 Check Redis cache first (short TTL for live matches)
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Fetch fixture data ONLY from pro_tips table
        cursor.execute("""
            SELECT 
                fixture_id,
                home_team,
                away_team,
                home_logo,
                away_logo,
                league,
                league_country,
                league_logo,
                match_datetime,
                date,
                status,
                elapsed,
                home_score,
                away_score,
                odd,
                prediction,
                extra,
                source,
                last_updated,
                result_notification_sent
            FROM pro_tips
            WHERE fixture_id = %s
            LIMIT 1
        """, (fixture_id,))
        
        fixture = cursor.fetchone()
        
        if not fixture:
            raise HTTPException(status_code=404, detail=f"Fixture {fixture_id} not found in pro_tips table")
        
        # Convert datetime objects to strings for JSON serialization
        if fixture.get('match_datetime'):
            if isinstance(fixture['match_datetime'], datetime):
                fixture['match_datetime'] = fixture['match_datetime'].isoformat()
        
        if fixture.get('last_updated'):
            if isinstance(fixture['last_updated'], datetime):
                fixture['last_updated'] = fixture['last_updated'].isoformat()
        
        if fixture.get('date'):
            if isinstance(fixture['date'], (date, datetime)):
                fixture['date'] = str(fixture['date'])
        
        # Convert None values to empty strings for better mobile handling
        for key, value in fixture.items():
            if value is None:
                fixture[key] = ""
            elif isinstance(value, (int, float)) and key in ['home_score', 'away_score']:
                fixture[key] = str(value) if value is not None else "0"
            elif isinstance(value, bool):
                fixture[key] = value
        
        # Add a timestamp to help mobile apps know when this data was fetched
        fixture['server_timestamp'] = datetime.now(timezone.utc).isoformat()
        
        # 🔥 Cache based on match status
        status = fixture.get('status', 'NS')
        if status in ["FT", "AET", "PEN"]:
            ttl = CACHE_TTL_LONG  # 3 days for finished matches
        elif status in ["1H", "2H", "LIVE", "HT"]:
            ttl = CACHE_TTL_SHORT  # 30 seconds for live matches
        else:
            ttl = CACHE_TTL_MEDIUM  # 10 minutes for upcoming matches
        
        # Cache the result
        redis_client.setex(cache_key, ttl, json.dumps(fixture, default=json_serializer))
        
        return JSONResponse(
            content=fixture,
            media_type="application/json; charset=utf-8"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching fixture: {str(e)}")
    
    finally:
        cursor.close()
        release_db(conn)


# ────────────────────────────────────────────────
# BOOKING CODES
# ────────────────────────────────────────────────

@app.get("/betcodes/today")
def get_betcodes_today():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT *
            FROM booking_codes
            WHERE post_date = CURRENT_DATE
            ORDER BY post_time DESC
        """)

        rows = cursor.fetchall()

        # optional: normalize datetime
        for r in rows:
            if r.get("post_time"):
                r["post_time"] = str(r["post_time"])
            if r.get("post_date"):
                r["post_date"] = str(r["post_date"])

        return rows

    finally:
        cursor.close()
        release_db(conn)


@app.get("/betcodes/today/grouped-sql")
def get_betcodes_grouped_sql():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT site, json_agg(b ORDER BY post_time DESC) AS data
            FROM booking_codes b
            WHERE post_date = CURRENT_DATE
            GROUP BY site
        """)

        rows = cursor.fetchall()

        return {r["site"]: r["data"] for r in rows}

    finally:
        cursor.close()
        release_db(conn)

# ────────────────────────────────────────────────
# HEALTH
# ────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}



