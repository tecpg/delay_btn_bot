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


class FixtureOut(BaseModel):
    fixture_id: int
    league: str
    league_logo: Optional[str] = None
    league_country: Optional[str] = None

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
    elapsed: Optional[str] = None
    extra: Optional[str] = None

    source: Optional[str] = None
    last_updated: Optional[str] = None
    result_notification_sent: Optional[bool] = False


from pydantic import BaseModel
from typing import Optional

class NotificationPreference(BaseModel):
    device_id: str
    fixture_id: int
    enabled: bool

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



@app.post("/notifications/enable-fixture")
async def enable_fixture_notification(pref: NotificationPreference):
    """Enable/disable notifications for a specific fixture per device"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        if pref.enabled:
            # Add to notification preferences
            cursor.execute("""
                INSERT INTO device_fixture_notifications (device_id, fixture_id, enabled, created_at)
                VALUES (%s, %s, TRUE, NOW())
                ON CONFLICT (device_id, fixture_id) 
                DO UPDATE SET enabled = TRUE, updated_at = NOW()
            """, (pref.device_id, pref.fixture_id))
        else:
            # Disable or remove
            cursor.execute("""
                UPDATE device_fixture_notifications 
                SET enabled = FALSE, updated_at = NOW()
                WHERE device_id = %s AND fixture_id = %s
            """, (pref.device_id, pref.fixture_id))
        
        conn.commit()
        return {"status": "success"}
    finally:
        cursor.close()
        release_db(conn)

@app.get("/notifications/fixture-status/{device_id}/{fixture_id}")
async def get_fixture_notification_status(device_id: str, fixture_id: int):
    """Get notification status for a fixture"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT enabled FROM device_fixture_notifications 
        WHERE device_id = %s AND fixture_id = %s
    """, (device_id, fixture_id))
    
    result = cursor.fetchone()
    cursor.close()
    release_db(conn)
    
    return {"enabled": result[0] if result else False}
# ────────────────────────────────────────────────
# FIXTURES
# ────────────────────────────────────────────────



@app.post("/device/register")
async def register_device(registration: DeviceRegistration):
    """Register device for notifications"""
    await notification_service.register_device(
        registration.onesignal_player_id,
        {
            'device_model': registration.device_model,
            'app_version': registration.app_version
        }
    )
    return {"status": "registered", "message": "Device registered for notifications"}

@app.get("/device/status")
async def get_device_status(device_id: str):
    """Check if device is registered"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT is_active, last_active 
        FROM devices 
        WHERE device_id = %s
    """, (device_id,))
    
    result = cursor.fetchone()
    cursor.close()
    release_db(conn)
    
    if result:
        return {"registered": True, "is_active": result[0], "last_active": result[1]}
    return {"registered": False}

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


@app.get("/fixtures/premium/{fixture_date}", response_model=List[FixtureOut])
def get_premium_fixtures(fixture_date: str):

    if fixture_date == "today":
        fixture_date = str(date.today())

    cache_key = f"fixtures_premium:{fixture_date}"
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
            ORDER BY date DESC
            LIMIT 3 OFFSET 4
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

                row["match_datetime"] = dt.isoformat()
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
        set_cache(cache_key, result, ttl)

        return result

    finally:
        cursor.close()
        release_db(conn)


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
  
  

# ────────────────────────────────────────────────
# HEALTH
# ────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}



