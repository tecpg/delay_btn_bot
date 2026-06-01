import json
import asyncio
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import httpx
import psycopg2
import redis
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from pydantic import BaseModel, field_validator

import kbt_load_env
from notification_service import MatchNotificationService

# ────────────────────────────────────────────────
# LOGGING
# ────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ────────────────────────────────────────────────
# CACHE TTL CONSTANTS
# ────────────────────────────────────────────────
CACHE_TTL_SHORT  = 30           # live matches
CACHE_TTL_MEDIUM = 600          # upcoming
CACHE_TTL_LONG   = 86400 * 3   # finished

# ────────────────────────────────────────────────
# INIT
# ────────────────────────────────────────────────
app = FastAPI(title="Match Fixtures API")

redis_client = redis.from_url(kbt_load_env.redis_url, decode_responses=True)

BASE_URL = "https://v3.football.api-sports.io"
HEADERS  = {"x-apisports-key": kbt_load_env.api_football_key}

notification_service = MatchNotificationService()

# ────────────────────────────────────────────────
# DB POOL
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
# REDIS CACHE  (✅ graceful — never crashes a request)
# ────────────────────────────────────────────────
def json_serializer(obj):
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)

def get_cache(key):
    try:
        data = redis_client.get(key)
        return json.loads(data) if data else None
    except Exception as e:
        logging.warning(f"⚠️ Redis get failed for '{key}': {e}")
        return None  # treat as cache miss

def set_cache(key, value, ttl):
    try:
        redis_client.setex(key, ttl, json.dumps(value, default=json_serializer))
    except Exception as e:
        logging.warning(f"⚠️ Redis set failed for '{key}': {e}")
        # non-fatal — endpoint still returns data, just uncached

def redis_get(key):
    """Direct Redis get with graceful failure."""
    try:
        return redis_client.get(key)
    except Exception as e:
        logging.warning(f"⚠️ Redis get failed for '{key}': {e}")
        return None

def redis_setex(key, ttl, value):
    """Direct Redis setex with graceful failure."""
    try:
        redis_client.setex(key, ttl, value)
    except Exception as e:
        logging.warning(f"⚠️ Redis setex failed for '{key}': {e}")

# ────────────────────────────────────────────────
# SMART TTL
# ────────────────────────────────────────────────
def get_ttl(match_date: date):
    today = date.today()
    if match_date < today:
        return 3600
    elif match_date == today:
        return 300
    else:
        return 86400

def get_fixture_ttl(status: str):
    if status in ["FT", "AET", "PEN"]:
        return CACHE_TTL_LONG
    elif status in ["1H", "2H", "LIVE", "HT"]:
        return CACHE_TTL_SHORT
    else:
        return CACHE_TTL_MEDIUM

# ────────────────────────────────────────────────
# MODELS
# ────────────────────────────────────────────────
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

    @field_validator('date', mode='before')
    @classmethod
    def convert_date(cls, v):
        if isinstance(v, date):
            return v.strftime("%Y-%m-%d")
        return v

    @field_validator('match_datetime', mode='before')
    @classmethod
    def convert_match_datetime(cls, v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    @field_validator('last_updated', mode='before')
    @classmethod
    def convert_last_updated(cls, v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    @field_validator('odd', mode='before')
    @classmethod
    def convert_odd(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return f"{float(v):.2f}"
        return str(v)

    @field_validator('league', 'home_team', 'away_team', mode='before')
    @classmethod
    def prevent_none_strings(cls, v):
        return v or ""

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
# FORM DATA HELPER
# ────────────────────────────────────────────────
def process_form_data(fixtures_data, current_team_name):
    form_results = []

    for match in fixtures_data[:5]:
        home_team      = match.get("teams", {}).get("home", {}).get("name", "")
        away_team      = match.get("teams", {}).get("away", {}).get("name", "")
        home_team_logo = match.get("teams", {}).get("home", {}).get("logo", "")
        away_team_logo = match.get("teams", {}).get("away", {}).get("logo", "")
        home_goals     = match.get("goals", {}).get("home") or 0
        away_goals     = match.get("goals", {}).get("away") or 0

        if home_team == current_team_name:
            our_score, their_score = home_goals, away_goals
            opponent, opponent_logo = away_team, away_team_logo
            location, was_home = "home", True
        elif away_team == current_team_name:
            our_score, their_score = away_goals, home_goals
            opponent, opponent_logo = home_team, home_team_logo
            location, was_home = "away", False
        else:
            continue

        if our_score > their_score:
            result = "W"
        elif our_score == their_score:
            result = "D"
        else:
            result = "L"

        league_name    = match.get("league", {}).get("name", "Unknown League")
        league_logo    = match.get("league", {}).get("logo", "")
        league_country = match.get("league", {}).get("country", "")

        match_date        = match.get("fixture", {}).get("date")
        formatted_date    = None
        formatted_datetime = None
        match_time        = None

        if match_date:
            try:
                dt = datetime.fromisoformat(match_date.replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                formatted_date     = dt.strftime("%Y-%m-%d")
                formatted_datetime = dt.isoformat()
                match_time         = dt.strftime("%H:%M")
            except:
                formatted_date     = match_date[:10] if len(match_date) >= 10 else match_date
                formatted_datetime = match_date
                match_time         = match_date[11:16] if len(match_date) >= 16 else None

        fixture_status = match.get("fixture", {}).get("status", {}).get("short", "")
        elapsed        = match.get("fixture", {}).get("status", {}).get("elapsed", None)

        form_results.append({
            "fixture_id": match.get("fixture", {}).get("id"),
            "match_datetime": formatted_datetime,
            "date": formatted_date,
            "match_time": match_time,
            "status": fixture_status,
            "elapsed": elapsed,
            "league": league_name,
            "league_logo": league_logo,
            "league_country": league_country,
            "home_team": home_team,
            "home_logo": home_team_logo,
            "home_score": home_goals,
            "away_team": away_team,
            "away_logo": away_team_logo,
            "away_score": away_goals,
            "current_team": current_team_name,
            "current_team_score": our_score,
            "opponent": opponent,
            "opponent_logo": opponent_logo,
            "opponent_score": their_score,
            "result": result,
            "score": f"{our_score}-{their_score}",
            "location": location,
            "was_home": was_home,
            "round": match.get("league", {}).get("round", ""),
            "season": match.get("league", {}).get("season", ""),
        })

    return form_results


# ────────────────────────────────────────────────
# NOTIFICATIONS
# ────────────────────────────────────────────────
@app.post("/notifications/enable-fixture")
async def enable_fixture_notification(pref: NotificationPreference):
    conn   = get_db()
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
    conn   = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT enabled FROM device_fixture_notifications
            WHERE user_id = %s AND fixture_id = %s
        """, (user_id, fixture_id))
        result = cursor.fetchone()
        return {"enabled": result[0] if result else False}
    finally:
        cursor.close()
        release_db(conn)


@app.post("/device/register")
async def register_device(registration: DeviceRegistration):
    await notification_service.register_user(
        registration.user_id,
        {'device_model': registration.device_model, 'app_version': registration.app_version}
    )
    return {"status": "registered"}


@app.get("/device/status")
async def get_device_status(user_id: str):
    conn   = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT last_active FROM users WHERE user_id = %s", (user_id,))
        result = cursor.fetchone()
        return {"registered": bool(result)}
    finally:
        cursor.close()
        release_db(conn)


@app.post("/notifications/test-reminder/{fixture_id}")
async def test_reminder(fixture_id: int):
    conn   = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT * FROM pro_tips WHERE fixture_id = %s", (fixture_id,))
        fixture = cursor.fetchone()
    finally:
        cursor.close()
        release_db(conn)

    if fixture:
        await notification_service.send_match_reminder(fixture)
        return {"status": "test_reminder_sent", "fixture_id": fixture_id}
    return {"error": "Fixture not found"}


@app.get("/notifications/debug/fixture/{fixture_id}")
async def debug_fixture_notifications(fixture_id: int):
    conn   = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
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
    finally:
        cursor.close()
        release_db(conn)

    return {"fixture": fixture, "users_enabled": len(users), "users": users}


# ────────────────────────────────────────────────
# VIP FIXTURES
# ────────────────────────────────────────────────
@app.get("/fixtures/vip", response_model=List[FixtureOut])
def get_vip():
    cache_key = "vip_today"
    cached = get_cache(cache_key)
    if cached:
        return cached

    conn   = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT p.*
            FROM pro_tips p
            JOIN vip_tips v ON p.fixture_id = v.fixture_id
            WHERE v.vip_date = CURRENT_DATE
            ORDER BY p.id DESC
        """)

        rows   = cursor.fetchall()
        result = []

        for r in rows:
            row = dict(r)

            if row.get("match_datetime"):
                dt = row["match_datetime"]
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                row["match_datetime"] = dt.isoformat()
                row["match_time"]     = dt.strftime("%H:%M")
                row["date"]           = dt.strftime("%Y-%m-%d")
            else:
                row["match_time"] = None
                row["date"]       = str(row.get("date"))

            if row.get("last_updated"):
                row["last_updated"] = row["last_updated"].isoformat()

            if not row.get("fixture_id"):
                continue
            if not row.get("home_team") or not row.get("away_team"):
                continue

            result.append(row)

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


# ────────────────────────────────────────────────
# VIP HISTORY
# ────────────────────────────────────────────────
@app.get("/fixtures/vip-history")
def get_vip_history():
    cache_key = "vip_history"
    cached = get_cache(cache_key)
    if cached:
        return cached

    conn   = get_db()
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

        rows    = cursor.fetchall()
        grouped = {}

        for r in rows:
            row = dict(r)

            if row.get("match_datetime"):
                dt = row["match_datetime"]
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                row["match_datetime"] = dt.isoformat()
                row["match_time"]     = dt.strftime("%H:%M")
                row["date"]           = dt.strftime("%Y-%m-%d")
            else:
                row["match_time"] = None
                row["date"]       = str(row.get("date"))

            if row.get("last_updated"):
                row["last_updated"] = row["last_updated"].isoformat()

            group_date = str(row.get("vip_date"))

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

        if not grouped:
            return []

        result = [
            {"date": d, "fixtures": grouped[d]}
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


# ────────────────────────────────────────────────
# VIP LIVE UPDATES
# ────────────────────────────────────────────────
@app.post("/fixtures/vip-updates")
def get_vip_updates(fixture_ids: List[int]):
    conn   = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT fixture_id, home_score, away_score, status, elapsed
            FROM pro_tips
            WHERE fixture_id = ANY(%s)
        """, (fixture_ids,))
        return cursor.fetchall()
    finally:
        cursor.close()
        release_db(conn)


# ────────────────────────────────────────────────
# FIXTURES BY DATE
# ────────────────────────────────────────────────
@app.get("/fixtures/{fixture_date}", response_model=List[FixtureOut])
def get_fixtures(fixture_date: str):
    if fixture_date == "today":
        fixture_date = str(date.today())

    cache_key = f"fixtures:{fixture_date}"
    cached    = get_cache(cache_key)

    if cached:
        if isinstance(cached, str):
            cached = json.loads(cached)
        return JSONResponse(content=cached, media_type="application/json; charset=utf-8")

    conn   = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT *
            FROM pro_tips
            WHERE date = %s
            ORDER BY match_time DESC
        """, (fixture_date,))

        rows   = cursor.fetchall()
        result = []

        for r in rows:
            row = dict(r)

            if row.get("match_datetime"):
                dt = row["match_datetime"]
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                row["match_datetime"] = dt.isoformat()
                row["match_time"]     = dt.strftime("%H:%M")
                row["date"]           = dt.strftime("%Y-%m-%d")
            else:
                row["match_time"] = None
                row["date"]       = fixture_date

            if row.get("last_updated"):
                row["last_updated"] = row["last_updated"].isoformat()

            result.append(row)

        ttl = get_ttl(date.fromisoformat(fixture_date))
        set_cache(cache_key, result, ttl)

        return JSONResponse(content=result, media_type="application/json; charset=utf-8")

    finally:
        cursor.close()
        release_db(conn)


# ────────────────────────────────────────────────
# FIXTURE DETAILS (full stats, lineups, h2h etc.)
# ────────────────────────────────────────────────
@app.get("/fixture-details/{fixture_id}")
async def get_fixture_details(fixture_id: int):
    cache_key = f"fixture_full:{fixture_id}"

    # 1. Redis cache
    cached = redis_get(cache_key)
    if cached:
        return json.loads(cached)

    # 2. DB freshness check
    conn   = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT full_json, status_short, last_updated
            FROM fixture_details
            WHERE fixture_id = %s
        """, (fixture_id,))

        row = cursor.fetchone()

        if row:
            data   = row["full_json"] if isinstance(row["full_json"], dict) else json.loads(row["full_json"])
            status = row["status_short"]
            age    = (datetime.now(timezone.utc) - row["last_updated"]).total_seconds()

            if status in ["FT", "AET", "PEN"]:
                redis_setex(cache_key, CACHE_TTL_LONG, json.dumps(data))
                return data

            if age < 60:
                redis_setex(cache_key, CACHE_TTL_SHORT, json.dumps(data))
                return data

    finally:
        cursor.close()
        release_db(conn)

    # 3. Fetch from API
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            fixture_resp = await client.get(f"{BASE_URL}/fixtures?id={fixture_id}", headers=HEADERS)
            fixture_resp.raise_for_status()

            fixture_data = fixture_resp.json()
            if not fixture_data.get("response"):
                raise HTTPException(404, "Fixture not found")

            fixture   = fixture_data["response"][0]
            league_id = fixture["league"]["id"]
            season    = fixture["league"]["season"]
            home_id   = fixture["teams"]["home"]["id"]
            away_id   = fixture["teams"]["away"]["id"]
            status    = fixture["fixture"]["status"]["short"]

            dt = datetime.fromisoformat(fixture["fixture"]["date"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("UTC"))

            result: Dict[str, Any] = {
                "fixture": {
                    "fixture_id": fixture_id,
                    "home_team":  fixture["teams"]["home"]["name"],
                    "away_team":  fixture["teams"]["away"]["name"],
                    "match_datetime": dt.isoformat(),
                    "status": status,
                    "score":  fixture["goals"],
                }
            }

            # 4. Conditional parallel API calls
            tasks = []
            tasks.append(client.get(f"{BASE_URL}/fixtures/lineups?fixture={fixture_id}", headers=HEADERS) if status != "NS" else None)
            tasks.append(client.get(f"{BASE_URL}/standings?league={league_id}&season={season}", headers=HEADERS))
            tasks.append(client.get(f"{BASE_URL}/fixtures/headtohead?h2h={home_id}-{away_id}", headers=HEADERS))
            tasks.append(client.get(f"{BASE_URL}/odds?fixture={fixture_id}&bookmaker=1", headers=HEADERS) if status not in ["FT", "AET", "PEN"] else None)
            tasks.append(client.get(f"{BASE_URL}/fixtures/statistics?fixture={fixture_id}", headers=HEADERS) if status in ["1H", "2H", "LIVE", "HT"] else None)
            tasks.append(client.get(f"{BASE_URL}/fixtures/events?fixture={fixture_id}", headers=HEADERS))
            tasks.append(client.get(f"{BASE_URL}/fixtures?team={home_id}&last=5", headers=HEADERS))
            tasks.append(client.get(f"{BASE_URL}/fixtures?team={away_id}&last=5", headers=HEADERS))

            responses = await asyncio.gather(*[t for t in tasks if t is not None], return_exceptions=True)
            idx = 0

            # Lineups
            if tasks[0]:
                lineup_resp = responses[idx]; idx += 1
                result["lineups"] = []
                for t in lineup_resp.json().get("response", []):
                    lineup_data = {
                        "team":       t.get("team", {}).get("name", ""),
                        "team_id":    t.get("team", {}).get("id"),
                        "team_logo":  t.get("team", {}).get("logo", ""),
                        "formation":  t.get("formation", ""),
                        "coach":      t.get("coach", {}).get("name", ""),
                        "players":    [],
                        "substitutes": []
                    }
                    for p in t.get("startXI", []):
                        player = p.get("player", {})
                        lineup_data["players"].append({
                            "name": player.get("name", ""),
                            "number": player.get("number"),
                            "pos": player.get("pos", ""),
                            "grid": player.get("grid")
                        })
                    for p in t.get("substitutes", []):
                        player = p.get("player", {})
                        lineup_data["substitutes"].append({
                            "name": player.get("name", ""),
                            "number": player.get("number"),
                            "pos": player.get("pos", "")
                        })
                    result["lineups"].append(lineup_data)

            # Standings
            stand_resp = responses[idx]; idx += 1
            result["standings"] = stand_resp.json().get("response", [])

            # H2H
            h2h_resp = responses[idx]; idx += 1
            result["h2h"] = h2h_resp.json().get("response", [])[:5]

            # Odds
            if tasks[3]:
                odds_resp = responses[idx]; idx += 1
                result["odds"] = odds_resp.json().get("response", [])
            else:
                result["odds"] = None

            # Stats
            if tasks[4]:
                stats_resp = responses[idx]; idx += 1
                result["statistics"] = stats_resp.json().get("response", [])
            else:
                result["statistics"] = None

            # Events
            events_resp = responses[idx]; idx += 1
            result["events"] = [
                {
                    "time":   e["time"]["elapsed"],
                    "team":   e["team"]["name"],
                    "type":   e["type"],
                    "detail": e["detail"],
                    "player": e["player"]["name"] if e.get("player") else None,
                    "assist": e["assist"]["name"] if e.get("assist") else None
                }
                for e in events_resp.json().get("response", [])
            ]

            # Home / Away form
            home_team_name = fixture["teams"]["home"]["name"]
            away_team_name = fixture["teams"]["away"]["name"]

            home_form_resp = responses[idx]; idx += 1
            home_form_data = home_form_resp.json().get("response", []) if not isinstance(home_form_resp, Exception) else []
            result["home_form"] = process_form_data(home_form_data, home_team_name)

            away_form_resp = responses[idx]; idx += 1
            away_form_data = away_form_resp.json().get("response", []) if not isinstance(away_form_resp, Exception) else []
            result["away_form"] = process_form_data(away_form_data, away_team_name)

            # 5. Save to DB
            conn   = get_db()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO fixture_details (
                    fixture_id, league_id, season,
                    home_team_id, away_team_id,
                    full_json, status_short, last_updated
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (fixture_id) DO UPDATE SET
                    full_json    = EXCLUDED.full_json,
                    status_short = EXCLUDED.status_short,
                    last_updated = NOW()
            """, (fixture_id, league_id, season, home_id, away_id, json.dumps(result), status))
            conn.commit()
            cursor.close()
            release_db(conn)

            # 6. Cache
            redis_setex(cache_key, get_fixture_ttl(status), json.dumps(result))

            return result

        except httpx.HTTPStatusError as exc:
            raise HTTPException(exc.response.status_code, str(exc))
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(500, f"Internal error: {str(exc)}")


# ────────────────────────────────────────────────
# SINGLE FIXTURE (for notification clicks)
# ────────────────────────────────────────────────
@app.get("/fixture/{fixture_id}")
async def get_single_fixture(fixture_id: int):
    cache_key = f"api_fixture:{fixture_id}"

    cached = redis_get(cache_key)
    if cached:
        return json.loads(cached)

    conn   = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT
                fixture_id, home_team, away_team, home_logo, away_logo,
                league, league_country, league_logo,
                match_datetime, date, status, elapsed,
                home_score, away_score, odd, prediction,
                extra, source, last_updated, result_notification_sent
            FROM pro_tips
            WHERE fixture_id = %s
            LIMIT 1
        """, (fixture_id,))

        fixture = cursor.fetchone()

        if not fixture:
            raise HTTPException(status_code=404, detail=f"Fixture {fixture_id} not found")

        # ✅ Convert to plain dict so we can mutate it
        fixture = dict(fixture)

        if isinstance(fixture.get('match_datetime'), datetime):
            fixture['match_datetime'] = fixture['match_datetime'].isoformat()

        if isinstance(fixture.get('last_updated'), datetime):
            fixture['last_updated'] = fixture['last_updated'].isoformat()

        if isinstance(fixture.get('date'), (date, datetime)):
            fixture['date'] = str(fixture['date'])

        fixture['server_timestamp'] = datetime.now(timezone.utc).isoformat()

        status = fixture.get('status', 'NS')
        ttl    = get_fixture_ttl(status)

        redis_setex(cache_key, ttl, json.dumps(fixture, default=json_serializer))

        return JSONResponse(content=fixture, media_type="application/json; charset=utf-8")

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
    conn   = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT *
            FROM booking_codes
            WHERE post_date = CURRENT_DATE
            ORDER BY post_time DESC
        """)
        rows = cursor.fetchall()
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
    conn   = get_db()
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
# HEALTH & DEBUG
# ────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/debug/redis")
def debug_redis():
    try:
        redis_client.ping()
        return {"redis": "connected"}
    except Exception as e:
        return {"redis": "failed", "error": str(e)}