from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import List, Optional
import mysql.connector
import redis
import json
from datetime import date
import kbt_funtions
import kbt_load_env
from pydantic import BaseModel, parse_obj_as
from typing import Dict, Any
import json
import httpx

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
            ORDER BY match_time DESC
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

BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": kbt_load_env.api_football_key}

CACHE_TTL_SHORT = 3600      # 1 hour for live/recent
CACHE_TTL_LONG  = 86400 * 3 # 3 days for finished matches

def get_db():
    return kbt_funtions.db_connection()

@app.get("/api/fixture-details/{fixture_id}")
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
        


# Add a manual cache-clear endpoint (useful for admin/debug)
# Call it with POST /admin/clear-cache/2026-03-11 when you know results have been updated.
@app.post("/admin/clear-cache/{fixture_date}")
def clear_cache(fixture_date: str):
    cache_key = f"fixtures:{fixture_date}"
    deleted = redis_client.delete(cache_key)
    return {"message": f"Cache for {fixture_date} cleared", "deleted": deleted > 0}