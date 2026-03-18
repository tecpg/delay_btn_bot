import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from typing import List, Optional
import mysql.connector
import redis
import json
from datetime import date
import kbt_funtions
import kbt_load_env
import requests  # ← add this import
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

app = FastAPI(title="Match Fixtures API")

# Redis
REDIS_URL = kbt_load_env.redis_url
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
CACHE_TTL = 24 * 60 * 60  # 24 hours default

# API-Football
API_KEY = kbt_load_env.api_football_key
HEADERS = {"x-apisports-key": API_KEY}

# Scheduler (global)
scheduler = BackgroundScheduler()

def get_db():
    return kbt_funtions.db_connection()

# ────────────────────────────────────────────────
# Background job: refresh only currently live matches
# ────────────────────────────────────────────────
def refresh_live_predictions():
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        # Only matches that are live *today*
        cursor.execute("""
            SELECT fixture_id, `date`, status
            FROM pro_tips
            WHERE `date` = CURDATE()
              AND status IN ('1H', 'HT', '2H', 'ET', 'BT', 'P', 'LIVE', 'SUSP', 'INT')
        """)
        live_rows = cursor.fetchall()

        if not live_rows:
            print(f"No live matches to refresh at {date.today()}")
            return

        print(f"Refreshing {len(live_rows)} live matches at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        for row in live_rows:
            fid = row['fixture_id']
            try:
                r = requests.get(
                    f"https://v3.football.api-sports.io/fixtures?id={fid}",
                    headers=HEADERS,
                    timeout=10
                )
                r.raise_for_status()
                api_data = r.json()

                if not api_data.get("response"):
                    print(f"No response for fixture {fid}")
                    continue

                fixture = api_data["response"][0]
                new_home = fixture["goals"]["home"]
                new_away = fixture["goals"]["away"]
                new_status = fixture["fixture"]["status"]["short"]

                cursor.execute("""
                    UPDATE pro_tips
                    SET home_score = %s,
                        away_score = %s,
                        status = %s,
                        last_updated = NOW()
                    WHERE fixture_id = %s
                """, (new_home, new_away, new_status, fid))

                # Invalidate Redis cache for that date
                redis_client.delete(f"fixtures:{row['date']}")

                print(f"Updated fixture {fid}: {new_home}-{new_away} ({new_status})")

            except requests.exceptions.RequestException as req_err:
                print(f"API request failed for fixture {fid}: {req_err}")
            except Exception as e:
                print(f"Unexpected error refreshing fixture {fid}: {e}")

        conn.commit()

    except mysql.connector.Error as mysql_err:
        print(f"MySQL error during live refresh: {mysql_err}")
    except Exception as e:
        print(f"General error in refresh_live_predictions: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# Schedule it
scheduler.add_job(
    refresh_live_predictions,
    trigger=IntervalTrigger(seconds=60),
    id='live_predictions_refresh',
    name='Refresh live football predictions',
    replace_existing=True
)

# Start scheduler on app startup
@app.on_event("startup")
async def startup_event():
    if not scheduler.running:
        scheduler.start()
        print("Live predictions auto-refresh scheduler started (every 60 seconds)")

# Clean shutdown (optional but good practice)
@app.on_event("shutdown")
def shutdown_event():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        print("Live refresh scheduler stopped")

# Your other endpoints go here...
# e.g. /fixtures/today, /fixtures/{date}, etc.

@app.get("/health")
def health():
    return {"status": "ok", "live_scheduler_running": scheduler.running}