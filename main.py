import httpx
import redis
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import kbt_load_env

# ─────────────────────────────
# INIT
# ─────────────────────────────
redis_client = redis.from_url(
    kbt_load_env.redis_url,
    decode_responses=True
)

BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": kbt_load_env.api_football_key}

db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=kbt_load_env.supabase_url
)

def get_db():
    return db_pool.getconn()

def release_db(conn):
    db_pool.putconn(conn)

# ─────────────────────────────
# JOB
# ─────────────────────────────
def refresh_live_predictions():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        print("⏱ Scheduler UTC:", datetime.utcnow())

        cursor.execute("""
            SELECT fixture_id, date, status
            FROM pro_tips
            WHERE match_datetime BETWEEN 
                NOW() - INTERVAL '3 hours'      -- Catch recently finished matches
                AND NOW() + INTERVAL '20 minutes'
            AND (
                -- Live matches: update every 2 minutes
                (status IN ('1H', 'HT', '2H', 'ET', 'P') 
                 AND (last_updated IS NULL OR last_updated < NOW() - INTERVAL '2 minutes'))
                OR
                -- Finished matches: update every 10 minutes
                (status = 'FT' 
                 AND (last_updated IS NULL OR last_updated < NOW() - INTERVAL '10 minutes'))
                OR
                -- Not started but close to kickoff
                (status = 'NS' 
                 AND match_datetime BETWEEN NOW() - INTERVAL '10 minutes' 
                 AND NOW() + INTERVAL '10 minutes')
            )
            ORDER BY 
                CASE 
                    WHEN status IN ('1H', 'HT', '2H', 'ET', 'P') THEN 0
                    WHEN status = 'FT' THEN 1
                    ELSE 2
                END,
                match_datetime
            LIMIT 40
        """)

        rows = cursor.fetchall()

        if not rows:
            print("⚠️ No matches to update at this time")
            return

        print(f"🔥 Updating {len(rows)} matches")

        deleted_dates = set()

        with httpx.Client(timeout=12) as client:
            for row in rows:
                try:
                    fid = row["fixture_id"]
                    current_status = row.get("status", "NS")

                    r = client.get(
                        f"{BASE_URL}/fixtures?id={fid}",
                        headers=HEADERS
                    )
                    data = r.json()

                    if not data.get("response"):
                        continue

                    f = data["response"][0]
                    new_status = f["fixture"]["status"]["short"]
                    home = f["goals"]["home"] or 0
                    away = f["goals"]["away"] or 0

                    # Only update if something actually changed
                    if (home != row.get("home_score") or 
                        away != row.get("away_score") or 
                        new_status != current_status):

                        cursor.execute("""
                            UPDATE pro_tips
                            SET home_score = %s,
                                away_score = %s,
                                status = %s,
                                last_updated = NOW()
                            WHERE fixture_id = %s
                        """, (home, away, new_status, fid))

                        print(f"🔄 {fid} → {home}-{away} ({new_status})")

                    # Clear cache once per date
                    if row["date"] not in deleted_dates:
                        redis_client.delete(f"fixtures:{row['date']}")
                        deleted_dates.add(row["date"])

                except Exception as e:
                    print(f"❌ Error updating {fid}:", e)

        conn.commit()
        print("✅ Scheduler commit complete")

    except Exception as e:
        print("🔥 Scheduler crash:", e)
        if conn:
            conn.rollback()

    finally:
        if cursor:
            cursor.close()
        if conn:
            release_db(conn)
# ─────────────────────────────
# SCHEDULER
# ─────────────────────────────
scheduler = BlockingScheduler()

scheduler.add_job(
    refresh_live_predictions,
    'interval',
    minutes=5,
    max_instances=1,      # 🔥 prevent overlapping jobs
    coalesce=True         # 🔥 skip missed runs
)
# main.py

from pydantic import BaseModel
from typing import Optional

class DeviceRegistration(BaseModel):
    onesignal_player_id: str
    device_model: Optional[str] = None
    app_version: Optional[str] = None

notification_service = MatchNotificationService()

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
    return {"status": "registered"}

# Manual trigger endpoints (for testing)
@app.post("/notifications/send-reminders")
async def send_match_reminders():
    """Manually trigger match reminders"""
    await notification_service.send_bulk_reminders()
    return {"status": "reminders_sent"}

@app.post("/notifications/send-results")
async def send_match_results():
    """Manually trigger result notifications"""
    await notification_service.send_bulk_results()
    return {"status": "results_sent"}

print("🚀 Worker started...")
scheduler.start()