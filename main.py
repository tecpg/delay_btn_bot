import json
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
redis_client = redis.from_url(kbt_load_env.redis_url, decode_responses=True)

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
            SELECT fixture_id, date
            FROM pro_tips
            WHERE match_datetime BETWEEN
                NOW() - INTERVAL '75 minutes'
                AND NOW() + INTERVAL '10 minutes'
            AND (
                last_updated IS NULL
                OR last_updated < NOW() - INTERVAL '5 minutes'
            )
            LIMIT 30
        """)

        rows = cursor.fetchall()

        if not rows:
            print("⚠️ No matches to update")
            return

        print(f"🔥 Updating {len(rows)} matches")

        deleted_dates = set()

        with httpx.Client(timeout=10) as client:
            for row in rows:
                try:
                    fid = row["fixture_id"]

                    r = client.get(
                        f"{BASE_URL}/fixtures?id={fid}",
                        headers=HEADERS
                    )

                    data = r.json()
                    if not data.get("response"):
                        continue

                    fixture = data["response"][0]

                    home = fixture["goals"]["home"] or 0
                    away = fixture["goals"]["away"] or 0
                    status = fixture["fixture"]["status"]["short"]

                    # ✅ SINGLE OPTIMIZED UPDATE
                    cursor.execute("""
                        UPDATE pro_tips
                        SET home_score = %s,
                            away_score = %s,
                            status = %s,
                            last_updated = NOW()
                        WHERE fixture_id = %s
                        AND (
                            home_score IS DISTINCT FROM %s OR
                            away_score IS DISTINCT FROM %s OR
                            status IS DISTINCT FROM %s
                        )
                        RETURNING fixture_id
                    """, (home, away, status, fid, home, away, status))

                    updated = cursor.fetchone()

                    # ✅ ONLY SEND IF CHANGED
                    if updated:
                        update_payload = {
                            "fixture_id": fid,
                            "home_score": home,
                            "away_score": away,
                            "status": status
                        }

                        redis_client.publish("live_scores", json.dumps(update_payload))
                        print("📡 SENT:", update_payload)

                    print(f"🔄 {fid} → {home}-{away} ({status})")

                    # 🧹 clear cache once per date
                    if row["date"] not in deleted_dates:
                        redis_client.delete(f"fixtures:{row['date']}")
                        deleted_dates.add(row["date"])

                except Exception as e:
                    print(f"❌ Error {fid}:", e)

        conn.commit()
        print("✅ Scheduler commit complete")

    except Exception as e:
        print("🔥 Scheduler crash:", e)
        conn.rollback()

    finally:
        cursor.close()
        release_db(conn)



# ─────────────────────────────
# SCHEDULER
# ─────────────────────────────
scheduler = BlockingScheduler()

scheduler.add_job(
    refresh_live_predictions,
    'interval',
    minutes=5
)

print("🚀 Worker started...")
scheduler.start()