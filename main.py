from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import httpx
import redis
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import kbt_load_env

# ────────────────────────────────────────────────
# INIT
# ────────────────────────────────────────────────
redis_client = redis.from_url(kbt_load_env.redis_url, decode_responses=True)

BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": kbt_load_env.api_football_key}

# DB POOL
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
# JOB
# ────────────────────────────────────────────────
def refresh_live_predictions():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        print("⏱ Running scheduler:", datetime.utcnow())

        cursor.execute("""
SELECT fixture_id, date
  FROM pro_tips
 WHERE date = CURRENT_DATE AT TIME ZONE 'Africa/Lagos'
   AND (last_updated IS NULL 
        OR last_updated < NOW() - INTERVAL '70 seconds')
   AND (date + match_time AT TIME ZONE 'Africa/Lagos') BETWEEN 
           NOW() - INTERVAL '3 hours'          -- Covers full matches + delays/postponed
           AND NOW() + INTERVAL '45 minutes'   -- Only poll upcoming if close (adjust 45–90 min)
   AND (
       -- Prefer live/in-play first (these change fast)
       status IN ('1H', 'HT', '2H', 'ET', 'BT', 'P', 'S')   -- API-Football in-play codes
       OR 
       -- Upcoming only if very close or never updated
       (status = 'NS' AND (date + match_time AT TIME ZONE 'Africa/Lagos') <= NOW() + INTERVAL '75 minutes')
       OR last_updated IS NULL   -- Force first poll even if far
   )
 ORDER BY 
     CASE 
         WHEN status IN ('1H', 'HT', '2H', 'ET', 'BT', 'P', 'S') THEN 0   -- live first
         ELSE 1 
     END,
     (date + match_time)
 LIMIT 20
FOR UPDATE SKIP LOCKED
        """)

        rows = cursor.fetchall()

        if not rows:
            print("⚠️ No matches found")
            return

        print(f"✅ Found {len(rows)} matches")

        deleted_dates = set()

        with httpx.Client(timeout=10) as client:
            for row in rows:
                try:
                    fid = row["fixture_id"]

                    r = client.get(f"{BASE_URL}/fixtures?id={fid}", headers=HEADERS)
                    data = r.json()

                    if not data.get("response"):
                        continue

                    f = data["response"][0]

                    cursor.execute("""
                        UPDATE pro_tips
                        SET home_score=%s,
                            away_score=%s,
                            status=%s,
                            last_updated=NOW()
                        WHERE fixture_id=%s
                    """, (
                        f["goals"]["home"] or 0,
                        f["goals"]["away"] or 0,
                        f["fixture"]["status"]["short"],
                        fid
                    ))

                    if row["date"] not in deleted_dates:
                        redis_client.delete(f"fixtures:{row['date']}")
                        deleted_dates.add(row["date"])

                    print(f"🔄 Updated {fid}")

                except Exception as e:
                    print("❌ Error:", e)

        conn.commit()

    finally:
        cursor.close()
        release_db(conn)

# ────────────────────────────────────────────────
# SCHEDULER (BLOCKING)
# ────────────────────────────────────────────────
scheduler = BlockingScheduler()

scheduler.add_job(
    refresh_live_predictions,
    'interval',
    seconds=90
)

print("🚀 Worker started...")
scheduler.start()