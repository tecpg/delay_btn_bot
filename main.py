# scheduler.py
import httpx
import redis
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from psycopg2.extras import RealDictCursor
import kbt_load_env
import asyncio
from db_utils import get_db, release_db  # Import from db_utils
from notification_service import MatchNotificationService

# ─────────────────────────────
# INIT
# ─────────────────────────────
redis_client = redis.from_url(
    kbt_load_env.redis_url,
    decode_responses=True
)

BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": kbt_load_env.api_football_key}

# Initialize notification service
notification_service = MatchNotificationService()

# ─────────────────────────────
# JOB
# ─────────────────────────────
def refresh_live_predictions():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Run async notification functions
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        print("⏱ Scheduler UTC:", datetime.utcnow())

        cursor.execute("""
            SELECT fixture_id, date, status, home_team, away_team, 
                   prediction, home_score, away_score, match_datetime
            FROM pro_tips
            WHERE match_datetime BETWEEN 
                NOW() - INTERVAL '3 hours'      
                AND NOW() + INTERVAL '30 minutes'
            AND (
                -- Live matches: update every 2 minutes
                (status IN ('1H', 'HT', '2H', 'ET', 'P') 
                 AND (last_updated IS NULL OR last_updated < NOW() - INTERVAL '2 minutes'))
                OR
                -- Finished matches: update every 10 minutes
                (status = 'FT' 
                 AND (last_updated IS NULL OR last_updated < NOW() - INTERVAL '10 minutes'))
                OR
                -- Not started but close to kickoff (15-30 min before)
                (status = 'NS' 
                 AND match_datetime BETWEEN NOW() + INTERVAL '15 minutes' 
                 AND NOW() + INTERVAL '30 minutes')
                OR
                -- Recently finished matches (for result notifications)
                (status IN ('FT', 'AET', 'PEN')
                 AND (result_notification_sent IS NULL OR result_notification_sent = FALSE)
                 AND match_datetime < NOW() - INTERVAL '10 minutes')
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
        matches_to_notify_reminder = []
        matches_to_notify_result = []

        with httpx.Client(timeout=12) as client:
            for row in rows:
                try:
                    fid = row["fixture_id"]
                    current_status = row.get("status", "NS")
                    old_home_score = row.get("home_score", 0)
                    old_away_score = row.get("away_score", 0)
                    
                    print(f"📊 Processing fixture {fid}: current_status={current_status}")

                    # Make API request
                    url = f"{BASE_URL}/fixtures?id={fid}"
                    print(f"   Calling API: {url}")
                    
                    r = client.get(url, headers=HEADERS)
                    print(f"   API Response Status: {r.status_code}")
                    
                    if r.status_code != 200:
                        print(f"   ❌ API returned {r.status_code}: {r.text[:200]}")
                        continue
                    
                    data = r.json()

                    if not data.get("response"):
                        print(f"   ⚠️ No response data for fixture {fid}")
                        continue

                    f = data["response"][0]
                    new_status = f["fixture"]["status"]["short"]
                    home = f["goals"]["home"] or 0
                    away = f["goals"]["away"] or 0
                    
                    print(f"   New data: status={new_status}, score={home}-{away}")

                    # Check if match just finished (status changed to FT)
                    if current_status != 'FT' and new_status == 'FT':
                        matches_to_notify_result.append(row)
                        print(f"   🎯 Match finished: {row['home_team']} vs {row['away_team']}")

                    # Check if match is starting soon (15-30 minutes before)
                    if new_status == 'NS':
                        match_time = row['match_datetime']
                        if isinstance(match_time, str):
                            match_time = datetime.fromisoformat(match_time)
                        
                        minutes_until = (match_time - datetime.now()).total_seconds() / 60
                        print(f"   Minutes until match: {minutes_until:.1f}")
                        
                        # Send reminder if between 15-30 minutes before match
                        if 15 <= minutes_until <= 30:
                            # Check if reminder not sent yet
                            cursor.execute("""
                                SELECT reminder_sent FROM notification_log WHERE fixture_id = %s
                            """, (fid,))
                            reminder_result = cursor.fetchone()
                            
                            if not reminder_result or not reminder_result[0]:
                                matches_to_notify_reminder.append(row)
                                print(f"   🔔 Will send reminder for {row['home_team']} vs {row['away_team']}")

                    # Only update if something actually changed
                    if (home != old_home_score or 
                        away != old_away_score or 
                        new_status != current_status):

                        cursor.execute("""
                            UPDATE pro_tips
                            SET home_score = %s,
                                away_score = %s,
                                status = %s,
                                last_updated = NOW()
                            WHERE fixture_id = %s
                        """, (home, away, new_status, fid))

                        print(f"   ✅ Updated {fid} → {home}-{away} ({new_status})")
                    else:
                        print(f"   ⏭️ No changes for fixture {fid}")

                    # Clear cache once per date
                    if row["date"] not in deleted_dates:
                        redis_client.delete(f"fixtures:{row['date']}")
                        deleted_dates.add(row["date"])

                except httpx.HTTPStatusError as e:
                    print(f"❌ HTTP Error for {fid}: {e.response.status_code} - {e.response.text[:200]}")
                except httpx.RequestError as e:
                    print(f"❌ Request Error for {fid}: {e}")
                except Exception as e:
                    print(f"❌ Unexpected Error for {fid}: {type(e).__name__} - {str(e)}")
                    import traceback
                    traceback.print_exc()

        # Send notifications asynchronously
        if matches_to_notify_reminder:
            print(f"📱 Sending {len(matches_to_notify_reminder)} match reminders...")
            for match in matches_to_notify_reminder:
                try:
                    loop.run_until_complete(
                        notification_service.send_match_reminder(match)
                    )
                    print(f"   ✅ Reminder sent for {match['fixture_id']}")
                except Exception as e:
                    print(f"   ❌ Error sending reminder for {match['fixture_id']}: {e}")

        if matches_to_notify_result:
            print(f"📱 Sending {len(matches_to_notify_result)} result notifications...")
            for match in matches_to_notify_result:
                try:
                    loop.run_until_complete(
                        notification_service.send_prediction_result(match)
                    )
                    
                    # Mark result notification as sent
                    cursor.execute("""
                        UPDATE pro_tips SET result_notification_sent = TRUE
                        WHERE fixture_id = %s
                    """, (match['fixture_id'],))
                    
                    print(f"   ✅ Result sent for {match['fixture_id']}")
                except Exception as e:
                    print(f"   ❌ Error sending result for {match['fixture_id']}: {e}")

        conn.commit()
        print("✅ Scheduler commit complete")

    except Exception as e:
        print("🔥 Scheduler crash:", e)
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()

    finally:
        loop.close()
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
    minutes=2,
    max_instances=1,
    coalesce=True
)

print("🚀 Worker started with notifications...")
scheduler.start()