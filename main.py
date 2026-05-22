import logging
import httpx
import pytz
import redis
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import get_betcodes
import kbt_load_env
import asyncio
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

# =========================
LAGOS_TZ = pytz.timezone("Africa/Lagos")

# =========================
# LOGGING
# =========================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )


db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=kbt_load_env.supabase_url
)

def get_db():
    return db_pool.getconn()

def release_db(conn):
    db_pool.putconn(conn)

# Initialize notification service
notification_service = MatchNotificationService()


# ─────────────────────────────
# ALTERNATIVE: Direct import and run (if scripts are modules)
# ─────────────────────────────
def daily_pipeline():
    """Alternative: Import and run scripts directly as modules"""
    print("=" * 60)
    print(f"🕐 Daily Pipeline Started at: {datetime.now()}")
    print("=" * 60)
    
    results = {}
    
    try:
        # Import your modules
        import api_football_call
        import api_football_yesterday_call
        import get_pro_tip_yesterday
        import get_pro_tips
        import post_pro_tips
        import update_pro_tip_results


           # Step 1: Call API football for today
        print("\n📋 STEP 1: Calling API football for today...")
        api_football_call.main()
        results['api_football_call'] = True
        print("   ✅ Completed: api_football_call")
        
        # Step 2: Call API football for yesterday
        print("\n📋 STEP 2: Calling API football for yesterday...")
        api_football_yesterday_call.main()
        results['api_football_yesterday_call'] = True
        print("   ✅ Completed: api_football_yesterday_call")
        
        # Step 3: Get pro tips for today
        print("\n📋 STEP 3: Getting today's pro tips...")
        get_pro_tips.main()
        results['get_pro_tips'] = True
        print("   ✅ Completed: get_pro_tips")
        
        # Step 4: Get pro tips for yesterday
        print("\n📋 STEP 4: Getting yesterday's pro tips...")
        get_pro_tip_yesterday.main()
        results['get_pro_tip_yesterday'] = True
        print("   ✅ Completed: get_pro_tip_yesterday")
        
     
        # Step 5: Post pro tips
        print("\n📋 STEP 5: Posting pro tips...")
        post_pro_tips.main()
        results['post_pro_tips'] = True
        print("   ✅ Completed: post_pro_tips")
        
        # Step 6: Update results
        print("\n📋 STEP 6: Updating results...")
        update_pro_tip_results.main()
        results['update_pro_tip_results'] = True
        print("   ✅ Completed: update_pro_tip_results")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Make sure all script files are in the same directory")
    except Exception as e:
        print(f"❌ Pipeline error: {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 PIPELINE SUMMARY")
    print("=" * 60)
    success_count = sum(1 for success in results.values() if success)
    total_count = len(results)
    print(f"   Success: {success_count}/{total_count} scripts")
    print("=" * 60)


# ─────────────────────────────
# JOB
# ─────────────────────────────
def refresh_live_predictions():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # For async notifications
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        print("⏱ Scheduler UTC:", datetime.utcnow())

        cursor.execute("""
            SELECT fixture_id, date, status, home_team, away_team, prediction, home_score, away_score, match_datetime
            FROM pro_tips
            WHERE match_datetime BETWEEN 
                NOW() - INTERVAL '3 hours'      
                AND NOW() + INTERVAL '45 minutes'   -- Changed from 20 to 45 minutes
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
                AND match_datetime BETWEEN NOW() - INTERVAL '30 minutes' 
                AND NOW() + INTERVAL '45 minutes')   -- Changed from 20 to 45
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

      

        # DEBUG: Check for fixture 1391834 specifically
        for row in rows:
            if row['fixture_id'] == 1391834:
                match_time = row['match_datetime']
                if isinstance(match_time, str):
                    match_time = datetime.fromisoformat(match_time)
                minutes_until = (match_time - datetime.now()).total_seconds() / 60
                print(f"🔍 Fixture 1391834 found! Minutes until: {minutes_until:.1f}")
                print(f"   In reminder window: {15 <= minutes_until <= 30}")

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
                    old_elapsed = row.get("elapsed")

                    r = client.get(
                        f"{BASE_URL}/fixtures?id={fid}",
                        headers=HEADERS
                    )
                    data = r.json()

                    if not data.get("response"):
                        continue

                    f = data["response"][0]

                    status_data = f["fixture"]["status"]

                    new_status = status_data.get("short")
                    elapsed = status_data.get("elapsed")
                    extra = status_data.get("extra")

                    home = f["goals"]["home"] or 0
                    away = f["goals"]["away"] or 0

                    # ───────── FORMAT ELAPSED (TEXT) ─────────
                    if new_status == "HT":
                        elapsed_display = "HT"
                    elif new_status == "FT":
                        elapsed_display = "FT"
                    elif new_status == "NS":
                        elapsed_display = "NS"
                    else:
                        if elapsed:
                            if extra:
                                elapsed_display = f"{elapsed}+{extra}'"
                            else:
                                elapsed_display = f"{elapsed}'"
                        else:
                            elapsed_display = None

                    # ───────── NOTIFICATIONS: Match finished ─────────
                    if current_status != 'FT' and new_status == 'FT':
                        matches_to_notify_result.append(row)
                        print(f"🎯 Match finished: {row['home_team']} vs {row['away_team']}")

                    # ───────── NOTIFICATIONS: Match starting soon ─────────
                    if new_status == 'NS':
                        match_time = row['match_datetime']
                        if isinstance(match_time, str):
                            match_time = datetime.fromisoformat(match_time)

                        minutes_until = (match_time - datetime.now()).total_seconds() / 60

                        if 15 <= minutes_until <= 30:
                            cursor.execute("""
                                SELECT reminder_sent FROM notification_log WHERE fixture_id = %s
                            """, (fid,))
                            reminder_result = cursor.fetchone()

                            if not reminder_result or not reminder_result[0]:
                                matches_to_notify_reminder.append(row)
                                print(f"🔔 Match starting soon: {row['home_team']} vs {row['away_team']}")

                    # ───────── UPDATE ONLY IF CHANGED ─────────
                    if (
                        home != old_home_score or
                        away != old_away_score or
                        new_status != current_status or
                        elapsed_display != old_elapsed
                    ):

                        cursor.execute("""
                            UPDATE pro_tips
                            SET home_score = %s,
                                away_score = %s,
                                status = %s,
                                elapsed = %s,
                                last_updated = NOW()
                            WHERE fixture_id = %s
                        """, (home, away, new_status, elapsed_display, fid))

                        print(f"🔄 {fid} → {home}-{away} ({new_status}) [{elapsed_display}]")

                    # ───────── CACHE CLEAR ─────────
                    if row["date"] not in deleted_dates:
                        redis_client.delete(f"fixtures:{row['date']}")
                        deleted_dates.add(row["date"])

                except Exception as e:
                    print(f"❌ Error updating {fid}:", e)
                # ───────── NOTIFICATIONS: Send reminders ─────────
                if matches_to_notify_reminder:
                    print(f"📱 Sending {len(matches_to_notify_reminder)} match reminders...")
                    for match in matches_to_notify_reminder:
                        try:
                            loop.run_until_complete(
                                notification_service.send_match_reminder(match)
                            )
                        except Exception as e:
                            print(f"Error sending reminder for {match['fixture_id']}: {e}")

                # ───────── NOTIFICATIONS: Send result notifications ─────────
                if matches_to_notify_result:
                    print(f"📱 Sending {len(matches_to_notify_result)} result notifications...")
                    for match in matches_to_notify_result:
                        try:
                            loop.run_until_complete(
                                notification_service.send_prediction_result(match)
                            )
                            
                            cursor.execute("""
                                UPDATE pro_tips SET result_notification_sent = TRUE
                                WHERE fixture_id = %s
                            """, (match['fixture_id'],))
                            
                        except Exception as e:
                            print(f"Error sending result for {match['fixture_id']}: {e}")

                conn.commit()
                print("✅ Scheduler commit complete")

    except Exception as e:
        print("🔥 Scheduler crash:", e)
        if conn:
            conn.rollback()

    finally:
        loop.close()
        if cursor:
            cursor.close()
        if conn:
            release_db(conn)


from notification_service import MatchNotificationService

def run_betcodes():
    logging.info(f"🧾 Running get_betcodes at {datetime.now(LAGOS_TZ)}")

    try:
        count = get_betcodes.run()
        logging.info("✅ get_betcodes completed")

        # 🔥 INIT SERVICE
        notifier = MatchNotificationService()

        # 🔥 SEND ONLY IF NEW DATA
        if count and count > 0:
            notifier.send_betcode_notification()
        else:
            logging.info("ℹ️ No new betcodes → skipping notification")

    except Exception as e:
        logging.exception("❌ get_betcodes failed")
# ─────────────────────────────
# SCHEDULER
# ─────────────────────────────
scheduler = BlockingScheduler(timezone=LAGOS_TZ)

scheduler.add_job(
    refresh_live_predictions,
    'interval',
    minutes=5,
    max_instances=1,      # 🔥 prevent overlapping jobs
    coalesce=True         # 🔥 skip missed runs
)

# Job 2: Run daily at 1:30 AM for pipeline tasks
scheduler.add_job(
    daily_pipeline,
    'cron',
    hour=2,
    minute=11,
    id='daily_pipeline',
    max_instances=1,
    coalesce=True
)

 # 🧾 Betcodes every 3 hours from 6AM → 21PM
scheduler.add_job(
    run_betcodes,
    'cron',
    hour='6-23/3',   # 6,9,12,15,18,21
    minute=10,       # 🔥 offset to avoid DB clash
    max_instances=1,
    coalesce=True
    )


print("🚀 Worker started...")
print("   - Live updates every 5 minutes")
print("   - Daily pipeline at 1:30 AM")
print("   - Pipeline scripts: get_pro_tips, api_football_call, post_pro_tips, etc.")

scheduler.start()
print("🚀 Worker started...")
