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

LAGOS_TZ = pytz.timezone("Africa/Lagos")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=kbt_load_env.supabase_url
)

def get_db():
    return db_pool.getconn()

def release_db(conn):
    db_pool.putconn(conn)

notification_service = MatchNotificationService()


# ─────────────────────────────
# DAILY PIPELINE
# ─────────────────────────────
def daily_pipeline():
    print("=" * 60)
    print(f"🕐 Daily Pipeline Started at: {datetime.now()}")
    print("=" * 60)

    results = {}

    try:
        import api_football_call
        import api_football_yesterday_call
        import get_pro_tip_yesterday
        import get_pro_tips
        import post_pro_tips
        import update_pro_tip_results

        print("\n📋 STEP 1: Calling API football for today...")
        api_football_call.run()
        results['api_football_call'] = True
        print("   ✅ Completed: api_football_call")

        print("\n📋 STEP 2: Calling API football for yesterday...")
        api_football_yesterday_call.run()
        results['api_football_yesterday_call'] = True
        print("   ✅ Completed: api_football_yesterday_call")

        print("\n📋 STEP 3: Getting today's pro tips...")
        get_pro_tips.run()
        results['get_pro_tips'] = True
        print("   ✅ Completed: get_pro_tips")

        print("\n📋 STEP 4: Getting yesterday's pro tips...")
        get_pro_tip_yesterday.run()
        results['get_pro_tip_yesterday'] = True
        print("   ✅ Completed: get_pro_tip_yesterday")

        print("\n📋 STEP 5: Posting pro tips...")
        post_pro_tips.run()
        results['post_pro_tips'] = True
        print("   ✅ Completed: post_pro_tips")

        print("\n📋 STEP 6: Updating results...")
        update_pro_tip_results.run()
        results['update_pro_tip_results'] = True
        print("   ✅ Completed: update_pro_tip_results")

    except ImportError as e:
        print(f"❌ Import error: {e}")
    except Exception as e:
        print(f"❌ Pipeline error: {e}")

    print("\n" + "=" * 60)
    print("📊 PIPELINE SUMMARY")
    print("=" * 60)
    success_count = sum(1 for success in results.values() if success)
    print(f"   Success: {success_count}/{len(results)} scripts")
    print("=" * 60)


# ─────────────────────────────
# LIVE PREDICTIONS
# ─────────────────────────────
def refresh_live_predictions():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        print("⏱ Scheduler UTC:", datetime.utcnow())

        cursor.execute("""
            SELECT fixture_id, date, status, home_team, away_team, prediction,
                   home_score, away_score, match_datetime
            FROM pro_tips
            WHERE match_datetime BETWEEN
                NOW() - INTERVAL '3 hours'
                AND NOW() + INTERVAL '45 minutes'
            AND (
                (status IN ('1H', 'HT', '2H', 'ET', 'P')
                AND (last_updated IS NULL OR last_updated < NOW() - INTERVAL '2 minutes'))
                OR
                (status = 'FT'
                AND (last_updated IS NULL OR last_updated < NOW() - INTERVAL '10 minutes'))
                OR
                (status = 'NS'
                AND match_datetime BETWEEN NOW() - INTERVAL '30 minutes'
                AND NOW() + INTERVAL '45 minutes')
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

        # DEBUG
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

                    r = client.get(f"{BASE_URL}/fixtures?id={fid}", headers=HEADERS)
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

                    if new_status == "HT":
                        elapsed_display = "HT"
                    elif new_status == "FT":
                        elapsed_display = "FT"
                    elif new_status == "NS":
                        elapsed_display = "NS"
                    else:
                        if elapsed:
                            elapsed_display = f"{elapsed}+{extra}'" if extra else f"{elapsed}'"
                        else:
                            elapsed_display = None

                    if current_status != 'FT' and new_status == 'FT':
                        matches_to_notify_result.append(row)
                        print(f"🎯 Match finished: {row['home_team']} vs {row['away_team']}")

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

                    if row["date"] not in deleted_dates:
                        redis_client.delete(f"fixtures:{row['date']}")
                        deleted_dates.add(row["date"])

                except Exception as e:
                    print(f"❌ Error updating {fid}:", e)

        # ── NOTIFICATIONS (after row loop) ──
        if matches_to_notify_reminder:
            print(f"📱 Sending {len(matches_to_notify_reminder)} match reminders...")
            for match in matches_to_notify_reminder:
                try:
                    loop.run_until_complete(notification_service.send_match_reminder(match))
                except Exception as e:
                    print(f"Error sending reminder for {match['fixture_id']}: {e}")

        if matches_to_notify_result:
            print(f"📱 Sending {len(matches_to_notify_result)} result notifications...")
            for match in matches_to_notify_result:
                try:
                    loop.run_until_complete(notification_service.send_prediction_result(match))
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


# ─────────────────────────────
# BETCODES
# ─────────────────────────────
def run_betcodes():
    logging.info(f"🧾 Running get_betcodes at {datetime.now(LAGOS_TZ)}")
    try:
        count = get_betcodes.run()
        logging.info("✅ get_betcodes completed")
        notifier = MatchNotificationService()
        if count and count > 0:
            notifier.send_betcode_notification()
        else:
            logging.info("ℹ️ No new betcodes → skipping notification")
    except Exception as e:
        logging.exception("❌ get_betcodes failed")


# ─────────────────────────────
# TOP LEAGUE CHECKER
# ─────────────────────────────
def check_top_league_matches():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        cursor.execute("""
        SELECT 
            pt.fixture_id, pt.home_team, pt.away_team, pt.league, pt.prediction,
            pt.home_score, pt.away_score, pt.status, pt.match_datetime,
            nl.top_league_reminder_sent,
            nl.top_league_result_sent
        FROM pro_tips pt
        LEFT JOIN notification_log nl ON nl.fixture_id = pt.fixture_id
        WHERE pt.match_datetime BETWEEN NOW() - INTERVAL '3 hours'
                                    AND NOW() + INTERVAL '45 minutes'
        AND pt.status IN ('NS', 'FT')
    """)

        rows = cursor.fetchall()

        for row in rows:
            league = row.get('league', '')
            if not notification_service.is_top_league(league):
                continue

            status = row.get('status')
            fixture_id = row['fixture_id']

            if status == 'NS':
                match_time = row['match_datetime']
                if isinstance(match_time, str):
                    match_time = datetime.fromisoformat(match_time)
                minutes_until = (match_time - datetime.now()).total_seconds() / 60
                if 15 <= minutes_until <= 30:
                    print(f"🏆 Top-league match soon: {row['home_team']} vs {row['away_team']} ({league})")
                    try:
                        loop.run_until_complete(
                            notification_service.send_top_league_reminder(row)
                        )
                    except Exception as e:
                        print(f"❌ Top-league reminder failed for {fixture_id}: {e}")

            elif status == 'FT':
                if row.get('top_league_result_sent'):
                    continue
                print(f"🏆 Top-league result: {row['home_team']} vs {row['away_team']} ({league})")
                try:
                    loop.run_until_complete(
                        notification_service.send_top_league_result(row)
                    )
                except Exception as e:
                    print(f"❌ Top-league result failed for {fixture_id}: {e}")

    except Exception as e:
        print(f"🔥 check_top_league_matches crash: {e}")

    finally:
        loop.close()
        cursor.close()
        release_db(conn)



def check_vip_results():
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        cursor.execute("""
            SELECT 
                pt.fixture_id, pt.home_team, pt.away_team, pt.prediction,
                pt.home_score, pt.away_score, pt.status,
                nl.vip_result_sent
            FROM pro_tips pt
            JOIN vip_tips vt ON vt.fixture_id = pt.fixture_id
            LEFT JOIN notification_log nl ON nl.fixture_id = pt.fixture_id
            WHERE vt.vip_date = CURRENT_DATE
            AND pt.status = 'FT'
            AND (nl.vip_result_sent IS NULL OR nl.vip_result_sent = FALSE)
        """)

        rows = cursor.fetchall()

        for row in rows:
            print(f"💎 VIP match finished: {row['home_team']} vs {row['away_team']}")
            try:
                loop.run_until_complete(notification_service.send_vip_result(row))
            except Exception as e:
                print(f"❌ VIP result notification failed for {row['fixture_id']}: {e}")

    except Exception as e:
        print(f"🔥 check_vip_results crash: {e}")

    finally:
        loop.close()
        cursor.close()
        release_db(conn)




# ─────────────────────────────
# PREDICTIONS READY BROADCAST
# ─────────────────────────────
def send_predictions_ready():
    logging.info(f"📢 Sending predictions ready notification at {datetime.now(LAGOS_TZ)}")
    try:
        notifier = MatchNotificationService()
        notifier.send_predictions_ready()
    except Exception as e:
        logging.exception("❌ send_predictions_ready failed")
# ─────────────────────────────
# SCHEDULER
# ─────────────────────────────
scheduler = BlockingScheduler(timezone=LAGOS_TZ)

scheduler.add_job(
    refresh_live_predictions,
    'interval',
    minutes=5,
    max_instances=1,
    coalesce=True
)

scheduler.add_job(
    daily_pipeline,
    'cron',
    hour=1,
    minute=0,
    id='daily_pipeline',
    max_instances=1,
    coalesce=True
)

scheduler.add_job(
    run_betcodes,
    'cron',
    hour='6-23/3',
    minute=10,
    max_instances=1,
    coalesce=True
)



scheduler.add_job(
    check_top_league_matches,
    'interval',
    minutes=5,
    max_instances=1,
    coalesce=True
)


scheduler.add_job(
    check_vip_results,
    'interval',
    minutes=5,
    max_instances=1,
    coalesce=True
)


# 1:30 AM — after daily pipeline posts tips
scheduler.add_job(
    send_predictions_ready,
    'cron', hour=1, minute=30,
    id='predictions_ready_0130',
    max_instances=1, coalesce=True
)

# 7:30 AM — morning reminder
scheduler.add_job(
    send_predictions_ready,
    'cron', hour=7, minute=30,
    id='predictions_ready_0730',
    max_instances=1, coalesce=True
)

# 11:45 AM — midday reminder
scheduler.add_job(
    send_predictions_ready,
    'cron', hour=11, minute=45,
    id='predictions_ready_1145',
    max_instances=1, coalesce=True
)

# 3:30 PM — afternoon reminder
scheduler.add_job(
    send_predictions_ready,
    'cron', hour=15, minute=30,
    id='predictions_ready_1530',
    max_instances=1, coalesce=True
)

print("🚀 Worker started...")
print("   - Live updates every 5 minutes")
print("   - Top league checker every 5 minutes")
print("   - Daily pipeline at 1:00 AM")
print("   - Betcodes every 3 hours from 6 AM")

scheduler.start()