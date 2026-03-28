import csv
import json
import mysql.connector
import redis
import kbt_funtions
from consts import global_consts as gc
import kbt_load_env

import psycopg2
from psycopg2.extras import RealDictCursor

API_RESULTS_CSV = gc.API_FOOTBALL_RESULTS_CSV
DB_FIXTURES_CSV = gc.PRO_RESULTS_CSV
MATCHED_CSV = gc.MATCHED_RESULTS_CSV

# ------------------------
# MySQL connection
# ------------------------
def get_db():
    return psycopg2.connect(
        kbt_load_env.supabase_url,
        cursor_factory=RealDictCursor,
        sslmode="require"
    )


# =========================
# Create matched CSV
# =========================
def create_matched_csv():

    api_results = {}

    # Load API results
    with open(API_RESULTS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            api_results[str(row["Fixture ID"])] = row

    db_fixture_ids = []

    # Load DB fixture ids
    with open(DB_FIXTURES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            db_fixture_ids.append(str(row["fixture_id"]))

    matched = []

    for fixture_id in db_fixture_ids:

        if fixture_id in api_results:

            r = api_results[fixture_id]

            matched.append({
                "FixtureID": fixture_id,
                "home_score": r["Home Score"],
                "away_score": r["Away Score"],
                "Status": r["Status"]
            })

    # Write matched results
    with open(MATCHED_CSV, "w", newline="", encoding="utf-8") as f:

        fieldnames = ["FixtureID", "home_score", "away_score", "Status"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(matched)

    print(f"✅ Matched results saved: {MATCHED_CSV}")
    print(f"Total matched fixtures: {len(matched)}")


# ------------------------
# Redis connection
# ------------------------
REDIS_URL = kbt_load_env.redis_url # or wherever your Redis URL is stored
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ------------------------
# Bulk MySQL update
# ------------------------
def update_postgres_bulk(csv_file):
    try:
        conn = get_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        fixture_ids = []
        home_scores = {}
        away_scores = {}
        statuses = {}
        dates_to_invalidate = set()

        updates_to_push = []

        # LOAD CSV
        with open(csv_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                fid = row["FixtureID"]

                if not fid:
                    continue

                fixture_ids.append(fid)

                home = int(row["home_score"] or 0)
                away = int(row["away_score"] or 0)
                status = row["Status"]

                home_scores[fid] = home
                away_scores[fid] = away
                statuses[fid] = status

                updates_to_push.append({
                    "fixture_id": int(fid),
                    "home_score": home,
                    "away_score": away,
                    "status": status
                })

        if not fixture_ids:
            print("No fixtures to update")
            return

        # 🔥 BULK UPDATE USING CASE
        home_case = "CASE fixture_id " + " ".join(
            [f"WHEN {fid} THEN {home_scores[fid]}" for fid in fixture_ids]
        ) + " END"

        away_case = "CASE fixture_id " + " ".join(
            [f"WHEN {fid} THEN {away_scores[fid]}" for fid in fixture_ids]
        ) + " END"

        status_case = "CASE fixture_id " + " ".join(
            [f"WHEN {fid} THEN '{statuses[fid]}'" for fid in fixture_ids]
        ) + " END"

        query = f"""
        UPDATE pro_tips
        SET home_score = {home_case},
            away_score = {away_case},
            status = {status_case},
            last_updated = NOW()
        WHERE fixture_id IN ({','.join(fixture_ids)})
        """

        # ✅ CORRECT EXECUTION
        cursor.execute(query)
        conn.commit()

        print(f"✅ Bulk update done: {cursor.rowcount} rows updated")

        # ------------------------
        # 🔥 REALTIME PUSH (NEW)
        # ------------------------
        for update in updates_to_push:
            redis_client.publish("live_scores", json.dumps(update))

        print(f"📡 Pushed {len(updates_to_push)} updates")

        # ------------------------
        # CACHE INVALIDATION
        # ------------------------
        for d in dates_to_invalidate:
            redis_client.delete(f"fixtures:{d}")

    except Exception as e:
        print("❌ DB ERROR:", e)
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
# =========================
# Run script
# =========================
if __name__ == "__main__":

    create_matched_csv()

    update_postgres_bulk(MATCHED_CSV)