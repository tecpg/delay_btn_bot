import csv
import mysql.connector
import redis
import kbt_funtions
from consts import global_consts as gc
import kbt_load_env

API_RESULTS_CSV = gc.API_FOOTBALL_RESULTS_CSV
DB_FIXTURES_CSV = gc.PRO_RESULTS_CSV
MATCHED_CSV = gc.MATCHED_RESULTS_CSV

# ------------------------
# MySQL connection
# ------------------------
def get_db():
    return kbt_funtions.db_connection()

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
def update_mysql_bulk(csv_file):
    try:
        connection = get_db()
        cursor = connection.cursor()
        
        fixture_ids = []
        home_scores = {}
        away_scores = {}
        statuses = {}
        dates_to_invalidate = set()

        # Load CSV
        with open(csv_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                fixture_id = row["FixtureID"]
                fixture_date = row.get("Date")  # optional if CSV has date
                if fixture_date:
                    dates_to_invalidate.add(fixture_date)

                if not fixture_id:
                    continue
                fixture_ids.append(fixture_id)
                home_scores[fixture_id] = row["home_score"]
                away_scores[fixture_id] = row["away_score"]
                statuses[fixture_id] = row["Status"]

        if not fixture_ids:
            print("No fixtures to update")
            return

        # Build CASE statements
        home_case = "CASE fixture_id " + " ".join(
            [f"WHEN {fid} THEN '{home_scores[fid]}'" for fid in fixture_ids]
        ) + " END"

        away_case = "CASE fixture_id " + " ".join(
            [f"WHEN {fid} THEN '{away_scores[fid]}'" for fid in fixture_ids]
        ) + " END"

        status_case = "CASE fixture_id " + " ".join(
            [f"WHEN {fid} THEN '{statuses[fid]}'" for fid in fixture_ids]
        ) + " END"

        # Single bulk update
        query = f"""
        UPDATE pro_tips
        SET home_score = {home_case},
            away_score = {away_case},
            status = {status_case}
        WHERE fixture_id IN ({','.join(fixture_ids)})
        """

        cursor.execute(query)
        connection.commit()

        print(f"✅ Bulk update done: {cursor.rowcount} rows updated")

        # ------------------------
        # Invalidate Redis cache
        # ------------------------
        for d in dates_to_invalidate:
            cache_key = f"fixtures:{d}"
            if redis_client.exists(cache_key):
                redis_client.delete(cache_key)
                print(f"🗑️  Redis cache invalidated for {cache_key}")

    except Exception as e:
        print("Error:", e)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
# =========================
# Run script
# =========================
if __name__ == "__main__":

    create_matched_csv()

    update_mysql_bulk(MATCHED_CSV)