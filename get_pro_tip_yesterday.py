import csv
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date, timedelta
import kbt_load_env
from consts import global_consts as gc

# CSV output path
result_csv_f = gc.PRO_RESULTS_CSV


# ────────────────────────────────────────────────
# DB CONNECTION
# ────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        kbt_load_env.supabase_url,
        cursor_factory=RealDictCursor,
        sslmode="require"
    )


# ────────────────────────────────────────────────
# FETCH + EXPORT
# ────────────────────────────────────────────────
def fetch_past_fixture(match_date: str):
    """
    Fetch fixture IDs + match_time from Supabase
    and save to CSV.
    """
    results = []
    connection = None

    try:
        connection = get_db()
        cursor = connection.cursor()

        query = """
            SELECT fixture_id, match_time
            FROM pro_tips
            WHERE date = %s
            ORDER BY fixture_id ASC
        """

        match_date_obj = date.fromisoformat(match_date)

        cursor.execute(query, (match_date_obj,))
        results = cursor.fetchall()

        print(f"[INFO] Fetched {len(results)} matches for {match_date}")

        if results:
            with open(result_csv_f, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["fixture_id", "match_time"])

                for row in results:
                    match_time = row.get("match_time")
                    if match_time:
                        match_time = match_time.strftime("%H:%M:%S")

                    writer.writerow([row["fixture_id"], match_time])

            print(f"[INFO] Saved CSV → {result_csv_f}")

    except Exception as e:
        print("[ERROR] Failed to fetch matches:", e)

    finally:
        if connection:
            connection.close()
            print("[INFO] Connection closed")

    return results


# ────────────────────────────────────────────────
# RUN
# ────────────────────────────────────────────────
def run():
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    fetch_past_fixture(yesterday)


if __name__ == "__main__":
    run()