import csv
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date
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
        sslmode="require"  # 🔥 important for Supabase
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

        # Convert string → date safely
        match_date_obj = date.fromisoformat(match_date)

        cursor.execute(query, (match_date_obj,))
        results = cursor.fetchall()

        print(f"[INFO] Fetched {len(results)} matches for {match_date}")

        # ─────────────────────────────────────────
        # WRITE TO CSV
        # ─────────────────────────────────────────
        if results:
            with open(result_csv_f, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                # Header
                writer.writerow(["fixture_id", "match_time"])

                for row in results:
                    match_time = row.get("match_time")

                    # 🔥 FIX: convert TIME → string
                    if match_time:
                        match_time = match_time.strftime("%H:%M:%S")

                    writer.writerow([
                        row["fixture_id"],
                        match_time
                    ])

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
if __name__ == "__main__":
    fetch_past_fixture(gc.YESTERDAY_YMD)