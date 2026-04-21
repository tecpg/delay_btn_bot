import csv
import re
import unicodedata
from datetime import datetime, date
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor

from consts import global_consts as gc
import kbt_load_env

# ────────────────────────────────────────────────
# PATHS
# ────────────────────────────────────────────────
SCRAPED_CSV = gc.PRO_CSV
API_CSV = gc.API_FOOTBALL_CSV
OUTPUT_CSV = gc.MATCHED_FIXTURES_CSV


# ────────────────────────────────────────────────
# DB
# ────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        kbt_load_env.supabase_url,
        cursor_factory=RealDictCursor,
        sslmode="require"
    )


# ────────────────────────────────────────────────
# NORMALIZATION
# ────────────────────────────────────────────────
def normalize_team(name):
    name = name.lower()
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = re.sub(r'[^a-z\s]', ' ', name)
    name = re.sub(r'\b(b|ii|ad|fc|sc|cf|club|rs|rj)\b', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return set(w for w in name.split() if len(w) > 2)


def partial_match(db_home, db_away, api_home, api_away):
    db_home_words = normalize_team(db_home)
    db_away_words = normalize_team(db_away)
    api_home_words = normalize_team(api_home)
    api_away_words = normalize_team(api_away)

    return (
        (db_home_words & api_home_words and db_away_words & api_away_words) or
        (db_home_words & api_away_words and db_away_words & api_home_words)
    )


# ────────────────────────────────────────────────
# LOAD CSV
# ────────────────────────────────────────────────
def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ────────────────────────────────────────────────
# MATCH
# ────────────────────────────────────────────────



def get_matched_fixtures(api_fixtures, predictions):
    matched = []

    for fixture in api_fixtures:
        api_home = fixture["Home Team"]
        api_away = fixture["Away Team"]

        for pred in predictions:
            try:
                db_home, db_away = pred["Fixtures"].split(" vs ")
            except:
                continue

            if partial_match(db_home, db_away, api_home, api_away):

                match_time_str = fixture.get("Match Time")
                match_date_str = fixture.get("Date")

                match_time = None
                match_datetime = None

                try:
                    if match_time_str:
                         match_datetime = datetime.strptime(
                                f"{match_date_str} {match_time_str}",
                                "%Y-%m-%d %H:%M"
                            ).replace(tzinfo=ZoneInfo("UTC"))

                    if match_date_str and match_time:
                        # 🔥 combine date + time → UTC datetime
                        match_datetime = datetime.strptime(
                            f"{match_date_str} {match_time_str}",
                            "%Y-%m-%d %H:%M"
                        )
                except Exception as e:
                    print("❌ datetime parse error:", e)


               

                matched.append({
                    "fixture_id": int(fixture["Fixture ID"]),
                    "league": fixture["League"],
                    "league_logo": fixture.get("League Logo"),
                    "league_country": fixture.get("League Country"),
                    "date": fixture.get("Date"),

                    "match_time": match_time,
                    "match_datetime": match_datetime,  # ✅ ADD THIS

                    "home_team": api_home,
                    "home_logo": fixture.get("Home Logo"),
                    "away_team": api_away,
                    "away_logo": fixture.get("Away Logo"),

                    "home_score": fixture.get("Home Score") or 0,
                    "away_score": fixture.get("Away Score") or 0,

                    "status": fixture.get("Status"),
                    "elapsed": fixture.get("Elapsed"),      # ← Added
                    "extra":fixture.get("Extra"),          # ← Added

                    "prediction": pred["Tip"],
                    "odd": pred["Odd"],
                    "source": pred["Source"]
                })

                break

    return matched
# ────────────────────────────────────────────────
# SAVE CSV
# ────────────────────────────────────────────────
def save_to_csv(data):
    if not data:
        print("⚠️ No matched fixtures")
        return

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()

        for row in data:
            row_copy = row.copy()

            # convert TIME → string for CSV
            if row_copy.get("match_time"):
                row_copy["match_time"] = row_copy["match_time"].strftime("%H:%M:%S")

            writer.writerow(row_copy)

    print(f"✅ CSV saved → {OUTPUT_CSV}")

# ────────────────────────────────────────────────
# INSERT / UPSERT into pro_tips (POSTGRES)
# ────────────────────────────────────────────────
def insert_matched_fixtures(data):
    if not data:
        print("⚠️ No data to insert")
        return

    conn = get_db()
    cursor = conn.cursor()

    try:
        query = """
            INSERT INTO pro_tips (
                fixture_id, 
                league, 
                league_country,
                league_logo,
                home_team, 
                home_logo,
                away_team, 
                away_logo,
                match_time, 
                match_datetime, 
                date,
                prediction, 
                odd,
                home_score, 
                away_score,
                status,
                elapsed,        -- ← Added
                extra,          -- ← Added
                source
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (fixture_id)
            DO UPDATE SET
                prediction = EXCLUDED.prediction,
                odd = EXCLUDED.odd,
                source = EXCLUDED.source,
                match_datetime = EXCLUDED.match_datetime,
                league_country = EXCLUDED.league_country,
                elapsed = EXCLUDED.elapsed,           -- ← Update elapsed
                extra = EXCLUDED.extra,               -- ← Update extra
                last_updated = NOW()
        """

        values = []
        for r in data:
            values.append((
                r.get("fixture_id"),
                r.get("league"),
                r.get("league_country"),
                r.get("league_logo"),
                r.get("home_team"),
                r.get("home_logo"),
                r.get("away_team"),
                r.get("away_logo"),
                r.get("match_time"),
                r.get("match_datetime"),
                r.get("date"),
                r.get("prediction"),
                r.get("odd"),
                r.get("home_score") or 0,
                r.get("away_score") or 0,
                r.get("status"),
                r.get("elapsed"),      # ← Added
                r.get("extra"),        # ← Added
                r.get("source")
            ))

        cursor.executemany(query, values)
        conn.commit()

        print(f"✅ Inserted/Updated {cursor.rowcount} rows")

    except Exception as e:
        print("❌ DB ERROR:", e)
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


def insert_vip_tips(matched_data):
    if not matched_data:
        print("⚠️ No matched data for VIP")
        return

    import hashlib
    from datetime import date

    today = date.today()

    conn = get_db()
    cursor = conn.cursor()

    try:
        # 🔥 Check if VIP already exists for today
        cursor.execute("""
            SELECT COUNT(*) 
            FROM vip_tips 
            WHERE vip_date = %s
        """, (today,))
        count = cursor.fetchone()[0]

        if count > 0:
            print("✅ VIP already generated for today")
            return

        # 🔥 Deterministic "random" picks (stable per day)
        picks = sorted(
            matched_data,
            key=lambda x: hashlib.md5(
                (str(x["fixture_id"]) + str(today)).encode()
            ).hexdigest()
        )[:min(3, len(matched_data))]

        values = [(p["fixture_id"], today) for p in picks]

        cursor.executemany("""
            INSERT INTO vip_tips (fixture_id, vip_date)
            VALUES (%s, %s)
        """, values)

        conn.commit()

        print(f"🔥 VIP PICKS GENERATED: {len(values)}")

    except Exception as e:
        print("❌ VIP INSERT ERROR:", e)
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

# ────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────
def run():
    predictions = load_csv(SCRAPED_CSV)
    api_fixtures = load_csv(API_CSV)

    matched = get_matched_fixtures(api_fixtures, predictions)

    save_to_csv(matched)

    # 🔥 insert into pro_tips
    insert_matched_fixtures(matched)

    # 🔥 generate VIP from same matched data
    insert_vip_tips(matched)


if __name__ == "__main__":
    run()