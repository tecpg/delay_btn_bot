import re
import time
import random
import csv
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import RealDictCursor
import pytz

import kbt_funtions
import kbt_load_env
from consts import global_consts as gc

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

lagos = pytz.timezone("Africa/Lagos")
set_date = datetime.now(lagos).date()
post_time = datetime.now().strftime('%H:%M:%S')

CSV_FILE = "csv_files/betcodes.csv"

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
# CSV HELPERS
# ────────────────────────────────────────────────
def clear_csv():
    with open(CSV_FILE, "w", encoding="utf-8") as f:
        f.truncate(0)
    print("🧹 CSV cleared")

# ────────────────────────────────────────────────
# SCRAPER
# ────────────────────────────────────────────────
def get_bet_codes():
    results = []
    # Headers for requests
    headers_list = [

    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.2 Safari/605.1.15", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/91.0.864.59", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/92.0.902.55", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/93.0.961.38", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.818.62 Safari/537.36 Edg/90.0.818.62"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:88.0) Gecko/20100101 Firefox/88.0"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"},
    {"User-Agent": "Mozilla/5.0 (Linux; Android 10; SM-G970F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Android 10; Mobile; rv:88.0) Gecko/88.0 Firefox/88.0" },
    {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1" },
    {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"}

      ]

    additional_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com",
        "Connection": "keep-alive"
    }

    # Select a random header and merge with additional headers
    headers = {**random.choice(headers_list), **additional_headers}

    clear_csv()  # 🔥 CLEAR BEFORE WRITING

    for page in range(1, 4):
        url = f"https://convertbetcodes.com/c/free-bet-codes-for-today?page={page}"
        logger.info(f"Scraping page {page}")

        try:
            res = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(res.content, "html.parser")

            for card in soup.find_all("div", class_="card"):
                try:
                    text = card.get_text()

                    odds_match = re.search(r'@([\d.]+)', text)
                    odds = odds_match.group(1) if odds_match else None

                    code_elem = card.find("code")
                    code = code_elem.text.strip() if code_elem else None

                    if not code or not odds:
                        continue

                    platform = code.split()[0].lower()

                    result = {
                        "site": platform,
                        "code": code,
                        "odd": odds,
                        "rate": kbt_funtions.get_random_rate(),
                        "email": "support@bettingtipsnet.com",
                        "price": "premium" if float(odds) > 1000 else "free",
                        "post_time": post_time,
                        "post_date": set_date,
                        "booking_code_id": kbt_funtions.get_betcode_uid(),
                        "slip_result_link": None,              # ✅ FIXED
                        "platform_logo_link": None,            # ✅ FIXED
                        "result": None                         # ✅ FIXED
                    }

                    results.append(result)

                except Exception as e:
                    logger.error(f"Card parse error: {e}")

        except Exception as e:
            logger.error(f"Page error: {e}")

        time.sleep(random.uniform(1, 2))

    # ─────────────────────────────
    # WRITE CSV
    # ─────────────────────────────
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = [
            "site", "code", "odd", "rate", "email", "price",
            "post_time", "post_date", "booking_code_id",
            "slip_result_link", "platform_logo_link", "result"
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    logger.info(f"✅ CSV written with {len(results)} rows")

# ────────────────────────────────────────────────
# DB INSERT
# ────────────────────────────────────────────────
def connect_server():
    conn = get_db()
    cursor = conn.cursor()
    inserted = 0

    try:
        print("✅ Connected to PostgreSQL")

        with open(CSV_FILE, "r", encoding='utf-8') as f:
            csv_data = csv.reader(f)
            next(csv_data, None)

            for row in csv_data:
                try:
                    # 🔥 Normalize
                    row = [val.strip() if isinstance(val, str) else val for val in row]
                    row = [val if val not in ("", None) else None for val in row]

                    code = row[1]
                    odd = row[2]
                    booking_id = row[8]

                    # 🚫 Skip bad rows
                    if not code or not odd or not booking_id:
                        continue

                    row[8] = int(booking_id)

                    float(odd)  # validate

                    cursor.execute("""
                        INSERT INTO booking_codes 
                        (site, code, odd, rate, email, price, post_time, post_date, booking_code_id, slip_result_link, platform_logo_link, result)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (code) DO NOTHING
                    """, row)

                    if cursor.rowcount > 0:
                        inserted += 1

                except Exception as e:
                    print("❌ Row insert error:", e)
                    conn.rollback()

        conn.commit()
        print(f"✅ Inserted {inserted} new rows")

        # 🔥 CLEAR CSV ONLY IF SUCCESS
        if inserted > 0:
            clear_csv()
            print("🧹 CSV cleared after successful insert")

    except Exception as e:
        import traceback
        print("❌ DB ERROR:", repr(e))
        traceback.print_exc()
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

    return inserted

# ────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────
def run():
    try:
        logger.info("🚀 Running betcodes pipeline")

        get_bet_codes()
        inserted = connect_server()

        logger.info(f"🎯 Done. Inserted: {inserted}")

        return inserted

    except Exception as e:
        logger.error(f"❌ Pipeline error: {e}")
        return 0


if __name__ == "__main__":
    run()