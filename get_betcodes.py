import re
import time
import random
from datetime import datetime, date
import csv
import json
import logging
import psycopg2
import requests
from bs4 import BeautifulSoup
import kbt_funtions
from consts import global_consts as gc
import kbt_load_env
from psycopg2.extras import RealDictCursor
import pytz

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

lagos = pytz.timezone("Africa/Lagos")


# ────────────────────────────────────────────────
# DB
# ────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        kbt_load_env.supabase_url,
        cursor_factory=RealDictCursor,
        sslmode="require"
    )


def get_bet_codes(set_date):
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
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"},
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"},
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.818.62 Safari/537.36 Edg/90.0.818.62"},
        {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
        {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:88.0) Gecko/20100101 Firefox/88.0"},
        {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15"},
        {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
        {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"},
        {"User-Agent": "Mozilla/5.0 (Linux; Android 10; SM-G970F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36"},
        {"User-Agent": "Mozilla/5.0 (Android 10; Mobile; rv:88.0) Gecko/88.0 Firefox/88.0"},
        {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"},
        {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36"},
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"}
    ]

    additional_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com",
        "Connection": "keep-alive"
    }

    headers = {**random.choice(headers_list), **additional_headers}
    card_index = 0
    results = []

    # Compute fresh per-run values
    post_date = date.today().strftime("%Y-%m-%d")
    post_time = datetime.now().strftime("%H:%M:%S")

    for page_num in range(1, 4):
        url = f"https://convertbetcodes.com/c/free-bet-codes-for-today?page={page_num}"
        logger.info(f"Scraping page {page_num}: {url}")

        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Failed to retrieve page {page_num}. Status: {response.status_code}")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            for card in soup.find_all("div", class_="card"):
                try:
                    left_text = card.select_one(".row .col-6:nth-of-type(1)").get_text(strip=True).replace('\n', ' ')

                    odds_match = re.search(r'@([\d.]+)', left_text)
                    odds = odds_match.group(1) if odds_match else ""

                    float_left = card.select_one("span.float-left")
                    from_code = float_left.contents[0].strip('@') if float_left and float_left.contents else ""
                    from_code = from_code.replace('\r', '').replace('\n', '').strip()

                    from_platform = ""
                    if float_left:
                        code_elem = float_left.select_one("code")
                        if code_elem:
                            from_platform = code_elem.get_text(strip=True).split()[0]
                            if from_platform == "DB":
                                from_platform = "db_bet"

                    flag_icon_elem = float_left.select_one("span.flag-icon") if float_left else None
                    platform_icon_class = flag_icon_elem["class"][-1].split('-')[-1] if flag_icon_elem and "class" in flag_icon_elem.attrs else ""

                    allowed_platforms = ["1xbet", "betano", "betika", "betway", "betwinner", "sportybet", "betcorrect", "betking", "paripulse"]
                    if from_platform.lower() in allowed_platforms:
                        site = f"{from_platform}:{platform_icon_class}"
                    else:
                        site = from_platform

                    rate = kbt_funtions.get_random_rate()
                    booking_code_id = kbt_funtions.get_betcode_uid()
                    platform_color = kbt_funtions.get_platforms_json(from_platform)

                    try:
                        numeric_odds = float(odds)
                        price = 'premium' if numeric_odds > 1000 else 'free'
                    except (ValueError, TypeError):
                        price = 'free'

                    result = {
                        "site": site,
                        "code": from_code,
                        "odd": odds,
                        "rate": rate,
                        "email": 'support@bettingtipsnet.com',
                        "price": price,
                        "post_time": post_time,
                        "post_date": post_date,
                        "booking_code_id": booking_code_id,
                        "slip_result_link": '',
                        "platform_logo_link": platform_color,
                        "result": ""
                    }

                    results.append(result)
                    card_index += 1

                except Exception as e:
                    logger.error(f"Error parsing card: {e}")

        except Exception as e:
            logger.error(f"Exception while scraping page {page_num}: {e}")

        sleep_time = random.uniform(1, 3)
        logger.info(f"Sleeping for {sleep_time:.2f} seconds before next request.")
        time.sleep(sleep_time)

    # Deduplicate by code
    seen = {}
    for r in results:
        seen[r["code"]] = r
    results = list(seen.values())
    logger.info(f"After deduplication: {len(results)} unique codes")

    csv_filename = "csv_files/betcodes.csv"
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = ["site", "code", "odd", "rate", "email", "price", "post_time", "post_date", "booking_code_id", "slip_result_link", "platform_logo_link", "result"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    logger.info(f"Results saved to {csv_filename}")
    return len(results)


def connect_server(csv_filename):
    conn = get_db()
    cursor = conn.cursor()
    inserted = 0

    try:
        print("✅ Connected to PostgreSQL")

        with open(csv_filename, "r", encoding='utf-8') as f:
            csv_data = csv.reader(f)
            next(csv_data, None)

            for row in csv_data:
                try:
                    row = [val.strip() if isinstance(val, str) else val for val in row]
                    row = [val if val not in ("", None) else None for val in row]

                    code = row[1]
                    odd = row[2]
                    booking_id = row[8]

                    if not code:
                        print("⚠️ Skipping (empty code):", row)
                        continue
                    if not odd:
                        print("⚠️ Skipping (empty odd):", row)
                        continue
                    if not booking_id:
                        print("⚠️ Skipping (empty booking_code_id):", row)
                        continue

                    try:
                        row[8] = int(booking_id)
                    except:
                        print("⚠️ Invalid booking_code_id:", booking_id)
                        continue

                    try:
                        float(odd)
                    except:
                        print("⚠️ Invalid odd:", odd)
                        continue

                    cursor.execute("""
                        INSERT INTO booking_codes
                        (site, code, odd, rate, email, price, post_time, post_date, booking_code_id, slip_result_link, platform_logo_link, result)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (code) DO UPDATE SET
                            site               = EXCLUDED.site,
                            odd                = EXCLUDED.odd,
                            rate               = EXCLUDED.rate,
                            email              = EXCLUDED.email,
                            price              = EXCLUDED.price,
                            post_time          = EXCLUDED.post_time,
                            post_date          = EXCLUDED.post_date,
                            booking_code_id    = EXCLUDED.booking_code_id,
                            slip_result_link   = EXCLUDED.slip_result_link,
                            platform_logo_link = EXCLUDED.platform_logo_link,
                            result             = EXCLUDED.result
                    """, row)

                    inserted += 1

                except Exception as e:
                    print("❌ Row insert error:", e)

        conn.commit()
        print(f"✅ Inserted/updated {inserted} rows")

    except Exception as e:
        import traceback
        print("❌ DB ERROR:", repr(e))
        traceback.print_exc()
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

    return inserted


def run():
    """Main function that returns the number of inserted records"""
    try:
        logger.info("🚀 Running betcodes pipeline")

        set_date = datetime.now(lagos).date()   # ← fresh on every call
        scraped_count = get_bet_codes(set_date)
        logger.info(f"📊 Scraped {scraped_count} unique bet codes")

        csv_filename = "csv_files/betcodes.csv"
        inserted_count = connect_server(csv_filename)

        logger.info(f"🎯 Pipeline complete. Inserted/updated: {inserted_count} rows")
        return inserted_count

    except Exception as e:
        logger.error(f"❌ Error during pipeline execution: {e}")
        return 0


if __name__ == "__main__":
    result = run()
    print(f"\n📊 FINAL RESULT: {result} rows inserted/updated")
    exit(0 if result > 0 else 1)