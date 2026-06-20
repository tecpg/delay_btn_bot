import re
import time
import random
import csv
import logging
import traceback
from datetime import datetime

import psycopg2
import requests
from bs4 import BeautifulSoup
from psycopg2.extras import RealDictCursor
import pytz

import kbt_funtions
import kbt_load_env

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────
LAGOS             = pytz.timezone("Africa/Lagos")
BASE_URL          = "http://paqbet.com/pg/bet-codes"
CSV_PATH          = "csv_files/betcodes.csv"
PAGES             = range(1, 4)
PAGE_SLEEP        = (2, 5)
ALLOWED_PLATFORMS = {
    "1xbet", "betano", "betika", "betway", "betwinner",
    "sportybet", "betcorrect", "betking", "paripulse",
    "bet9ja", "paripesa", "msport", "db_bet",
}

USER_AGENTS = [
    # Chrome - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    # Firefox - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    # Edge - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/90.0.818.62",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/91.0.864.59",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/92.0.902.55",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/93.0.961.38",
    # Chrome - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    # Firefox - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:88.0) Gecko/20100101 Firefox/88.0",
    # Safari - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    # Chrome - Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    # Firefox - Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
    # Chrome - Android
    "Mozilla/5.0 (Linux; Android 10; SM-G970F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36",
    # Firefox - Android
    "Mozilla/5.0 (Android 10; Mobile; rv:88.0) Gecko/88.0 Firefox/88.0",
    # Safari - iOS
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    # Chrome - iOS
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36",
]

# ─────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        kbt_load_env.supabase_url,
        cursor_factory=RealDictCursor,
        sslmode="require",
    )


# ─────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────
def make_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com",
        "Connection": "keep-alive",
    }


def fetch_page(url: str, retries: int = 3) -> requests.Response | None:
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=make_headers(), timeout=30)
            if r.status_code == 200:
                return r
            logger.warning(f"[Attempt {attempt}/{retries}] Status {r.status_code} → {url}")
        except requests.RequestException as e:
            logger.error(f"[Attempt {attempt}/{retries}] Error: {e}")
        if attempt < retries:
            time.sleep(2 ** attempt)
    logger.error(f"All {retries} attempts failed for {url}")
    return None


# ─────────────────────────────────────────────
# Parsing — paqbet.com HTML structure
#
# Each card looks like:
#   <div class="card mg-b-5">
#     <h4>
#       <span class="float-left">
#         <small>PLATFORM</small>
#         <span class="badge">N events <span class="flag-icon flag-icon-ng"></span></span>
#       </span>
#       <span class="float-right">
#         BOOKING_CODE
#         <span class="badge">@ODDS odds</span>
#       </span>
#     </h4>
#   </div>
# ─────────────────────────────────────────────
def parse_card(card, post_date: str, post_time: str) -> dict | None:
    try:
        # ── Platform (e.g. "bet9ja", "1xbet") ──
        platform_elem = card.select_one("h4 .float-left small")
        if not platform_elem:
            return None
        from_platform = platform_elem.get_text(strip=True).lower()
        if from_platform == "db":
            from_platform = "db_bet"

        # ── Country flag class (e.g. "ng") ──
        flag_elem = card.select_one("h4 .float-left .flag-icon")
        country_code = ""
        if flag_elem:
            classes = flag_elem.get("class", [])
            for cls in classes:
                if cls.startswith("flag-icon-"):
                    country_code = cls.replace("flag-icon-", "")
                    break

        # ── Booking code and odds — inside float-right ──
        float_right = card.select_one("h4 .float-right")
        if not float_right:
            return None

        # The booking code is the first text node inside float-right
        raw_text = float_right.get_text(separator="|", strip=True)
        # raw_text looks like: "5HSG9P9|@3.34 odds"
        parts = [p.strip() for p in raw_text.split("|") if p.strip()]

        from_code = parts[0] if parts else ""
        from_code = from_code.strip("@").strip()
        if not from_code:
            return None

        # Odds: find the badge inside float-right
        odds_badge = float_right.select_one(".badge")
        odds_text = odds_badge.get_text(strip=True) if odds_badge else ""
        odds_match = re.search(r"@([\d.]+)", odds_text)
        odds = odds_match.group(1) if odds_match else ""

        # ── Build site field ──
        site = (
            f"{from_platform}:{country_code}"
            if from_platform in ALLOWED_PLATFORMS
            else from_platform
        )

        # ── Price tier ──
        try:
            price = "premium" if float(odds) > 1000 else "free"
        except (ValueError, TypeError):
            price = "free"

        return {
            "site":               site,
            "code":               from_code,
            "odd":                odds,
            "rate":               kbt_funtions.get_random_rate(),
            "email":              "support@bettingtipsnet.com",
            "price":              price,
            "post_time":          post_time,
            "post_date":          post_date,
            "booking_code_id":    kbt_funtions.get_betcode_uid(),
            "slip_result_link":   "",
            "platform_logo_link": kbt_funtions.get_platforms_json(from_platform),
            "result":             "",
        }

    except Exception as e:
        logger.error(f"Card parse error: {e}")
        return None


# ─────────────────────────────────────────────
# Scraper
# ─────────────────────────────────────────────
def scrape_betcodes() -> int:
    post_date = datetime.now(LAGOS).strftime("%Y-%m-%d")
    post_time = datetime.now(LAGOS).strftime("%H:%M:%S")
    raw_results: list[dict] = []

    for page_num in PAGES:
        url = f"{BASE_URL}?&page={page_num}"
        logger.info(f"Scraping page {page_num}: {url}")

        response = fetch_page(url)
        if not response:
            continue

        soup = BeautifulSoup(response.content, "html.parser")

        # paqbet wraps each code in a div.mg-y-10 > div.card.mg-b-5
        # We only want the summary cards (not the modal copies)
        # The summary card is always the FIRST .card inside each .mg-y-10 wrapper
        wrappers = soup.select("div.mg-y-10")
        logger.info(f"  Found {len(wrappers)} code blocks on page {page_num}")

        for wrapper in wrappers:
            # First card only (the visible summary, not the modal duplicate)
            card = wrapper.select_one("div.card.mg-b-5")
            if not card:
                continue
            record = parse_card(card, post_date, post_time)
            if record:
                raw_results.append(record)

        sleep = random.uniform(*PAGE_SLEEP)
        logger.info(f"  Sleeping {sleep:.1f}s…")
        time.sleep(sleep)

    # Deduplicate by booking code
    unique = {r["code"]: r for r in raw_results}
    results = list(unique.values())
    logger.info(f"Unique codes after deduplication: {len(results)}")

    # Write CSV
    fieldnames = [
        "site", "code", "odd", "rate", "email", "price",
        "post_time", "post_date", "booking_code_id",
        "slip_result_link", "platform_logo_link", "result",
    ]
    with open(CSV_PATH, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    logger.info(f"CSV written → {CSV_PATH}")
    return len(results)


# ─────────────────────────────────────────────
# DB upsert
# ─────────────────────────────────────────────
def upsert_to_db(csv_path: str) -> int:
    conn = get_db()
    cursor = conn.cursor()
    inserted = 0

    INSERT_SQL = """
        INSERT INTO booking_codes
            (site, code, odd, rate, email, price, post_time, post_date,
             booking_code_id, slip_result_link, platform_logo_link, result)
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
    """

    try:
        logger.info("Connected to PostgreSQL")
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row = {k: (v.strip() if v and v.strip() else None) for k, v in row.items()}

                if not row.get("code"):
                    continue
                if not row.get("odd"):
                    continue

                try:
                    booking_id = int(row["booking_code_id"])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid booking_code_id: {row.get('booking_code_id')}")
                    continue

                try:
                    float(row["odd"])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid odd: {row.get('odd')}")
                    continue

                cursor.execute(INSERT_SQL, (
                    row["site"], row["code"], row["odd"], row["rate"],
                    row["email"], row["price"], row["post_time"], row["post_date"],
                    booking_id, row["slip_result_link"],
                    row["platform_logo_link"], row["result"],
                ))
                inserted += 1

        conn.commit()
        logger.info(f"Upserted {inserted} rows")

    except Exception as e:
        logger.error(f"DB error: {e}")
        traceback.print_exc()
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

    return inserted


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
def run() -> int:
    logger.info("🚀 Betcodes pipeline starting (source: paqbet.com)")

    scraped = scrape_betcodes()
    logger.info(f"📥 Scraped {scraped} unique codes")

    if scraped == 0:
        logger.warning("⚠️  No codes scraped — skipping DB upsert")
        return 0

    inserted = upsert_to_db(CSV_PATH)
    logger.info(f"✅ Pipeline complete — {inserted} rows upserted")
    return inserted


if __name__ == "__main__":
    result = run()
    print(f"\n📊 FINAL RESULT: {result} rows inserted/updated")
    exit(0 if result > 0 else 1)