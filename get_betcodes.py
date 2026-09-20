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
BASE_URL          = "https://convertbetcodes.com/c/free-bet-codes-for-today"
CSV_PATH          = "csv_files/betcodes.csv"
PAGES             = range(1, 4)
PAGE_SLEEP        = (2, 5)
ALLOWED_PLATFORMS = {
    "1xbet", "betano", "betika", "betway", "betwinner",
    "sportybet", "betcorrect", "betking", "paripulse",
    "bet9ja", "paripesa", "msport", "db_bet",
}

# Each card on convertbetcodes.com is a conversion: SOURCE code → CONVERTED code.
# Both are real booking codes on their own platform, so we can keep either or both.
INCLUDE_SOURCE_CODE    = True   # left side  (e.g. betjam  3QBR3)
INCLUDE_CONVERTED_CODE = True   # right side (e.g. helabet ZPN83)

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
# Parsing — convertbetcodes.com HTML structure
#
# Each conversion is one card:
#
#   <div class="card">
#     <div class="row">
#       <div class="col-6"><span>1events <br>@2.01 odds</span></div>      ← source stats
#       <div class="col-6 text-right"><span>1events <br>@2.19 odds</span></div>  ← converted stats
#     </div>
#     <h4>
#       <span class="float-left">
#         3QBR3 <br>                                                      ← source code
#         <code class="badge">betjam <span class="flag-icon flag-icon-ng"></span></code>
#       </span>
#       <i class="conversion-arrow"></i>
#       <span class="float-right">
#         ZPN83 <br>                                                      ← converted code
#         <code class="badge">helabet <span class="flag-icon flag-icon-ng"></span></code>
#       </span>
#     </h4>
#     <small><a href=".../c/7196267/...">10 minutes ago</a> ...</small>
#     <div class="modal">...</div>                                        ← ignored
#   </div>
# ─────────────────────────────────────────────
def parse_side(side_span, stats_col) -> tuple[str, str, str, str] | None:
    """
    Parse one side of a conversion card.
    Returns (code, platform, country_code, odds) or None.
    """
    # ── Booking code: first non-empty direct text node (before the <br>) ──
    code = ""
    for node in side_span.find_all(string=True, recursive=False):
        text = node.strip()
        if text:
            code = text
            break
    if not code:
        return None

    # ── Platform + country flag: <code class="badge"> ──
    badge = side_span.select_one("code.badge")
    if not badge:
        return None

    platform = badge.get_text(strip=True).lower()
    if not platform:
        return None
    if platform == "db":
        platform = "db_bet"

    country_code = ""
    flag = badge.select_one(".flag-icon")
    if flag:
        for cls in flag.get("class", []):
            if cls.startswith("flag-icon-"):
                country_code = cls.replace("flag-icon-", "")
                break

    # ── Odds: "1events @2.01 odds" in the matching stats column ──
    odds = ""
    if stats_col:
        m = re.search(r"@\s*([\d.]+)", stats_col.get_text(" ", strip=True))
        if m:
            odds = m.group(1)

    return code, platform, country_code, odds


def build_record(code, platform, country_code, odds, post_date, post_time) -> dict:
    site = (
        f"{platform}:{country_code}"
        if platform in ALLOWED_PLATFORMS and country_code
        else platform
    )

    try:
        price = "premium" if float(odds) > 1000 else "free"
    except (ValueError, TypeError):
        price = "free"

    return {
        "site":               site,
        "code":               code,
        "odd":                odds,
        "rate":               kbt_funtions.get_random_rate(),
        "email":              "support@bettingtipsnet.com",
        "price":              price,
        "post_time":          post_time,
        "post_date":          post_date,
        "booking_code_id":    kbt_funtions.get_betcode_uid(),
        "slip_result_link":   "",
        "platform_logo_link": kbt_funtions.get_platforms_json(platform),
        "result":             "",
    }


def parse_card(card, post_date: str, post_time: str) -> list[dict]:
    """Parse one conversion card into 0–2 records (source and/or converted code)."""
    try:
        left  = card.select_one("h4 > span.float-left")
        right = card.select_one("h4 > span.float-right")
        if not left or not right:
            return []

        # Stats columns: first = source side, second = converted side
        stat_cols = card.select("div.row > div.col-6")
        left_stats  = stat_cols[0] if len(stat_cols) > 0 else None
        right_stats = stat_cols[1] if len(stat_cols) > 1 else None

        sides = []
        if INCLUDE_SOURCE_CODE:
            sides.append((left, left_stats))
        if INCLUDE_CONVERTED_CODE:
            sides.append((right, right_stats))

        records = []
        for span, stats in sides:
            parsed = parse_side(span, stats)
            if parsed:
                code, platform, country_code, odds = parsed
                records.append(
                    build_record(code, platform, country_code, odds, post_date, post_time)
                )
        return records

    except Exception as e:
        logger.error(f"Card parse error: {e}")
        return []


# ─────────────────────────────────────────────
# Scraper
# ─────────────────────────────────────────────
def scrape_betcodes() -> int:
    post_date = datetime.now(LAGOS).strftime("%Y-%m-%d")
    post_time = datetime.now(LAGOS).strftime("%H:%M:%S")
    raw_results: list[dict] = []

    for page_num in PAGES:
        url = f"{BASE_URL}?page={page_num}"
        logger.info(f"Scraping page {page_num}: {url}")

        response = fetch_page(url)
        if not response:
            continue

        soup = BeautifulSoup(response.content, "html.parser")

        # One div.card per conversion (the modal inside it has no .card class)
        cards = soup.select("div.card")
        logger.info(f"  Found {len(cards)} cards on page {page_num}")

        for card in cards:
            raw_results.extend(parse_card(card, post_date, post_time))

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
    logger.info("🚀 Betcodes pipeline starting (source: convertbetcodes.com)")

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