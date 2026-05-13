import re
import time
import random
from datetime import datetime, timedelta
import csv
import json
import logging
import psycopg2
import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import errorcode
import csv
import time
import kbt_funtions

# Custom functions (ensure these are defined elsewhere)
import kbt_funtions
import mysql
from consts import global_consts as gc
import kbt_load_env
import psycopg2
from psycopg2.extras import RealDictCursor

from consts import global_consts as gc
import kbt_load_env

# Configure logging for better tracking
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import pytz
from datetime import datetime

lagos = pytz.timezone("Africa/Lagos")

set_date = datetime.now(lagos).date()

post_time = datetime.now().strftime('%H:%M:%S')

# ────────────────────────────────────────────────
# DB
# ────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        kbt_load_env.supabase_url,
        cursor_factory=RealDictCursor,
        sslmode="require"
    )

# print(post_time)
def get_bet_codes(set_date):
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
    base_time = datetime.now()
    card_index = 0
    results = []

    # Loop through pages 1 to 3
    for page_num in range(1, 4):
        url = f"https://convertbetcodes.com/c/free-bet-codes-for-today?page={page_num}"
        logger.info(f"Scraping page {page_num}: {url}")

        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Failed to retrieve page {page_num}. Status: {response.status_code}")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            # Loop through each card
            for card in soup.find_all("div", class_="card"):
                try:
                    # Extracting data from each card
                   # Extracting the text from the card
                    left_text = card.select_one(".row .col-6:nth-of-type(1)").get_text(strip=True).replace('\n', ' ')
                   
                    
                    # Regular expression to extract the odds (e.g., 3.42 from '2events@3.42 odds')
                    odds_match = re.search(r'@([\d.]+)', left_text)
                    
                    # Check if we found a match
                    if odds_match:
                        odds = odds_match.group(1)  # This will give the odds as a string (e.g., '3.42')
                    else:
                        odds = ""  # If no odds are found, set to an empty string or a default value
                    
                    float_left = card.select_one("span.float-left")
                    from_code = float_left.contents[0].strip('@') if float_left and float_left.contents else ""
                    from_code =  from_code.replace('\r', '').replace('\n', '').strip()

                    from_platform = ""

                    if float_left:
                        code_elem = float_left.select_one("code")
                        if code_elem:
                            from_platform = code_elem.get_text(strip=True).split()[0]

                            # Normalize platform name if it's 'DB'
                            if from_platform == "DB":
                                from_platform = "db_bet"


                    flag_icon_elem = float_left.select_one("span.flag-icon") if float_left else None
                    platform_icon_class = flag_icon_elem["class"][-1].split('-')[-1] if flag_icon_elem and "class" in flag_icon_elem.attrs else ""

                    # Generating the current post time
                    # post_time_dt = base_time + timedelta(minutes=-13 * card_index)
                    # post_time = post_time_dt.strftime('%H:%M:%S')

                    # print(post_time)
                    


                    post_date = gc.PRESENT_DAY_YMD
                  # List of allowed platforms
                    allowed_platforms = ["1xbet", "betano", "betika", "betway", "betwinner", "sportybet", "betcorrect", "betking", "paripulse"]

                    # Check if the 'from_platform' is in the allowed platforms list
                    if from_platform.lower() in allowed_platforms:
                        site = f"{from_platform}:{platform_icon_class}"
                    else:
                        site = from_platform  # You can leave this as an empty string or set it to some other value if you prefer


                    # Generating random rate and booking code ID
                    rate = kbt_funtions.get_random_rate()
                    booking_code_id = kbt_funtions.get_betcode_uid()
                    platform_color = kbt_funtions.get_platforms_json(from_platform)

                    # Forming the result for this card
                    try:
                        numeric_odds = float(odds)  # Convert the string to a float
                        price = 'premium' if numeric_odds > 1000 else 'free'
                    except (ValueError, TypeError):
                        price = 'free'  # Fallback if conversion fails

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



                    # Append result to the results list
                    results.append(result)
                    card_index += 1

                except Exception as e:
                    logger.error(f"Error parsing card: {e}")

        except Exception as e:
            logger.error(f"Exception while scraping page {page_num}: {e}")

        # Rate limiting: Sleep for a random time between 1 and 3 seconds between requests
        sleep_time = random.uniform(1, 3)
        logger.info(f"Sleeping for {sleep_time:.2f} seconds before next request.")
        time.sleep(sleep_time)  # Add sleep time between requests

    # Exporting results to CSV
    csv_filename = "csv_files/betcodes.csv"
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = ["site", "code", "odd", "rate", "email", "price", "post_time", "post_date", "booking_code_id", "slip_result_link", "platform_logo_link", "result"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    logger.info(f"Results saved to {csv_filename}")

   



def connect_server(csv_filename):
    conn = get_db()
    cursor = conn.cursor()

    try:
        print("✅ Connected to PostgreSQL")

        inserted = 0

        with open(csv_filename, "r", encoding='utf-8') as f:
            csv_data = csv.reader(f)
            next(csv_data, None)  # skip header

            for row in csv_data:
                try:
                    # 🔥 Normalize values
                    row = [val.strip() if isinstance(val, str) else val for val in row]
                    row = [val if val not in ("", None) else None for val in row]

                    code = row[1]
                    odd = row[2]
                    booking_id = row[8]

                    # 🚫 Skip invalid rows
                    if not code:
                        print("⚠️ Skipping (empty code):", row)
                        continue

                    if not odd:
                        print("⚠️ Skipping (empty odd):", row)
                        continue

                    if not booking_id:
                        print("⚠️ Skipping (empty booking_code_id):", row)
                        continue

                    # 🔥 Convert booking_code_id
                    try:
                        row[8] = int(booking_id)
                    except:
                        print("⚠️ Invalid booking_code_id:", booking_id)
                        continue

                    # 🔥 Validate odd
                    try:
                        float(odd)
                    except:
                        print("⚠️ Invalid odd:", odd)
                        continue

                    # ✅ Insert
                    cursor.execute("""
                        INSERT INTO booking_codes 
                        (site, code, odd, rate, email, price, post_time, post_date, booking_code_id, slip_result_link, platform_logo_link, result)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, row)

                    inserted += 1

                except Exception as e:
                    print("❌ Row insert error:", e)
                    conn.rollback()  # reset failed transaction

        # ✅ Commit AFTER loop
        conn.commit()
        print(f"✅ Inserted {inserted} rows")

        # 🔥 Cleanup duplicates (PostgreSQL safe)
        cursor.execute("""
            DELETE FROM booking_codes
            WHERE id NOT IN (
                SELECT MAX(id)
                FROM booking_codes
                GROUP BY code
            )
        """)
        conn.commit()

        print(f"🧹 Deleted {cursor.rowcount} duplicates")

    except Exception as e:
        import traceback
        print("❌ DB ERROR:", repr(e))
        traceback.print_exc()
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        
def run():
    try:
        get_bet_codes(set_date)
        # Insert data into the database (pass the csv_filename)
        # Generate the CSV filename for the current date
        csv_filename = f"bet_codes_{set_date}.csv"
        connect_server(csv_filename)
    except Exception as e:
        logger.error(f"Error during the run: {e}")



if __name__ == "__main__":
    run()
