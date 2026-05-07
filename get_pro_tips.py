import csv
import random
import requests
import json
import re
import unicodedata
from bs4 import BeautifulSoup as soup
from lxml import html
from datetime import datetime, time
from consts import global_consts as gc
import kbt_funtions

# ========================= CONFIG =========================
date_ = gc.PRESENT_DAY_YMD
p_date = gc.PRESENT_DAY_YMD
csv_f = gc.PRO_CSV

# ========================= HELPERS =========================

def normalize(text):
    """Optional: remove accents for search"""
    return unicodedata.normalize('NFKD', text)\
        .encode('ascii', 'ignore')\
        .decode('utf-8')

def is_allowed_match_time(time_str):
    if not time_str:
        return False

    match = re.search(r'(\d{1,2}:\d{2})', time_str)
    if not match:
        return False

    try:
        match_time = datetime.strptime(match.group(1), "%H:%M").time()
    except ValueError:
        return False

    return not (match_time >= time(22, 30) or match_time < time(5, 30))

# ========================= SCRAPER =========================

def scrape_tips():
    session = requests.Session()
    session.headers.update(gc.MY_HEARDER)

    url = "https://oddslot.com/tips/?page="
    predictions = []

    for page in range(1, 10):
        try:
            response = session.get(url + str(page), timeout=10)
            response.encoding = 'utf-8'  # ✅ FORCE UTF-8

            spider = soup(response.text, "html.parser")
            dom = html.fromstring(str(spider))

        except Exception as e:
            print(f"❌ Failed to fetch page {page}: {e}")
            continue

        for i in range(1, 11):
            try:
                # TIME
                time_node = dom.xpath(f'//tbody/tr[{i}]/td[1]')
                if not time_node:
                    continue

                timez = time_node[0].xpath("string()").strip()

                if not is_allowed_match_time(timez):
                    continue

                # LEAGUE
                league_node = dom.xpath(f'//tbody/tr[{i}]/td[4]')
                if not league_node:
                    continue

                league = league_node[0].xpath("string()").strip()

                # TEAMS
                home_node = dom.xpath(f'(//tbody/tr[{i}]//a[contains(@class,"team-cell")])[1]')
                away_node = dom.xpath(f'(//tbody/tr[{i}]//a[contains(@class,"team-cell")])[2]')

                if not (home_node and away_node):
                    continue

                home = home_node[0].xpath("string()").strip()
                away = away_node[0].xpath("string()").strip()

                if not home or not away or home == away:
                    continue

                # ODDS
                odds_node = dom.xpath(f'//tbody/tr[{i}]//span[contains(@class,"odds-badge")]/text()')
                if not odds_node:
                    continue

                odds = float(odds_node[0].strip())

                # PICKS
                picks_node = dom.xpath(f'//tbody/tr[{i}]//span[contains(@class,"prediction-badge")]/text()')
                picks = picks_node[0].strip() if picks_node else "N/A"

                if picks == "AWAY WIN":
                    picks = "AWAY DC"
                elif picks == "HOME WIN":
                    picks = "HOME DC"

                # STATUS
                result_node = dom.xpath(f'//tbody/tr[{i}]//span[contains(@class,"result-badge")]/text()')
                result_text = result_node[0].strip() if result_node else ""

                if "NOT STARTED" not in result_text.upper():
                    continue

                # FINAL OBJECT
                prediction = {
                    "league": league,
                    "league_normalized": normalize(league),

                    "home_team": home,
                    "home_team_normalized": normalize(home),

                    "away_team": away,
                    "away_team_normalized": normalize(away),

                    "fixtures": f"{home} vs {away}",
                    "tip": picks,
                    "odd": round(odds + 0.06, 2) if kbt_funtions.check_odd_range(odds) else odds,
                    "match_time": timez,
                    "score": "?:?",
                    "date": date_,
                    "match_date": p_date,
                    "result": "?",
                    "code": kbt_funtions.get_code(8),
                    "source": "pro_tips",
                    "protip": ""
                }

                predictions.append(prediction)

            except Exception as e:
                print(f"⚠️ Error parsing row {i} page {page}: {e}")
                continue

    random.shuffle(predictions)
    return predictions[:45]

# ========================= SAVE CSV =========================

def save_predictions_to_csv(predictions, csv_file):
    if not predictions:
        print("⚠️ No predictions to save")
        return

    try:
        with open(csv_file, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)

            writer.writerow([
                "League", "Fixtures", "Tip", "Odd", "Match Time",
                "Score", "Date", "Match Date", "Result", "Code", "Source"
            ])

            for match in predictions:
                writer.writerow([
                    match["league"],
                    match["fixtures"],
                    match["tip"],
                    match["odd"],
                    match["match_time"],
                    match["score"],
                    match["date"],
                    match["match_date"],
                    match["result"],
                    match["code"],
                    match["source"]
                ])

        print(f"✅ CSV saved: {csv_file}")

    except Exception as e:
        print(f"❌ CSV write error: {e}")

# ========================= OPTIONAL: SEND TO SUPABASE =========================

def to_json(predictions):
    return json.dumps(predictions, ensure_ascii=False)

# ========================= RUN =========================

def run():
    predictions = scrape_tips()
    save_predictions_to_csv(predictions, csv_f)

    # Optional: print JSON preview
    print(to_json(predictions[:2]))

if __name__ == "__main__":
    run()