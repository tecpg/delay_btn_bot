import csv
import datetime
import random
import time
import requests
from bs4 import BeautifulSoup as soup
from lxml import html
from consts import global_consts as gc
import kbt_funtions
from datetime import date

# Today's date
date_ = gc.PRESENT_DAY_YMD
p_date = gc.PRESENT_DAY_YMD

# CSV path
csv_f = gc.PRO_CSV


import re
from datetime import datetime, time

def is_allowed_match_time(time_str):
    """
    Allow ONLY 05:30 → 22:29
    Input may contain 'GMT'
    """

    if not time_str:
        return False

    # 🔹 Extract HH:MM ONLY (strip GMT or any text)
    match = re.search(r'(\d{1,2}:\d{2})', time_str)
    if not match:
        return False

    try:
        match_time = datetime.strptime(match.group(1), "%H:%M").time()
    except ValueError:
        return False

    # ❌ BLOCK 22:30 → 05:29
    if match_time >= time(22, 30) or match_time < time(5, 30):
        return False

    return True


def scrape_tips():
    session = requests.Session()
    my_headers = gc.MY_HEARDER
    url = "https://oddslot.com/tips/?page="

    predictions = []

    # Loop through pages
    for page in range(1, 10):
        webpage = requests.get(url + str(page), headers=my_headers)
        spider = soup(webpage.content, "html.parser")
        dom = html.fromstring(str(spider))

        for x in range(10):
            i = x + 1
            try:
                # 🔹 TIME
                time_node = dom.xpath(f'//tbody/tr[{i}]/td[1]')
                if not time_node:
                    continue
                timez = time_node[0].xpath("string()").strip()

                if not is_allowed_match_time(timez):
                    continue

                # 🔹 LEAGUE
                league_node = dom.xpath(f'//tbody/tr[{i}]/td[4]')
                if not league_node:
                    continue
                league = league_node[0].xpath("string()").strip()

                # 🔹 TEAMS
                home_node = dom.xpath(f'(//tbody/tr[{i}]//a[contains(@class,"team-cell")])[1]')
                away_node = dom.xpath(f'(//tbody/tr[{i}]//a[contains(@class,"team-cell")])[2]')

                if not (home_node and away_node):
                    continue

                home_teams = home_node[0].xpath("string()").strip()
                away_teams = away_node[0].xpath("string()").strip()

                if not home_teams or not away_teams or home_teams == away_teams:
                    continue

                # 🔹 ODDS
                odds_node = dom.xpath(f'//tbody/tr[{i}]//span[contains(@class,"odds-badge")]/text()')
                if not odds_node:
                    continue
                odds = float(odds_node[0].strip())

                # 🔹 PICKS
                picks_node = dom.xpath(f'//tbody/tr[{i}]//span[contains(@class,"prediction-badge")]/text()')
                picks = picks_node[0].strip() if picks_node else "N/A"

                # Replace picks
                if picks == "AWAY WIN":
                    picks = "AWAY DC"
                elif picks == "HOME WIN":
                    picks = "HOME DC"

                # 🔹 RATE (optional)
                rates_node = dom.xpath(f'//tbody/tr[{i}]//span[contains(@class,"chance-badge")]/text()')
                rates = rates_node[0].strip() if rates_node else ""

                # 🔹 RESULT / STATUS
                result_node = dom.xpath(f'//tbody/tr[{i}]//span[contains(@class,"result-badge")]/text()')
                result_text = result_node[0].strip() if result_node else ""

                if "NOT STARTED" not in result_text.upper():
                    continue
                else:
                    results = "?"
                    score = "?:?"

                source = "pro_tips"
                match_code = kbt_funtions.get_code(8)

                prediction = {
                    "league": league,
                    "fixtures": f"{home_teams} vs {away_teams}",
                    "tip": picks,
                    "odd": round(odds + 0.06, 2) if kbt_funtions.check_odd_range(odds) else odds,
                    "match_time": timez,
                    "score": score,
                    "date": date_,
                    "match_date": p_date,
                    "flag": "",
                    "result": results,
                    "code": match_code,
                    "source": source,
                    "protip": ""
                }

                predictions.append(prediction)

            except Exception as e:
                print(f"Error parsing row {i} on page {page}: {e}")
                continue

    # Shuffle and take max 45
    random.shuffle(predictions)
    predictions = predictions[:45]

    return predictions
# =========================


def save_predictions_to_csv(predictions, csv_file):
    if not predictions:
        print("⚠️ No predictions to save")
        return

    try:
        with open(csv_file, mode="w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            header = [
                "League",
                "Fixtures",
                "Tip",
                "Odd",
                "Match Time",
                "Score",
                "Date",
                "Match Date",
                "League Logo",
                "League Flag",
                "Home Flag",
                "Away Flag",
                "Flag",
                "Result",
                "Code",
                "Source",
                "Protip"
            ]
            writer.writerow(header)
            for match in predictions:
                row = [
                    match.get("league", ""),
                    match.get("fixtures", ""),
                    match.get("tip", ""),
                    match.get("odd", ""),
                    match.get("match_time", ""),
                    match.get("score", ""),
                    match.get("date", ""),
                    match.get("match_date", ""),
                    match.get("flag", ""),
                    match.get("league_logo", ""),
                    match.get("league_flag", ""),
                    match.get("home_flag", ""),
                    match.get("away_flag", ""),
                    match.get("result", ""),
                    match.get("code", ""),
                    match.get("source", ""),
                    match.get("protip", "")
                ]
                writer.writerow(row)

        print(f"✅ Predictions saved to {csv_file}")

    except Exception as e:
        print(f"Failed to write CSV: {e}")


def run():
    predictions = scrape_tips()
    save_predictions_to_csv(predictions, csv_f)


if __name__ == "__main__":
    run()
