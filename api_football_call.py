import requests
import csv
from datetime import datetime
from consts import global_consts as gc

API_KEY = "c45c4f7d3cf56a3173d13c30180aa40a"


def run():
    DATE = gc.PRESENT_DAY_YMD
    URL = f"https://v3.football.api-sports.io/fixtures?date={DATE}"

    headers = {"x-apisports-key": API_KEY}
    csv_file = gc.API_FOOTBALL_CSV

    try:
        response = requests.get(URL, headers=headers, timeout=30)
        response.raise_for_status()

        fixtures = response.json().get("response", [])
        print(f"Total matches on {DATE}: {len(fixtures)}\n")

        with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # CSV Header - Added League Country
            writer.writerow([
                "Fixture ID",
                "League",
                "League Logo",
                "League Flag",
                "League Country",      # ← NEW COLUMN
                "Date",
                "Match Time",
                "Home Team",
                "Home Logo",
                "Away Team",
                "Away Logo",
                "Home Score",
                "Away Score",
                "Status"
            ])

            for match in fixtures:
                fixture = match.get("fixture", {})
                league = match.get("league", {})
                teams = match.get("teams", {})
                goals = match.get("goals", {})

                fixture_id = fixture.get("id")

                # Extract datetime
                fixture_datetime = fixture.get("date")
                match_date = ""
                match_time = ""

                if fixture_datetime:
                    try:
                        dt = datetime.fromisoformat(fixture_datetime.replace("Z", "+00:00"))
                        match_date = dt.strftime("%Y-%m-%d")
                        match_time = dt.strftime("%H:%M")
                    except:
                        pass

                # League Information
                league_name = league.get("name")
                league_logo = league.get("logo")
                league_flag = league.get("flag")
                league_country = league.get("country")          # ← This is what you want

                # Teams
                home = teams.get("home", {})
                away = teams.get("away", {})

                home_name = home.get("name")
                home_logo = home.get("logo")

                away_name = away.get("name")
                away_logo = away.get("logo")

                # Scores
                score_home = goals.get("home")
                score_away = goals.get("away")

                # Status
                status = fixture.get("status", {})
                status_short = status.get("short") if isinstance(status, dict) else status

                # Write row with League Country
                writer.writerow([
                    fixture_id,
                    league_name,
                    league_logo,
                    league_flag,
                    league_country,           # ← Added here
                    match_date,
                    match_time,
                    home_name,
                    home_logo,
                    away_name,
                    away_logo,
                    score_home,
                    score_away,
                    status_short
                ])

                print(
                    f"{fixture_id} | {match_date} {match_time} | "
                    f"{league_country} - {league_name} | "
                    f"{home_name} {score_home} - {score_away} {away_name} | {status_short}"
                )

        print(f"\nAll matches saved to {csv_file}")

    except requests.exceptions.RequestException as e:
        print("API request failed:", e)
    except Exception as e:
        print("Unexpected error:", e)


if __name__ == "__main__":
    run()