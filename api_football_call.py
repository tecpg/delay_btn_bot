import requests
import csv
from datetime import datetime, date
from consts import global_consts as gc

API_KEY = "c45c4f7d3cf56a3173d13c30180aa40a"


def run():
    DATE = date.today().strftime("%Y-%m-%d")
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

            writer.writerow([
                "Fixture ID",
                "League",
                "League Logo",
                "League Flag",
                "League Country",
                "Date",
                "Match Time",
                "Home Team",
                "Home Logo",
                "Away Team",
                "Away Logo",
                "Home Score",
                "Away Score",
                "Status",
                "Elapsed",
                "Extra Time"
            ])

            for match in fixtures:
                fixture = match.get("fixture", {})
                league = match.get("league", {})
                teams = match.get("teams", {})
                goals = match.get("goals", {})

                fixture_id = fixture.get("id")

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

                league_name = league.get("name")
                league_logo = league.get("logo")
                league_flag = league.get("flag")
                league_country = league.get("country")

                home = teams.get("home", {})
                away = teams.get("away", {})

                home_name = home.get("name")
                home_logo = home.get("logo")

                away_name = away.get("name")
                away_logo = away.get("logo")

                score_home = goals.get("home")
                score_away = goals.get("away")

                status = fixture.get("status", {})
                status_short = ""
                elapsed = None
                extra = None

                if isinstance(status, dict):
                    status_short = status.get("short")
                    elapsed = status.get("elapsed")
                    extra = status.get("extra")
                else:
                    status_short = str(status)

                writer.writerow([
                    fixture_id,
                    league_name,
                    league_logo,
                    league_flag,
                    league_country,
                    match_date,
                    match_time,
                    home_name,
                    home_logo,
                    away_name,
                    away_logo,
                    score_home,
                    score_away,
                    status_short,
                    elapsed,
                    extra
                ])

                elapsed_str = f"{elapsed}'" if elapsed is not None else ""
                if extra:
                    elapsed_str = f"{elapsed}+{extra}'"

                print(
                    f"{fixture_id} | {match_date} {match_time} | "
                    f"{league_country or ''} - {league_name} | "
                    f"{home_name} {score_home or ''} - {score_away or ''} {away_name} | "
                    f"{status_short} {elapsed_str}"
                )

        print(f"\nAll matches saved to {csv_file}")

    except requests.exceptions.RequestException as e:
        print("API request failed:", e)
    except Exception as e:
        print("Unexpected error:", e)


if __name__ == "__main__":
    run()