from csv import writer
import random

import kbt_funtions
from cmath import cos
from csv import DictReader, writer
import csv
import datetime
from lib2to3.pgen2 import driver
import random
import string
from urllib import request
import requests
from bs4 import BeautifulSoup as soup
import time
from wsgiref import headers
# importing webdriver from selenium
import requests
import os
import time
import io
import requests
import mysql.connector
from mysql.connector import errorcode
from datetime import date
from lxml import etree
import kbt_funtions
from consts import global_consts as gc
from datetime import datetime


date_ = gc.PRESENT_DAY_DATE
p_date = gc.PRESENT_DAY_DMY

csv_f = gc.BASKETBALL_CSV

additional_headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com",
    "Connection": "keep-alive"
}

my_headers = gc.MY_HEARDER

country_name = gc.COUNTRIES

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

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

def get_random_headers():
    return {**random.choice(headers_list), **additional_headers}

def get_country_name_from_code(img_code, countries):
    for country in countries:
        if img_code.lower() == country["2_code"].lower() or img_code.lower() == country["3_code"].lower():
            return country["name"]
    return ""

def scrape_basketball_data():
    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        headers = get_random_headers()

        context = browser.new_context(
            user_agent=headers.get("User-Agent"),
            locale="en-US",
            extra_http_headers=headers
        )

        page = context.new_page()
        page.goto("https://www.forebet.com/en/basketball/predictions-today")
        page.wait_for_selector("h1.frontH", timeout=60000)
        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")
    games = soup.select(".rcnt")
    print(f"Found {len(games)} games.")

    for i, game in enumerate(games):
        if i >= 50:  # ✅ Limit to first 50 games
            break

        try:
            league = game.select_one(".shortTag")
            home = game.select_one(".homeTeam span")
            away = game.select_one(".awayTeam span")
            date_ = game.select_one(".date_bah")
            prob1 = game.select_one(".fprc span:nth-of-type(1)")
            prob2 = game.select_one(".fprc span:nth-of-type(2)")
            pred_div = game.select_one("div.predict")

            pred = pred_div.get_text(strip=True) if pred_div else "N/A"

            result = "N/A"
            if pred_div and "class" in pred_div.attrs:
                class_list = pred_div["class"]
                if "predict_y" in class_list:
                    result = "Won"
                elif "predict_no" in class_list:
                    result = "Lost"

            score = game.select_one(".ex_sc b")
            avg_points = game.select_one(".avg_sc")

            img_tag = game.select_one('div.shortagDiv.tghov img')
            img_link = img_tag['src'] if img_tag else ""
            img_code = img_link.split('/')[-1].split('.')[0] if img_link else ""
            flag_name = get_country_name_from_code(img_code, country_name)

            a_tag = game.select_one("a.tnmscn")
            if a_tag and a_tag.has_attr("href"):
                href = a_tag["href"]
                path_parts = href.strip("/").split("/")
                league_slug = path_parts[3] if len(path_parts) > 4 else ""
                fixtures_link = f"https://www.forebet.com{href}" if href else ""
            else:
                league_slug = ""
                fixtures_link = ""

            league = league.text.strip() if league else ""
            home = home.text.strip() if home else "N/A"
            away = away.text.strip() if away else "N/A"

            date_time_str = date_.text.strip() if date_ else "N/A"
            if date_time_str != "N/A":
                try:
                    dt = datetime.strptime(date_time_str, "%d/%m/%Y %H:%M")
                    date_only = dt.strftime("%d-%m-%Y")
                    time_only = dt.strftime("%H:%M")
                except ValueError:
                    date_only = "N/A"
                    time_only = "N/A"
            else:
                date_only = "N/A"
                time_only = "N/A"

            prob1 = prob1.text.strip() if prob1 else "N/A"
            prob2 = prob2.text.strip() if prob2 else "N/A"
            score1 = score.text.strip() if score else "N/A"
            score2 = score.find_next("br").next_sibling.strip() if score else "N/A"
            avg_points = avg_points.text.strip() if avg_points else "N/A"
            code = kbt_funtions.get_code(8)

            row = [
                league,
                f"{home} vs {away}",
                pred,
                "",                     # odd
                time_only,              # match_time
                f"{score1} - {score2}",
                date_only,
                f"{flag_name} - {img_code}",
                result,
                code,
                "fb_basketball",
                f"{prob1} - {prob2}",
                avg_points,
                f"{score1} - {score2}",
                fixtures_link,
                league_slug,
            ]

            all_rows.append(row)

        except Exception as e:
            print("Error parsing game:", e)

    print(f"✅ Returning {len(all_rows)} games.")
    return all_rows


def save_to_csv(rows):
    with open(csv_f, "w", newline="", encoding="utf-8") as f:
        writer_ = csv.writer(f)
        writer_.writerow([
            "league", "fixtures", "tip", "odd", "match_time", "score", "date",
            "flag", "result", "code", "source", "rate", "avg_point","correct_score" ,"fixtures_link, league_slug"
        ])
        writer_.writerows(rows)
    print(f"Saved {len(rows)} rows to {csv_f}")



def post_to_mysql():
    # #insert into db

    # csv_f = "oddslot_data.csv"
    #NOTE::::::::::::when i experience bad connection: 10458 (28000) in ip i browse my ip address and paste it inside cpanel add host then copy my cpanel sharedhost ip
    #and paste here as my host ip address
    try:
        connection = kbt_funtions.db_connection()
        
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()

            print("You're connected to database: ", record)

                
        with open(csv_f, "r") as f:
        
            csv_data = csv.reader(f)
            for row in csv_data:
                if len(row) == 16:
                    cursor.execute(
                        'INSERT INTO basketball (league, fixtures, tip, odd, match_time, score, date, flag, result, code, source,rate, avg_point,correct_score,fixtures_link, league_slug)'
                        'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        row
                    )
                else:
                    print(f"Skipping row with incorrect number of values: {row}")

               

        print("Inserting tips now... ", time.ctime())
        print(cursor.rowcount," record(s) created==============", time.ctime())

        
        time.sleep(3) 
        print("==============Bot is taking a nap... whopps!==================== ", time.ctime())  
        print("============Bot deleting previous tips from  database:=============== ")


        cursor.execute('DELETE t1 FROM basketball AS t1 INNER JOIN basketball AS t2 WHERE t1.id < t2.id AND t1.fixtures = t2.fixtures AND t1.source = t2.source')

            
        print(cursor.rowcount," record(s) deleted==============", time.ctime()) 



    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password ", err)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print("Error while connecting to MySQL", err)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.commit()
            connection.close()
                
            print("MySQL connection is closed")


def main():
    rows = scrape_basketball_data()
    if rows:
        save_to_csv(rows)
        post_to_mysql()
    else:
        print("No games found or something went wrong.")


if __name__ == "__main__":
    main()
