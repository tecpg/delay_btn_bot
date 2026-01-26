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


date_ = gc.PRESENT_DAY_YMD
p_date =  gc.PRESENT_DAY_YMD




csv_f = gc.VIP_CSV

def post_tips():
    session = requests.Session()
    my_headers = gc.MY_HEARDER
    url = "https://oddslot.com/tips/?page="
    dt = []
    first_item = True  # Only the first match across all pages gets 'free'
    free_limit = 2  # how many items should be marked as 'free'

    for page in range(1, 10):
        webpage = requests.get(url + str(page), headers=my_headers)
        spider = soup(webpage.content, "html.parser")
        dom = etree.HTML(str(spider))

        for x in range(0, 10):
            i = str(1 + x)

            try:
                league = dom.xpath(f'/html/body/div[2]/div[4]/div/div/div/div/div[2]/div/table/tbody/tr[{i}]/td[4]/strong')[0].text
                timez = dom.xpath(f'/html/body/div[2]/div[4]/div/div/div/div/div[2]/div/table/tbody/tr[{i}]/td[1]/strong')[0].text
                picks = dom.xpath(f'/html/body/div[2]/div[4]/div/div/div/div/div[2]/div/table/tbody/tr[{i}]/td[7]/strong')[0].text
                home_teams = dom.xpath(f'/html/body/div[2]/div[4]/div/div/div/div/div[2]/div/table/tbody/tr[{i}]/td[2]/div/div/a/h4/strong')[0].text
                away_teams = dom.xpath(f'/html/body/div[2]/div[4]/div/div/div/div/div[2]/div/table/tbody/tr[{i}]/td[3]/div/div/a/h4/strong')[0].text
                odds = float(dom.xpath(f'/html/body/div[2]/div[4]/div/div/div/div/div[2]/div/table/tbody/tr[{i}]/td[6]/a/strong')[0].text)
                rates = dom.xpath(f'/html/body/div[2]/div[4]/div/div/div/div/div[2]/div/table/tbody/tr[{i}]/td[5]/strong')[0].text

                result_text = dom.xpath(f'/html/body/div[2]/div[4]/div/div/div/div/div[2]/div/table/tbody/tr[{i}]/td[8]/a')[0].text
                if result_text.find("NOT STARTED") == -1:
                    continue
                else:
                    results = score = "Not Yet"
                # Assign 'free' to the first two items
                
                protip = 'free' if free_limit > 0 else 'premium'
                if free_limit > 0:
                    free_limit -= 1

                source = "vip_tips"
                flag = ""
                match_date = p_date
                _date = p_date
                match_code = kbt_funtions.get_code(8)

                prediction = [
                    league,
                    home_teams + ' vs ' + away_teams,
                    picks,
                    odds,
                    timez,
                    score,
                    match_date,
                    _date,
                    flag,
                    results,
                    match_code,
                    source,
                    protip
                ]
                dt.append(prediction)

            except Exception as e:
                print(f"Error on item {i}: {e}")
                continue

    # Shuffle and select
    random.shuffle(dt)
    selected_list = dt[:20]


    # Write to CSV
    with open(csv_f, "w", encoding="utf-8", newline="") as f:
        thewriter = writer(f)
        free_assigned = 0  # Counter for how many 'free' tags we've used

        for sublist in selected_list:
            if kbt_funtions.check_odd_range(sublist[3]):
                if free_assigned <= 2:
                    sublist[12] = 'free'
                    free_assigned += 1
                else:
                    sublist[12] = 'premium'

                sublist[3] = round(sublist[3] + 0.04, 2)
                match_list = [str(value) for value in sublist]
                thewriter.writerow(match_list)
                print(match_list)



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
                if len(row) == 13:
                    cursor.execute(
                        'INSERT INTO soccerpunt(league, fixtures, tip, odd, match_time, score, date, match_date, flag, result, code, source, protip)'
                        'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        row
                    )
                else:
                    print(f"Skipping row with incorrect number of values: {row}")

               

        print("Inserting tips now... ", time.ctime())
        print(cursor.rowcount," record(s) created==============", time.ctime())

        
        time.sleep(3) 
        print("==============Bot is taking a nap... whopps!==================== ", time.ctime())  
        print("============Bot deleting previous tips from  database:=============== ")


        cursor.execute('DELETE t1 FROM soccerpunt AS t1 INNER JOIN soccerpunt AS t2 WHERE t1.id < t2.id AND t1.fixtures = t2.fixtures AND t1.source = t2.source')

            
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


def run():
    post_tips()
    post_to_mysql()


if __name__ == "__main__":
     run()
