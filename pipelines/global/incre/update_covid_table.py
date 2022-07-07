import requests
import json
from datetime import date, timedelta
import psycopg2

# global_total_cases_yesterday = \
#     "https://disease.sh/v3/covid-19/all?yesterday=yesterday"
covid_for_all_countries_yesterday = \
    "https://disease.sh/v3/covid-19/countries?yesterday=yesterday"
# vaccination_of_all_countries_nearest_period_of_time = \
#     "https://disease.sh/v3/covid-19/vaccine/coverage/countries?lastdays=1&fullData=true"


def get_key_of_a_date(a_date) -> int:
    year = a_date.year
    month = a_date.month
    day = a_date.day
    key = -1
    print(f"{year}-{month}-{day}")
    try:
        conn = psycopg2.connect(
        host='localhost', 
        port='5433', 
        database='luandb',
        user='luan', 
        password='luan_password'
        )
        cur = conn.cursor()
        cur.execute(f"SELECT _key FROM date_dim WHERE year = {year} AND month = {month} AND day = {day};"
        )
        row = cur.fetchone()
        key = row[0]
        print(f"Got key: {key}")
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return key

def update_covid():
    data = requests.get(covid_for_all_countries_yesterday)
    parse_json = json.loads(data.text)
    yesterday = date.today() - timedelta(days=1)
    yesterday_key = get_key_of_a_date(yesterday)
    covid_data = []
    for country in parse_json:
        iso3 = country['countryInfo']['iso3']
        total_cases = country['cases']
        new_cases = country['todayCases']
        total_deaths = country['deaths']
        new_deaths = country['todayDeaths']
        covid_data.append(
            ( 
                iso3,
                yesterday_key, 
                new_cases, 
                total_cases, 
                new_deaths, 
                total_deaths
            )
        )
    # print(covid_data)
    conn = None
    try:
        conn = psycopg2.connect(
            host='localhost', 
            port='5433', 
            database='luandb',
            user='luan', 
            password='luan_password'
        )
        cur = conn.cursor()
        for record in covid_data:
            # print(record)
            cur.execute(
                """
                    INSERT INTO covid (
                        iso_code, date_key, 
                        new_cases, total_cases, 
                        new_deaths, total_deaths
                    )
                    VALUES (%s, %s, %s, %s, %s, %s); 
                """, 
                record
            )
        conn.commit()
        print("Commit successfully")
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Postgresql Error: {error}")
    finally:
        if conn is not None:
            conn.close()