from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import psycopg2
import json

def fetch_and_store_belgium_holidays():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="CurrencyExchange",
        user="airflow",
        password="airflow"
    )

    # Create a cursor object
    cursor = conn.cursor()

    # Send a GET request to the API and retrieve the data
    url = "https://openholidaysapi.org/PublicHolidays?countryIsoCode=BE&languageIsoCode=EN&validFrom=2023-01-01&validTo=2023-12-31"
    response = requests.get(url)
    data = response.json()

    # Iterate over the data and insert it into the database
    for holiday in data:
        id = holiday["id"]
        event_date = holiday["startDate"]
        holiday_description = holiday["name"][0]["text"]

        # Execute the INSERT statement with ON CONFLICT clause to perform an upsert operation
        cursor.execute(
            "INSERT INTO be_holidays (id, event_date, holiday_description) VALUES (%s, %s, %s) ON CONFLICT (id) DO UPDATE SET event_date = excluded.event_date, holiday_description = excluded.holiday_description",
            (id, event_date, holiday_description)
        )

    # Commit the changes and close the cursor and connection
    conn.commit()
    cursor.close()
    conn.close()

def fetch_and_store_usd_base_exchange_rate():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="CurrencyExchange",
        user="airflow",
        password="airflow"
    )

    # Create a cursor object
    cursor = conn.cursor()

    # Send a GET request to the API and retrieve the data
    url = "https://open.er-api.com/v6/latest/USD"
    response = requests.get(url)
    data = response.json()
    data_json = json.dumps(data)

    cursor.execute(
        "INSERT INTO source_data_USD (upload_dt, api_data) VALUES (NOW(), %s)",
        (data_json,)
    )

    # Step 7: Commit the changes and close the cursor and connection
    conn.commit()
    cursor.close()
    conn.close()

def fetch_and_store_eur_base_exchange_rate():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="CurrencyExchange",
        user="airflow",
        password="airflow"
    )

    # Create a cursor object
    cursor = conn.cursor()

    cursor.execute(
        "SELECT api_data from source_data_usd"
    )
    row = cursor.fetchone()

    rates = row[0]["rates"]
    eurorate = row[0]["rates"]["EUR"]

    for currency, rate in rates.items():
        eurobaserate = round(rate / eurorate, 4)

        cursor.execute(
            "INSERT INTO source_data_eur (upload_dt, currency, rate) VALUES (NOW(), %s, %s)",
            (currency, eurobaserate,)
        )

    # Step 7: Commit the changes and close the cursor and connection
    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('ExchangeRate_DAG', default_args=default_args, schedule_interval='0 0 * * *')

fetch_belgium_holidays_data_task = PythonOperator(
    task_id='fetch_data_belgium_holidays',
    python_callable=fetch_and_store_belgium_holidays,
    dag=dag
)
fetch_usd_base_exchange_rate_data_task = PythonOperator(
    task_id='fetch_data_usd_base_exchange_rate',
    python_callable=fetch_and_store_usd_base_exchange_rate,
    dag=dag
)
fetch_eur_base_exchange_rate_data_task = PythonOperator(
    task_id='fetch_data_eur_base_exchange_rate',
    python_callable=fetch_and_store_eur_base_exchange_rate,
    dag=dag
)

fetch_belgium_holidays_data_task>>fetch_usd_base_exchange_rate_data_task>>fetch_eur_base_exchange_rate_data_task
