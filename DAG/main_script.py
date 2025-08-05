import pandas as pd
import numpy as np
import requests
import time
import pytz
from datetime import datetime
from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import os

load_dotenv()
db_password = os.getenv('DATABASE_PASSWORD')
db_user = os.getenv('DATABASE_USER')
prt = os.getenv('PORT')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 7, 13)
}

dag = DAG(
    dag_id='ParseBybitKline',
    default_args=default_args,
    schedule_interval='5,55 * * * *',
    description='ETL Kline с Bybit',
    catchup=False,
    max_active_runs=1,
    tags=["MYJOB"]
)


def create_df(df):
    df = pd.DataFrame(df)
    df.columns = ["open_time", "open", "high", "low", "close", "volume", "turnover"]
    df = df.astype(np.float64)
    moscow = pytz.timezone("Europe/Moscow")
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(moscow)
    df['open_time'] = df['open_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    load_date = datetime.now(tz=moscow)
    load_date = load_date.strftime('%Y-%m-%d %H:%M:%S')
    df['load_date'] = load_date
    return df

def get_data(ticker):
    #получаем данные: тикер, таймфрейм, дата начала, дата конца данных
    moscow = pytz.timezone("Europe/Moscow")
    now = datetime.now(tz=moscow)
    now = now.strftime('%Y-%m-%d %H')
    now = datetime.strptime(now, '%Y-%m-%d %H')
    now = int(now.timestamp()) * 1000
    start_time = now - 86400000
    end_time = now

    resp = request(ticker, "60", start_time, end_time)
    resp = resp.json()
    resp = resp['result']['list']
    df = create_df(resp)

    return df

def request(symbol, interval, start_time, end_time):
    url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&start={start_time}&end={end_time}"
    response = requests.get(url)
    if response.status_code == 200:
        return response
    else:
        raise Exception(f"API вернул ошибку: {response.status_code}")

def insert_to_db(ticker):
    client = Client('host.docker.internal',         #или 127.0.0.1
                    user=db_user,
                    password=db_password,
                    port=prt,
                    verify=False,
                    database='default',
                    settings={"numpy_columns": False, 'use_numpy': True},
                    compression=False)
    df = get_data(ticker)
    print(df)
    client.insert_dataframe(f'INSERT INTO default.{ticker} VALUES', df)


def main():
    list_tickers = ["BTCUSDT", "ETHUSDT"]
    for i in list_tickers:
        insert_to_db(i)


task1 = PythonOperator(
    task_id='ParseBybitKline_task', python_callable=main, dag=dag)