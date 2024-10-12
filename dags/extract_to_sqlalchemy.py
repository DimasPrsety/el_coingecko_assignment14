import os
import argparse
import requests
import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Folder untuk staging
staging_folder = os.path.expanduser('~/Documents/DE7/Assignment14/airflow/data/')
os.makedirs(staging_folder, exist_ok=True)

# Fungsi untuk mengambil data dari CoinGecko API
def extract_from_coingecko_to_staging():
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd'
    response = requests.get(url)
    data = response.json()

    # Konversi JSON ke DataFrame
    df = pd.DataFrame(data)

    # Simpan data ke CSV di staging folder
    file_path = os.path.join(staging_folder, 'crypto_data.csv')
    df.to_csv(file_path, index=False)
    print(f'Data dari CoinGecko disimpan ke {file_path}')

def extract_from_local_csv():
    file_path = os.path.join(staging_folder, 'crypto_data.csv')
    df = pd.read_csv(file_path)

    print(f'Data dari file CSV local disimpan ke {file_path}')
    return df

# Fungsi untuk load data dari staging ke SQLite
def load_data_to_sqlite():
    # Path database
    db_path = os.path.join(staging_folder, 'crypto_database.db')
    engine = create_engine(f'sqlite:///{db_path}')

    # Baca file CSV dari staging folder
    file_path = os.path.join(staging_folder, 'crypto_data.csv')
    df = pd.read_csv(file_path)

    # Simpan ke SQLite
    df.to_sql('crypto_data', con=engine, if_exists='replace', index=False)
    print(f'Data dari {file_path} telah dimuat ke dalam SQLite database di {db_path}')

# fungsi utk memilih jenis ekstraksi data 
def choose_extract_task(source_type):
    if source_type == 'api':
        return 'extract_from_coingecko_task'
    elif source_type == 'csv':
        return 'extract_from_csv_task'
    else:
        return 'end_task'

# BranchOperator
with DAG(
    'crypto_etl_dag',
    schedule_interval = None,
    start_date = days_ago(1),
    catchup = False,
) as dag : 
    
    start_task = EmptyOperator(task_id = 'start_task')
    end_task   = EmptyOperator(task_id = 'end_task')

    choose_extract_task = BranchPythonOperator(
        task_id = 'choose_extract_task',
        python_callable = choose_extract_task,
        op_kwargs = {'source_type' : 'api'},
        provide_context = True,
    )

    extract_from_coingecko_task = PythonOperator(
        task_id = 'extract_from_coingecko_task',
        python_callable = extract_from_coingecko_to_staging,
    )

    extract_from_csv_task = PythonOperator(
        task_id = 'extract_from_csv_task',
        python_callable = extract_from_local_csv,
    )

    load_to_sqlite_task = PythonOperator(
        task_id = 'load_to_sqlite_task',
        python_callable = load_data_to_sqlite,
        provide_context = True,
        trigger_rule = TriggerRule.ONE_SUCCESS
    )

# Pengaturan task
start_task >> choose_extract_task
choose_extract_task >> [extract_from_coingecko_task, extract_from_csv_task]
extract_from_coingecko_task >> load_to_sqlite_task >> end_task
extract_from_csv_task >> load_to_sqlite_task >> end_task