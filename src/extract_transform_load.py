from airflow import DAG
from airflow.operators.python import PythonOperator
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Variáveis de ambiente do destino
DB_HOST_DEST= os.getenv('DB_HOST_DEST')
DB_PORT_DEST= os.getenv('DB_PORT_DEST')
DB_NAME_DEST= os.getenv('DB_NAME_DEST')
DB_USER_DEST= os.getenv('DB_USER_DEST')
DB_PASS_DEST= os.getenv('DB_PASS_DEST')
DB_SCHEMA_DEST= os.getenv('DB_SCHEMA_DEST')

# Variáveis de ambiente da fonte
DB_HOST_SOURCE= os.getenv('DB_HOST_SOURCE')
DB_PORT_SOURCE= os.getenv('DB_PORT_SOURCE')
DB_NAME_SOURCE= os.getenv('DB_NAME_SOURCE')
DB_USER_SOURCE= os.getenv('DB_USER_SOURCE')
DB_PASS_SOURCE= os.getenv('DB_PASS_SOURCE')
DB_SCHEMA_SOURCE= os.getenv('DB_SCHEMA_SOURCE')

# Outras variáveis de ambiente
API_URL= os.getenv('API_URL')
PARQUET_FILE_PATH= os.getenv('PARQUET_FILE_PATH')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_teste_1',
    default_args=default_args,
    description='A simple DAG',
    schedule=timedelta(days=1),
)


def extract_sales_data():
    conn = psycopg2.connect(
        dbname=DB_NAME_SOURCE, 
        user=DB_USER_SOURCE, 
        password=DB_PASS_SOURCE, 
        host=DB_HOST_SOURCE, 
        port=DB_PORT_SOURCE
    )
    query = "SELECT * FROM public.venda;"
    df_sales = pd.read_sql(query, conn)
    conn.close()
    return df_sales

def load_sales_data(ti):
    df_sales = ti.xcom_pull(task_ids='extract_sales_data')
    engine = create_engine(f'postgresql://{DB_USER_DEST}:{DB_PASS_DEST}@{DB_HOST_DEST}:{DB_PORT_DEST}/{DB_NAME_DEST}')
    df_sales.to_sql('venda', engine, if_exists='replace', index=False, schema=DB_SCHEMA_DEST)

with dag:
    extract_task = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales_data
)


    load_task = PythonOperator(
        task_id='load_sales_data',
        python_callable=load_sales_data
        
    )

    extract_task >> load_task
