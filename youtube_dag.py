import pandas as pd
from dateutil import parser
from IPython.display import display,JSON

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from youtube_etl import youtube_data_analysis

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'youtube_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_Youtube_etl',
    python_callable=youtube_data_analysis,
    dag=dag, 
)

run_etl