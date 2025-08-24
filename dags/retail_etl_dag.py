from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
import json
import pandas as pd
import os
from dotenv import load_dotenv
from helper import download_json_file, validate_json_structure, extract_orders_data

load_dotenv()

default_args = {
    'owner': 'liliya',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 24),
    'email': [os.getenv('my_email')],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'retail_orders_to_bigquery',
    default_args=default_args,
    description='Process retail orders JSON and load to BigQuery',
    schedule_interval=timedelta(hours=11),
    catchup=False,
    tags=['retail', 'bigquery', 'json']
)