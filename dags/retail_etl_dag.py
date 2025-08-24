from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import json
import pandas as pd
import os
from dotenv import load_dotenv
from helper import download_json_file, validate_json_structure, extract_orders_data, load_to_bigquery, send_notification

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

# Task 1: Download/check for JSON file
download_task = PythonOperator(
    task_id='download_json',
    python_callable=download_json_file,
    dag=dag
)

# Task 2: Validate JSON structure
validate_task = PythonOperator(
    task_id='validate_json',
    python_callable=validate_json_structure,
    dag=dag
)

# Task 3: Extract order data
extract_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders_data,
    dag=dag
)

# Task 4: Load to BigQuery
load_bigquery_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag
)

# Task 5: Send notification email
email_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    dag=dag
)

# Define task dependencies
download_task >> validate_task >> extract_task >> load_bigquery_task >> email_task