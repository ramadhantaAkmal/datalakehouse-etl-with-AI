import sys
import os

external_script_path = '/opt/airflow/lib'
sys.path.append(external_script_path)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime
from pendulum import duration
from main import main
from load_to_minio import upload_file_to_minio

with DAG(
    dag_id='google_jobs_scrape',
    start_date = datetime(2025,11,9),
    schedule= '@weekly',
    catchup=True,
    description='weekly scrape jobs',
    tags=['scrape','weekly'],
    default_args={"retries":1},
    dagrun_timeout=duration(minutes=20)

)as dag:
    scrape_task = PythonOperator(
        task_id='scrape_google_jobs',
        python_callable=main,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id = 'load_to_minio',
        python_callable = upload_file_to_minio,
        provide_context=True
    )
    
    run_n8n_task = SimpleHttpOperator(
        task_id='execute_ai_agent',
        http_conn_id='workflow_conn',
        endpoint='/webhook/8606c214-29e4-4dc8-a7ed-6aac1b6f1371',
        method='GET',
        headers={
            'Content-Type': 'application/json',
            'X-Source': 'Airflow'
        },
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )
    
    scrape_task >> load_task >> run_n8n_task