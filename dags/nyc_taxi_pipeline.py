import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# This line explicitly tells Python to look in the current folder (dags) for your scripts
sys.path.append(os.path.dirname(__file__))

# Import the functions from your separate script files
# This assumes a folder structure of: dags/scripts/ingest.py, etc.
from dags.scripts.nyc_taxi_pipeline_script.ingest_taxi_data import ingest_taxi_data 
from dags.scripts.nyc_taxi_pipeline_script.clean_taxi_data import clean_taxi_data
from dags.scripts.nyc_taxi_pipeline_script.transform_taxi_data import transform_taxi_data
from dags.scripts.nyc_taxi_pipeline_script.load_taxi_data import load_taxi_model

# Standard Airflow default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    # params={
    #     "transaction_date": Param("2024-01-01", type="string", description="Date to process (YYYY-MM-DD)")
    # },
    tags=['etl', 'datalake', 'local']
) as dag:

    # Task 1: Ingest
    ingest_task = PythonOperator(
        task_id='ingest_taxi_data',
        python_callable=ingest_taxi_data,
    )

    # Task 2: Clean
    clean_task = PythonOperator(
        task_id='clean_taxi_data',
        python_callable=clean_taxi_data,
    )

    # Task 3: Transform
    transform_task = PythonOperator(
        task_id='transform_taxi_data',
        python_callable=transform_taxi_data,
    )

    # Task 4: Load to MySQL
    load_task = PythonOperator(
        task_id='load_taxi_model',
        python_callable=load_taxi_model,
    )

    # Define the execution order (Dependencies)
    ingest_task >> clean_task >> transform_task >> load_task