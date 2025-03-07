from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# Import the dlt extractor
import sys
sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'scripts'))
from data_extractor import run_world_bank_pipeline, run_climate_trace_pipeline


# Define default arguments
default_args = {
    'owner': 'zoomcamp',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define paths
DATA_DIR = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
WORLD_BANK_DIR = os.path.join(DATA_DIR, 'world_bank')
CLIMATE_TRACE_DIR = os.path.join(DATA_DIR, 'climate_trace')

# Define GCS bucket and BigQuery dataset
GCS_BUCKET = 'zoomcamp-climate-trace' 
BQ_DATASET = ' zoomcamp_climate_raw'
EXTRACTION_YEAR = 2016  # Update this with your desired year

# Create the DAG
dag = DAG(
    'climate_data_pipeline',
    default_args=default_args,
    description='Extract climate and world bank data with dlt, load to GCS and BigQuery',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['climate_data', 'dlt'],
)

# Function to extract World Bank data using dlt
def extract_world_bank_data(year, **kwargs):
    return run_world_bank_pipeline(year, WORLD_BANK_DIR)

# Function to extract Climate Trace data using dlt
def extract_climate_trace_data(year, **kwargs):
    return run_climate_trace_pipeline(year, CLIMATE_TRACE_DIR)

# Define tasks
extract_world_bank = PythonOperator(
    task_id='extract_world_bank_data',
    python_callable=extract_world_bank_data,
    op_kwargs={'year': EXTRACTION_YEAR},
    dag=dag,
)

extract_climate_trace = PythonOperator(
    task_id='extract_climate_trace_data',
    python_callable=extract_climate_trace_data,
    op_kwargs={'year': EXTRACTION_YEAR},
    dag=dag,
)

# Task to upload World Bank data to GCS
upload_world_bank_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_world_bank_to_gcs',
    src=f"{WORLD_BANK_DIR}/world_bank_indicators_{EXTRACTION_YEAR}.csv",
    dst=f'world_bank/world_bank_indicators_{EXTRACTION_YEAR}.csv',
    bucket=GCS_BUCKET,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Task to upload Climate Trace data to GCS
upload_climate_trace_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_climate_trace_to_gcs',
    src=f"{CLIMATE_TRACE_DIR}/global_emissions_{EXTRACTION_YEAR}.csv",
    dst=f'climate_trace/global_emissions_{EXTRACTION_YEAR}.csv',
    bucket=GCS_BUCKET,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Define task dependencies
extract_world_bank >> upload_world_bank_to_gcs
extract_climate_trace >> upload_climate_trace_to_gcs