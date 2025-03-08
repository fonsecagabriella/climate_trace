from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable, TaskInstance
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.trigger_rule import TriggerRule
# Import the Spark configuration
from spark_config import *

# Import the Spark processing functions
import sys
sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'scripts'))
from spark_processor import process_world_bank_data, process_climate_trace_data, combine_datasets

# Define default arguments
default_args = {
    'owner': 'zoomcamp',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Get processing year from a parameter
PROCESSING_YEAR = Variable.get("processing_year", default_var="2016")

# Define GCS and BigQuery configurations
GCS_BUCKET = 'zoomcamp-climate-trace'
BQ_DATASET = 'zoomcamp_climate_raw'

# Define GCS paths for processed data
GCS_PROCESSED_BUCKET = f"gs://{GCS_BUCKET}/processed"
WORLD_BANK_PROCESSED = f"{GCS_PROCESSED_BUCKET}/world_bank/{PROCESSING_YEAR}"
CLIMATE_TRACE_PROCESSED = f"{GCS_PROCESSED_BUCKET}/climate_trace/{PROCESSING_YEAR}"
COMBINED_DATA_PATH = f"{GCS_PROCESSED_BUCKET}/combined/{PROCESSING_YEAR}"

# Create the DAG
dag = DAG(
    'climate_data_historical_data_processing',
    default_args=default_args,
    description='Process existing climate and world bank data with Spark',
    schedule_interval=None,  # Run manually (no schedule)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['climate_data', 'spark', 'historical', 'bigquery'],
)

# Function to check if a file exists in GCS
def check_file_exists(bucket_name, object_name):
    """Check if a file exists in GCS"""
    gcs_hook = GCSHook()
    return gcs_hook.exists(bucket_name=bucket_name, object_name=object_name)

# Function to choose the appropriate processing path based on file existence
def choose_processing_path(**kwargs):
    """Choose which processing paths to follow based on file existence"""
    world_bank_file = f"world_bank/world_bank_indicators_{PROCESSING_YEAR}.csv"
    climate_trace_file = f"climate_trace/global_emissions_{PROCESSING_YEAR}.csv"

    paths = []

    # Check World Bank file
    if check_file_exists(GCS_BUCKET, world_bank_file):
        print(f"World Bank file exists: {world_bank_file}")
        paths.append('process_world_bank_data')
    else:
        print(f"World Bank file does not exist: {world_bank_file}")

    # Check Climate Trace file
    if check_file_exists(GCS_BUCKET, climate_trace_file):
        print(f"Climate Trace file exists: {climate_trace_file}")
        paths.append('process_climate_trace_data')
    else:
        print(f"Climate Trace file does not exist: {climate_trace_file}")

    # If both datasets exist, add the combine step
    if len(paths) == 2:
        paths.append('combine_data')

    return paths

# Branch task to check files and determine processing path
check_files = BranchPythonOperator(
    task_id='check_files',
    python_callable=choose_processing_path,
    dag=dag,
)

# Spark processing tasks
process_wb_data = PythonOperator(
    task_id='process_world_bank_data',
    python_callable=process_world_bank_data,
    op_kwargs={
        'input_path': f"gs://{GCS_BUCKET}/world_bank/world_bank_indicators_{PROCESSING_YEAR}.csv",
        'output_path': WORLD_BANK_PROCESSED
    },
    dag=dag,
)

process_ct_data = PythonOperator(
    task_id='process_climate_trace_data',
    python_callable=process_climate_trace_data,
    op_kwargs={
        'input_path': f"gs://{GCS_BUCKET}/climate_trace/global_emissions_{PROCESSING_YEAR}.csv",
        'output_path': CLIMATE_TRACE_PROCESSED
    },
    dag=dag,
)

combine_data = PythonOperator(
    task_id='combine_data',
    python_callable=combine_datasets,
    op_kwargs={
        'world_bank_path': WORLD_BANK_PROCESSED,
        'climate_trace_path': CLIMATE_TRACE_PROCESSED,
        'output_path': COMBINED_DATA_PATH
    },
    dag=dag,
)

# Create external tables in BigQuery pointing to the processed Parquet files
create_wb_bq_table = BigQueryCreateExternalTableOperator(
    task_id='create_wb_bq_table',
    table_resource={
        'tableReference': {
            'projectId': '{{ var.value.gcp_project }}',
            'datasetId': BQ_DATASET,
            'tableId': f'world_bank_indicators_{PROCESSING_YEAR}',
        },
        'externalDataConfiguration': {
            'sourceFormat': 'PARQUET',
            'sourceUris': [f"{WORLD_BANK_PROCESSED}/*.parquet"],
            'autodetect': True
        },
    },
    trigger_rule=TriggerRule.ONE_SUCCESS,  # Run even if previous task skipped
    dag=dag,
)

create_ct_bq_table = BigQueryCreateExternalTableOperator(
    task_id='create_ct_bq_table',
    table_resource={
        'tableReference': {
            'projectId': '{{ var.value.gcp_project }}',
            'datasetId': BQ_DATASET,
            'tableId': f'climate_trace_emissions_{PROCESSING_YEAR}',
        },
        'externalDataConfiguration': {
            'sourceFormat': 'PARQUET',
            'sourceUris': [f"{CLIMATE_TRACE_PROCESSED}/*.parquet"],
            'autodetect': True
        },
    },
    trigger_rule=TriggerRule.ONE_SUCCESS,  # Run even if previous task skipped
    dag=dag,
)

create_combined_bq_table = BigQueryCreateExternalTableOperator(
    task_id='create_combined_bq_table',
    table_resource={
        'tableReference': {
            'projectId': '{{ var.value.gcp_project }}',
            'datasetId': BQ_DATASET,
            'tableId': f'combined_climate_economic_{PROCESSING_YEAR}',
        },
        'externalDataConfiguration': {
            'sourceFormat': 'PARQUET',
            'sourceUris': [f"{COMBINED_DATA_PATH}/*.parquet"],
            'autodetect': True
        },
    },
    trigger_rule=TriggerRule.ONE_SUCCESS,  # Run even if previous task skipped
    dag=dag,
)

# Define the dependencies
check_files >> [process_wb_data, process_ct_data, combine_data]
process_wb_data >> create_wb_bq_table
process_ct_data >> create_ct_bq_table
combine_data >> create_combined_bq_table