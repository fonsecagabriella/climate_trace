from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# Import the Python processing functions
import sys
sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'scripts'))
from python_processor import process_world_bank_data, process_climate_trace_data, combine_datasets

# Set up GCP credentials in your DAG
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gabi/codes/climate_trace/climate_data_pipeline/config/peppy.json'

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
PROCESSING_YEAR = Variable.get("processing_year", default_var="2015")

# Define GCS bucket and paths
GCS_BUCKET = "zoomcamp-climate-trace"  # Update this to your bucket name
GCS_PATH = f"gs://{GCS_BUCKET}"

# Define GCS paths for data
GCS_WORLD_BANK = f"{GCS_PATH}/world_bank/world_bank_indicators_{PROCESSING_YEAR}.csv"
GCS_CLIMATE_TRACE = f"{GCS_PATH}/climate_trace/global_emissions_{PROCESSING_YEAR}.csv"
GCS_WORLD_BANK_PROCESSED = f"{GCS_PATH}/processed/world_bank"
GCS_CLIMATE_TRACE_PROCESSED = f"{GCS_PATH}/processed/climate_trace"
GCS_COMBINED_DATA = f"{GCS_PATH}/processed/combined"

# Define BigQuery dataset
BQ_DATASET = 'zoomcamp_climate_raw'
BQ_PROJECT = Variable.get("gcp_project")  # Make sure this variable exists in Airflow

# Create the DAG
dag = DAG(
    'climate_data_gcs_historical_data_processing',
    default_args=default_args,
    description='Process climate and world bank data with Python using GCS',
    schedule_interval=None,  # Run manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['climate_data', 'python', 'gcs', 'historical'],
)

# Processing tasks
process_wb_data = PythonOperator(
    task_id='process_world_bank_data',
    python_callable=process_world_bank_data,
    op_kwargs={
        'input_path': GCS_WORLD_BANK,
        'output_path': GCS_WORLD_BANK_PROCESSED
    },
    dag=dag,
)

process_ct_data = PythonOperator(
    task_id='process_climate_trace_data',
    python_callable=process_climate_trace_data,
    op_kwargs={
        'input_path': GCS_CLIMATE_TRACE,
        'output_path': GCS_CLIMATE_TRACE_PROCESSED
    },
    dag=dag,
)

combine_data = PythonOperator(
    task_id='combine_data',
    python_callable=combine_datasets,
    op_kwargs={
        'world_bank_path': GCS_WORLD_BANK_PROCESSED,
        'climate_trace_path': GCS_CLIMATE_TRACE_PROCESSED,
        'output_path': GCS_COMBINED_DATA
    },
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,  # Run even if previous tasks fail
)

# Create BigQuery external tables
create_combined_bq_table = BigQueryCreateExternalTableOperator(
    task_id='create_combined_bq_table',
    table_resource={
        'tableReference': {
            'projectId': BQ_PROJECT,
            'datasetId': BQ_DATASET,
            'tableId': 'combined_climate_economic',
        },
        'externalDataConfiguration': {
            'sourceFormat': 'PARQUET',
            'sourceUris': [f"{GCS_COMBINED_DATA}/combined_data.parquet"],
            'autodetect': True
        },
    },
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Define dependencies
[process_wb_data, process_ct_data] >> combine_data >> create_combined_bq_table