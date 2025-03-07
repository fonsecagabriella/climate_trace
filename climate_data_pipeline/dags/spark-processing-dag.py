from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.sensors.external_task import ExternalTaskSensor

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

# Use the same extraction year as the extraction DAG
current_year = datetime.now().year
EXTRACTION_YEAR = Variable.get("extraction_year", default_var=current_year)

# Define GCS and BigQuery configurations
# Use the same bucket and ds as your extraction DAG

GCS_BUCKET = 'zoomcamp-climate-trace' 
BQ_DATASET = ' zoomcamp_climate_raw'

# Define GCS paths for processed data
GCS_PROCESSED_BUCKET = f"gs://{GCS_BUCKET}/processed"
WORLD_BANK_PROCESSED = f"{GCS_PROCESSED_BUCKET}/world_bank/{EXTRACTION_YEAR}"
CLIMATE_TRACE_PROCESSED = f"{GCS_PROCESSED_BUCKET}/climate_trace/{EXTRACTION_YEAR}"
COMBINED_DATA_PATH = f"{GCS_PROCESSED_BUCKET}/combined/{EXTRACTION_YEAR}"

# Create the DAG
dag = DAG(
    'climate_data_spark_processing',
    default_args=default_args,
    description='Process climate and world bank data with Spark and load to BigQuery',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['climate_data', 'spark', 'bigquery'],
)

# Wait for the extraction DAG to complete
wait_for_extraction = ExternalTaskSensor(
    task_id='wait_for_extraction',
    external_dag_id='climate_data_pipeline',
    external_task_id=None,  # Wait for the entire DAG to complete
    timeout=600,  # 10 minutes timeout
    poke_interval=60,  # Check every minute
    dag=dag,
)

# Spark processing tasks
process_wb_data = PythonOperator(
    task_id='process_world_bank_data',
    python_callable=process_world_bank_data,
    op_kwargs={
        'input_path': f"gs://{GCS_BUCKET}/world_bank/world_bank_indicators_{EXTRACTION_YEAR}.csv",
        'output_path': WORLD_BANK_PROCESSED
    },
    dag=dag,
)

process_ct_data = PythonOperator(
    task_id='process_climate_trace_data',
    python_callable=process_climate_trace_data,
    op_kwargs={
        'input_path': f"gs://{GCS_BUCKET}/climate_trace/global_emissions_{EXTRACTION_YEAR}.csv",
        'output_path': CLIMATE_TRACE_PROCESSED
    },
    dag=dag,
)

combine_data = PythonOperator(
    task_id='combine_datasets',
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
            'tableId': f'world_bank_indicators_{EXTRACTION_YEAR}',
        },
        'externalDataConfiguration': {
            'sourceFormat': 'PARQUET',
            'sourceUris': [f"{WORLD_BANK_PROCESSED}/*.parquet"],
            'autodetect': True
        },
    },
    dag=dag,
)

create_ct_bq_table = BigQueryCreateExternalTableOperator(
    task_id='create_ct_bq_table',
    table_resource={
        'tableReference': {
            'projectId': '{{ var.value.gcp_project }}',
            'datasetId': BQ_DATASET,
            'tableId': f'climate_trace_emissions_{EXTRACTION_YEAR}',
        },
        'externalDataConfiguration': {
            'sourceFormat': 'PARQUET',
            'sourceUris': [f"{CLIMATE_TRACE_PROCESSED}/*.parquet"],
            'autodetect': True
        },
    },
    dag=dag,
)

create_combined_bq_table = BigQueryCreateExternalTableOperator(
    task_id='create_combined_bq_table',
    table_resource={
        'tableReference': {
            'projectId': '{{ var.value.gcp_project }}',
            'datasetId': BQ_DATASET,
            'tableId': f'combined_climate_economic_{EXTRACTION_YEAR}',
        },
        'externalDataConfiguration': {
            'sourceFormat': 'PARQUET',
            'sourceUris': [f"{COMBINED_DATA_PATH}/*.parquet"],
            'autodetect': True
        },
    },
    dag=dag,
)

# Define the dependencies
wait_for_extraction >> [process_wb_data, process_ct_data]
process_wb_data >> create_wb_bq_table
process_ct_data >> create_ct_bq_table
[process_wb_data, process_ct_data] >> combine_data >> create_combined_bq_table