from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

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
DBT_PROJECT_DIR = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'dbt_climate_data/climate_transforms')

# Get processing year from Airflow variable
PROCESSING_YEAR = Variable.get("processing_year", default_var="2016")

# Create the DAG
dag = DAG(
    'climate_data_dbt_transformations',
    default_args=default_args,
    description='Transform combined climate data with dbt',
    schedule_interval=None,  # Run manually (coordinate with your upstream DAGs)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['climate_data', 'dbt', 'bigquery'],
)

# Wait for the historical data processing to complete
wait_for_spark_processing = ExternalTaskSensor(
    task_id='wait_for_spark_processing',
    external_dag_id='climate_data_spark_historical_data_processing',
    external_task_id='create_combined_bq_table',
    timeout=600,  # 10 minutes timeout
    poke_interval=60,  # Check every minute
    dag=dag,
)

# Task to run dbt
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --vars "{{\'year\': \'{PROCESSING_YEAR}\'}}"',
    dag=dag,
)

# Task to test dbt
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt test',
    dag=dag,
)

# Task to generate documentation
dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt docs generate',
    dag=dag,
)

# Define dependencies
wait_for_spark_processing >> dbt_run >> dbt_test >> dbt_docs_generate