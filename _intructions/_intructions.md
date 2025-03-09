# instructions

## 1.0 Create a clean Anaconda Environment

- Create a new conda environment
`conda create -n airflow_env python=3.8`

- Activate the environment
`conda activate airflow_env`

## 2.0 Create the project structure

- Create a new project directory
`mkdir -p ~/climate_data_pipeline`
`cd ~/climate_data_pipeline`

- Create necessary subdirectories
`mkdir -p dags logs plugins config scripts data`
`mkdir -p data/world_bank data/climate_trace`

## 3.0 Install Airflow with constraints
- Install airflow
`pip install "apache-airflow==2.7.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.8.txt"`

- Install Google provider
`pip install apache-airflow-providers-google`
`pip install google-cloud-storage dlt`

## 4.0 Copy Python Scripts
- Copy your [extraction scripts](../extractors/) to the scripts folder
`cp /path/to/world_bank_extractor.py scripts/`

`cp /path/to/climate_trace_extractor.py scripts/`

## 5.0 Initialise Airflow Database
- Export Airflow home
`export AIRFLOW_HOME=~/climate_data_project`

- Initialize the database
`airflow db init`

- Create an admin user
```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

## 6.0 Create your DAG file

Create a [dag file](./../climate_data_pipeline/dags/climate-data-dag.py).

## 7.0 Start Aiflow Services

Run the commands below in two different terminals:

```bash
cd ~/climate_data_pipeline
conda activate airflow_env
export AIRFLOW_HOME=/Users/gabi/codes/climate_trace/climate_data_pipeline
airflow webserver --port 8080
```

Terminal 02:

```bash
cd ~/climate_data_project
conda activate airflow_env
export AIRFLOW_HOME=/Users/gabi/codes/climate_trace/climate_data_pipeline
airflow scheduler
```

## 8.0 Access the Airflow UI

Once both services are running, access the Airflow UI at:
http://localhost:8080

Login with:

Username: admin
Password: admin

In admin, add the connection to GSC.

<img src="./images/airflow_gcs.png" width="80%">

## 9.0 Create the DAG and data_extractor
Create a [data_extractor](./../climate_data_pipeline/scripts/data_extractor.py), which combines the climate and word bank extractors.
With this, you can create the [dag - climate-data-dag](../climate_data_pipeline/dags/climate-data-dag.py).


<img src="./images/dag_climate_data.png" width="80%">

## 10.0 Run the DAG

In the DAG above, you will need to specify the year to collect the data.
This can be done in different ways.

**Option 01: Via the Airflow UI**
- Go to Admin >> Variables
- Set Key as "extraction_year" and Value as your desired year (e.g., "2016")

**Option 02: triggering the DAG via API**
````bash
curl -X POST \
  http://localhost:8080/api/v1/dags/climate_data_pipeline/dagRuns \
  -H 'Content-Type: application/json' \
  --user "username:password" \
  -d '{"conf": {"extraction_year": 2016}}'
````

You will need to run the dag for 2015 to 2024 as a backfill.

## 11.0 Batch processing
Now we will use PySpark to process the data from the data lake (Google buckets) to a data warehouse (BigQuery).
With the virtual environment active, download Pyspark:

`pip install pyspark`

Create a new file in the scripts directory: [spark_processor.py](../climate_data_pipeline/scripts/spark_processor.py)
Create a new dag [spark-processing-dag.py](../climate_data_pipeline/dags/spark-processing-dag.py)

By creating another DAG, we implement a modula approach which gives us flexibility to run either DAG independently if needed.

<img src="./images/airflow-dag-bigquery.png" width="80%">

**Additional considerations for production deployment**
For a production environment, you may want to consider:

- Using a dedicated Spark cluster instead of running Spark within Airflow
- Configuring Spark memory and executor settings based on your data volume
- Adding error handling and retries for the Spark jobs
- Setting up monitoring for the Spark jobs


This approach will let you process any historical data that's already in your bucket without having to re-download it through your extraction pipeline. It's perfect for:

Reprocessing data with an updated Spark transformation
Processing multiple years of historical data in sequence
Testing your Spark transformations on existing data

### Python

The workflow is as structured:

You process the World Bank data and Climate Trace data separately
You combine these datasets into a single dataset
You create three BigQuery tables: one for each original dataset and one for the combined data

<img src="./images/python-dag.png">

Process Tasks (in pink):

process_world_bank_data (PythonOperator)
process_climate_trace_data (PythonOperator)


Combine Task (in pink):

combine_data (PythonOperator) - This is your single data combination task


BigQuery Table Creation Tasks (in blue):

create_world_bank_bq_table (BigQueryCreateExternalTableOperator)
create_climate_trace_bq_table (BigQueryCreateExternalTableOperator)
create_combined_bq_table (BigQueryCreateExternalTableOperator)



