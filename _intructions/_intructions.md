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




