# Pipeline Flow Structure

Airflow: Orchestrates the entire workflow (scheduling, monitoring, and managing tasks).
dlt: Extracts and loads the Climate Trace data into Google Cloud Storage (GCS) as raw files.
GCS (Datalake): Stores the raw Climate Trace data.
Airflow + dlt: Moves data from GCS to BigQuery in a staging table.
Spark: Processes and transforms large-scale data inside BigQuery.
dbt: Performs further transformations and modeling for analytics (e.g., cleaning, aggregating).
BigQuery: Acts as your data warehouse to store both raw and transformed data.
Dashboard: Visualizes the processed Climate Trace data using a BI tool (e.g., Looker Studio).

# Flow diagram
Climate Trace Data → (Airflow + dlt) → GCS (Datalake) → (Airflow + dlt) → BigQuery (Staging)
    → Spark (Transformation) → BigQuery (Analytics) → Dashboard (Visualization)

https://www.climatetrace.org/inventory?year_from=2015&year_to=2024&gas=co2e100
https://climatetrace.org/data
https://api.climatetrace.org/v6/swagger/index.html#tag/sources

# Running steps

## ✅ 1. Innitial installs
- install airflow
- create VM
- activate VM and run the two airflow services
- create a bucket in GCS `myzoomcamp_climate_trace`
- create a service account with `storage admin` permissions
- download the service key and set as an env variable so dlt can have access to it
`export GOOGLE_APPLICATION_CREDENTIALS="keys/keys.json"`

 python -c "from google.cloud import bigquery; client = bigquery.Client(); print(list(client.list_datasets()))"

export GOOGLE_APPLICATION_CREDENTIALS=~/airflow/keys/keys-climate.json


## ✅ 2. Configure Airflow to Access GCS
Go to the Airflow UI: http://localhost:8080

Navigate to Admin → Connections.

Click + Add Connection and configure:

Connection ID: gcs_default
Connection Type: Google Cloud
Keyfile Path: /absolute/path/to/dlt_gcs_key.json
Project ID: Your Google Cloud project ID
Click Save.

✅ 3. Create Your First DAG
Navigate to your Airflow DAGs folder:
bash
Copy
Edit
cd ~/airflow/dags
Create a new DAG file:
bash
Copy
Edit
touch climate_trace_to_gcs.py
Add this code to the file:
python
Copy
Edit
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import dlt
import requests

# Function to extract and load Climate Trace data
def extract_and_load():
    # Download dataset (replace with the actual Climate Trace URL)
    url = "https://climatetrace.org/data/sample.csv"
    response = requests.get(url)
    
    with open("/tmp/climate_trace.csv", "wb") as f:
        f.write(response.content)
    
    # Load to GCS
    pipeline = dlt.pipeline(pipeline_name="climate_trace_pipeline", destination="gcs")
    pipeline.run(["/tmp/climate_trace.csv"], table_name="climate_raw")

# Define DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id="climate_trace_to_gcs",
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:

    extract_and_load_task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load
    )

    extract_and_load_task
✅ 4. Restart Airflow
In separate terminals, restart your Airflow services:

Stop the running processes:
Press CTRL+C in each terminal.

Restart them:

bash
Copy
Edit
airflow scheduler
bash
Copy
Edit
airflow webserver --port 8080
✅ 5. Trigger the DAG
Go to Airflow UI.
Find climate_trace_to_gcs.
Toggle the DAG "ON".
Click ▶️ to run it manually.
✅ If successful, you will see the Climate Trace data in your GCS bucket.

👉 Let me know if you want to:

Handle more complex Climate Trace data.
Set up the next stage: Move data from GCS to BigQuery.




✅ 1. Check and Update Airflow Configuration
Open your Airflow configuration file (airflow.cfg):
bash
Copy
Edit
nano ~/airflow/airflow.cfg
Search for the setting allow_tests (use CTRL+W to search).
If it's not present, add this under the [webserver] section:

ini
Copy
Edit
[webserver]
allow_tests = True
If it's already there but set to False, change it to:

ini
Copy
Edit
allow_tests = True
Save and exit:
Press CTRL+X, then Y, and hit Enter.



Navigate to the Default AIRFLOW_HOME:

In your terminal, run: cd ~/airflow
Then, run ls to see what files and folders are present.
Create the dags Folder (If It Doesn't Exist):

If you don't see a dags folder, create it: mkdir dags
Set AIRFLOW_HOME (Optional but Recommended):

While Airflow works without AIRFLOW_HOME being set, it's good practice to set it explicitly.
Run: export AIRFLOW_HOME=~/airflow
To make this permanent, add this line to your ~/.zshrc or ~/.bash_profile file:
Open the file with a text editor (e.g., nano ~/.zshrc).
Add the line export AIRFLOW_HOME=~/airflow.
Save the file and exit.
Then, run source ~/.zshrc to apply the changes.