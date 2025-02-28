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


# Running steps

- install airflow
- create VM
- activate VM and run the two airflow services
- create a bucket in GCS `myzoomcamp_climate_trace`
- create a service account with storage admin permissions