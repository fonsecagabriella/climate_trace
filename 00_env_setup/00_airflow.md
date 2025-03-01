# Setting Up Airflow on macOS with Anaconda and Homebrew

This guide will walk you through setting up Apache Airflow on your macOS system using Anaconda and Homebrew.

## Prerequisites

* macOS
* Homebrew (if not already installed)
* Anaconda

## Steps

**1. Install Homebrew (If Not Already Installed)**

* Open your terminal and run:

    ```bash
    /bin/bash -c "$(curl -fsSL [https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh](https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh))"
    ```

* Follow the on-screen instructions.

**2. Install Python (Recommended: Python 3.8 or higher)**

* Ensure you have a suitable Python version. Anaconda usually provides this. You can check your Python version with:

    ```bash
    python --version
    ```

* If you need to install or manage Python versions, Anaconda is a great tool.

**3. Create a Virtual Environment (Recommended)**

* It's best practice to create a virtual environment to isolate Airflow's dependencies.

    ```zsh
    conda create -n airflow_env python=3.9  # Or your preferred version
    conda activate airflow_env
    ```

**4. Install Airflow**

* Airflow requires a database backend. For simplicity, we'll start with SQLite.
* Install Airflow and its dependencies:

    ```zsh
  pip install 'apache-airflow[gcp,google_auth,cncf.kubernetes,pandas,apache.spark,dlt,dbt-bigquery]==2.8.2' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.2/constraints-3.9.txt"straints-2.8.2/constraints-3.9.txt](https://raw.githubusercontent.com/apache/airflow/constraints-2.8.2/constraints-3.9.txt)"
    ```

    * **Explanation:**
        * `apache-airflow`: Installs the core Airflow package.
        * `[gcp,google_auth,cncf.kubernetes,pandas,apache.spark,dlt,dbt-bigquery]`: Installs extra dependencies for GCP, Google authentication, kubernetes, pandas, spark, dlt and dbt.
        * `--constraint`: Ensures dependency compatibility.

* **Important:** Pay close attention to the version. I have selected 2.8.2, as it is a stable recent version. Always check the airflow documentation for the most recent version.

**5. Initialize the Airflow Database**

* Initialize the Airflow database and create the necessary tables:

    ```bash
    airflow db init
    ```

**6. Set the AIRFLOW_HOME Environment Variable**

* Airflow needs a directory to store its configuration and logs. By default it is set to ~/airflow.
* If you wish to change it, set it with:

    ```bash
    export AIRFLOW_HOME=~/your_airflow_home
    ```

* Make sure to create the directory if it doesn't exist.
* Add the export command to your `~/.bash_profile` or `~/.zshrc` file to make it permanent.
    * For example, using `nano`:

        ```bash
        nano ~/.zshrc
        ```

    * Add the line, save, and exit. Then run:

        ```bash
        source ~/.zshrc
        ```

**7. Start the Airflow Web Server and Scheduler**

* Start the Airflow web server:

    ```bash
    airflow webserver -p 8080
    ```

    * This will run the web UI on `http://localhost:8080`.

* Open a new terminal window (or tab) and activate your virtual environment:

    ```bash
    conda activate airflow_env
    ```

* Start the Airflow scheduler:

    ```bash
    airflow scheduler
    ```

    * The scheduler monitors your DAGs (Directed Acyclic Graphs) and triggers tasks.

**8. Access the Airflow Web UI**

* Open your web browser and go to `http://localhost:8080`.
* You should see the Airflow web UI.

## Important Notes

* **Security:** For production environments, you'll need to configure Airflow's security settings.
* **Database:** For production, use a more robust database like PostgreSQL or MySQL.
* **Executor:** The default executor is `SequentialExecutor`, which is fine for development. For parallel execution, use `CeleryExecutor` or `KubernetesExecutor`.
* **DAGs:** Airflow DAGs are Python scripts that define your workflows. You'll create these to orchestrate your data pipeline.

```
airflow users create \
    --username gabi \
    --firstname Gabi \
    --lastname User \
    --role Admin \
    --email gabi@example.com \
    --password airflow-psw
```



airflow connections add google_cloud_default \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{"project_id": "peppy-plateau-447914-j6", "key_path": "/Users/gabi/codes/climate_trace/keys/keys.json"}'




