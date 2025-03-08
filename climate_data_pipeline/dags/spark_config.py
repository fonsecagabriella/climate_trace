import os
from airflow.models import Variable

# Set Java and Spark environment variables
os.environ['JAVA_HOME'] = '/path/to/your/java'  # Update this to your Java installation path
os.environ['SPARK_HOME'] = '/path/to/spark'     # Update this to your Spark installation path
os.environ['PYSPARK_PYTHON'] = '/usr/local/anaconda3/envs/airflow_env/bin/python'  # Use your Airflow Python
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/anaconda3/envs/airflow_env/bin/python'  # Use your Airflow Python

# Add Spark's bin directory to PATH
os.environ['PATH'] = os.environ['SPARK_HOME'] + '/bin:' + os.environ['PATH']