from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 10),
}

# Define the DAG
with DAG(
    dag_id='spark_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually
    catchup=False,
    description='A Spark ETL pipeline DAG',
) as dag:

    # Task 1: Extract and clean data
    extract_task = SparkSubmitOperator(
        task_id='extract_and_clean_data',
        application='/opt/airflow/code/extract.py',
        conn_id='spark_default',
        verbose=False,
    )

    # Task 2: Transform and load data
    transform_load_task = SparkSubmitOperator(
        task_id='transform_and_load_data',
        application='/opt/airflow/code/transform_load.py',
        conn_id='spark_default',
        packages='com.datastax.spark:spark-cassandra-connector_2.12:3.2.0',
        py_files='/opt/airflow/code/transformations.py,/opt/airflow/code/user_defined_function.py',
        verbose=True,
    )

    # Set task dependencies
    extract_task >> transform_load_task