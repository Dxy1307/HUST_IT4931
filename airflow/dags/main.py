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
    dag_id='batch_processing',
    default_args=default_args,
    schedule_interval='@monthly',  # Run the DAG once a month
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

    # Task 3: Sync data from Cassandra to Elasticsearch
    cassandra_to_es_sync_task = SparkSubmitOperator(
        task_id='cassandra_to_elasticsearch_sync',
        application='/opt/airflow/code/cassandra_es_sync.py',
        conn_id='spark_default',
        packages='com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0',
        conf={
            'spark.cassandra.connection.host': 'cassandra',
            'spark.cassandra.connection.port': '9042',
            'spark.es.nodes': 'elasticsearch',
            'spark.es.port': '9200',
            'spark.es.nodes.wan.only': 'true'
        },
        verbose=True,
    )


    # Set task dependencies
    extract_task >> transform_load_task >> cassandra_to_es_sync_task