from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nyc_taxi_medallion_pipeline',
    default_args=default_args,
    description='NYC Yellow Taxi Medallion Pipeline v2.0.0',
    schedule=None,  # Run on demand
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

# Jar path inside the containers where /spark-app is mounted
jar_path = '/spark-app/target/scala-2.12/nyc-taxi-analytics-assembly-2.0.0.jar'

ingest_task = SparkSubmitOperator(
    task_id='ingest_bronze',
    application=jar_path,
    java_class='jobs.IngestJob',
    conn_id='spark_default',
    driver_memory='1G',
    executor_memory='2G',
    executor_cores=2,
    conf={
        'spark.hadoop.fs.permissions.umask-mode': '000',
    },
    dag=dag,
)

clean_task = SparkSubmitOperator(
    task_id='clean_silver',
    application=jar_path,
    java_class='jobs.CleanJob',
    conn_id='spark_default',
    driver_memory='1G',
    executor_memory='2G',
    executor_cores=2,
    conf={
        'spark.hadoop.fs.permissions.umask-mode': '000',
    },
    env_vars={
        'TARGET_SAMPLE_SIZE': os.environ.get('TARGET_SAMPLE_SIZE', '100000'),
    },
    dag=dag,
)

analytics_task = SparkSubmitOperator(
    task_id='analytics_gold',
    application=jar_path,
    java_class='jobs.AnalyticsJob',
    conn_id='spark_default',
    driver_memory='1G',
    executor_memory='2G',
    executor_cores=2,
    conf={
        'spark.hadoop.fs.permissions.umask-mode': '000',
    },
    env_vars={
        'CMS_EPSILON': os.environ.get('CMS_EPSILON', '0.0001'),
        'CMS_CONFIDENCE': os.environ.get('CMS_CONFIDENCE', '0.95'),
        'CMS_SEED': os.environ.get('CMS_SEED', '1'),
    },
    dag=dag,
)

validate_task = SparkSubmitOperator(
    task_id='validate_quality_gate',
    application=jar_path,
    java_class='jobs.ValidateJob',
    conn_id='spark_default',
    driver_memory='1G',
    executor_memory='2G',
    executor_cores=2,
    conf={
        'spark.hadoop.fs.permissions.umask-mode': '000',
    },
    dag=dag,
)

ingest_task >> clean_task >> analytics_task >> validate_task
