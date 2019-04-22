from airflow import DAG

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta



default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 29),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('android_addons', default_args=default_args, schedule_interval='@daily')

android_addons = SparkSubmitOperator(
    task_id="android_addons",
    job_name="Update android addons",
    execution_timeout=timedelta(hours=4),
    instance_count=5,
    owner="frank@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
    env={"date": "{{ ds_nodash }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/android-addons.ipynb",
    output_visibility="public",
    dag=dag)
