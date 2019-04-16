from airflow import DAG

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta


args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 7, 31)
}
dag = DAG('spark_example_new', default_args=args, schedule_interval = '@hourly'
)

operator = SparkSubmitOperator(
    task_id='spark_submit_job',
    conn_id='spark_default',
    java_class='org.apache.spark.examples.SparkPi',
    application='/path/to/spark-examples_2.11-2.3.1.jar',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='1',
    name='airflow-spark-example',
    verbose=False,
    driver_memory='1g',
    application_args=["1000"],
    conf={'master':'spark://<HOSTNAME>:7077'},
    dag=dag,
)