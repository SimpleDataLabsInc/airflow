from airflow import DAG

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta


args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 7, 31)
}
dag = DAG('spark_example_new', default_args=args, schedule_interval='@monthly',
          dagrun_timeout=timedelta(minutes=60)
)

operator = SparkSubmitOperator(
    task_id='spark_submit_job',
    application='/usr/local/airflow/dags/repo/examples/spark_count_line.py',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='1',
    name='airflow-spark',
    verbose=False,
    driver_memory='1g',
    conf={'master':'spark://local:7077'},
    dag=dag,
)
