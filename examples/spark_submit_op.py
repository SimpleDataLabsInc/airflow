from airflow import DAG

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils import timezone, timedelta
from airflow.operators.bash_operator import BashOperator
import os

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
srcDir = os.getcwd() + '/dags/repo/examples/hello_2.11-1.0.jar'
args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE
}
dag = DAG('sparkjob', default_args=args, schedule_interval='@monthly',
                                                            dagrun_timeout=timedelta(minutes=60))


operator = SparkSubmitOperator(
    task_id='spark_submit_job1',
    dag=dag,
    conn_id='spark_default',
    java_class='hello',
    application=srcDir,
    verbose=False,
)

