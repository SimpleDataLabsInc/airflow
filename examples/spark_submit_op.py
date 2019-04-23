from airflow import DAG

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils import timezone
from airflow.operators.bash_operator import BashOperator
import os
from datetime import datetime, timedelta

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
srcDir = os.getcwd() + '/dags/repo/examples/hello_2.11-1.0.jar'
args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE
}
dag = DAG('sparkjob', default_args=args ,schedule_interval='@monthly',
          dagrun_timeout=timedelta(minutes=60))

spark_task = BashOperator(
    task_id='spark_java_bash',
    bash_command='spark-submit --class {{ params.class }} {{ params.jar }}',
    params={'class': 'hello', 'jar': srcDir},
    dag=dag
)

_config ={'application': srcDir,
          'master' : 'local',
          'deploy-mode' : 'cluster',
          'executor_cores': 1,
          'EXECUTORS_MEM': '1G'
          }

operator = SparkSubmitOperator(
    task_id='spark_submit_op_job',
    dag=dag,
    java_class='hello',
    **_config
)

operator >> spark_task