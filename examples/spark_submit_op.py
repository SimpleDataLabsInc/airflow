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

operator = SparkSubmitOperator(
    task_id='spark_submit_op_job',
    dag=dag,
    conn_id='spark_default',
    java_class='hello',
    jars=srcDir,
    driver_class_path = srcDir,
    verbose=False,
    driver_memory = '1g',
    application_args= [
                            '-f', 'foo',
                            '--bar', 'bar',
                            '--start', '{{ macros.ds_add(ds, -1)}}',
                            '--end', '{{ ds }}',
                            '--with-spaces', 'args should keep embdedded spaces',
                        ],
)

operator >> spark_task