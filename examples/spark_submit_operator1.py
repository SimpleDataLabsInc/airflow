from airflow import DAG

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils import timezone
from airflow.operators import PythonOperator
import os

DEFAULT_DATE = timezone.datetime(2019, 4, 17)

srcDir = os.getcwd() + '/dags/repo/examples/hello_2.11-1.0.jar'

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE
}
dag = DAG('test_dag_id', default_args=args)

def get_some_value(**kwargs):
    some_value = 10
    return some_value

task1 = PythonOperator(task_id='run_task_1',
                       python_callable=get_some_value,
                       provide_context=True,
                       dag=dag)

task2 = SparkSubmitOperator(
    task_id='run_sparkSubmit_job',
    conn_id='spark_default',
    java_class='hello',
    application=srcDir,
    name='airflow-spark-job',
    verbose=True,
    application_args=["{{ti.xcom_pull(task_ids='run_task_1')}}"],  
    conf={'master':'local'},
    dag=dag,
)
task1 >> task2
