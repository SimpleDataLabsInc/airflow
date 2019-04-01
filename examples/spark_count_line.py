import logging

from airflow import DAG
from airflow.operators import PythonOperator

from datetime import datetime

args = {
    'owner': 'airflow'
    , 'start_date': datetime(2019, 4, 1)
    , 'provide_context': True
}

dag = DAG(
    'spark_count_lines'
    , start_date = datetime(2019, 4, 1)
    , schedule_interval = '@hourly'
    , default_args = args
)

def run_spark(**kwargs):
    import pyspark
    sc = pyspark.SparkContext()
    df = sc.textFile('file:////usr/local/airflow/dags/test.py')
    logging.info('Number of lines in people.txt = {0}'.format(df.count()))
    sc.stop()

t_main = PythonOperator(
    task_id = 'call_spark'
    , dag = dag
    , python_callable = run_spark
)
