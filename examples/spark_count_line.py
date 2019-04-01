import logging

from airflow import DAG
from airflow.operators import PythonOperator

from datetime import datetime

from airflow.utils.email import send_email

def notify_email(contextDict, **kwargs):
    """Send custom email alerts."""

    # email title.
    title = "Airflow alert: {task_name} Failed".format(**contextDict)

    # email contents
    body = """
    Hi Everyone, <br>
    <br>
    There's been an error in the {task_name} job.<br>
    <br>
    Forever yours,<br>
    Airflow bot <br>
    """.format(**contextDict)

    send_email('you_email@address.com', title, body)

args = {
    'owner': 'airflow'
    , 'start_date': datetime(2019, 4, 1)
    , 'provide_context': True
    , 'email': "kajari@simpledatalabs.com"
    , 'email_on_failure': True
    , 'email_on_retry': False
    , 'retries': 1
    , 'retry_delay': timedelta(minutes=2),
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
   , on_failure_callback=notify_email
)
