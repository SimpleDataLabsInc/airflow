import logging

from airflow import DAG
from airflow.operators import PythonOperator

from datetime import datetime, timedelta

from airflow.utils.email import send_email


#from airflow.operators.slack_operator import SlackAPIPostOperator

def notify_email(contextDict, **kwargs):
    """Send custom email alerts."""

    # email title.
    title = "Airflow alert: {task_id} Failed".format(**contextDict)

    # email contents
    body = """
    Hi Everyone, <br>
    <br>
    There's been an error in the {task_id} job.<br>
    <br>
    Forever yours,<br>
    Airflow bot <br>
    """.format(**contextDict)

    send_email('kajari@simpledatalabs.com', title, body)

args = {
    'owner': 'airflow'
    , 'start_date': datetime(2019, 4, 1)
    , 'provide_context': True
    , 'email': "kajari@simpledatalabs.com"
    , 'email_on_failure': True
    , 'email_on_retry': False
    #, 'retries': 1
    #, 'retry_delay': timedelta(minutes=20),
}

dag = DAG(
    'spark_count_lines'
    , start_date = datetime(2019, 4, 1)
    , default_args = args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(minutes=6)
)

#def slack_failed_task(context):
 #   failed_alert = SlackAPIPostOperator(
  #      task_id='slack_failed',
   #     channel="#airflow",
    #    token="",
     #   text = ':red_circle: Task Failed',
      #  username = 'kajari',)
    #return failed_alert.execute(context=context)

def run_spark(**kwargs):
    import pyspark
    sc = pyspark.SparkContext("local")
    words = sc.parallelize(["scala","java","hadoop","spark","akka"])
    count = words.count()
    print(count)
    df = sc.textFile('file:////usr/local/airflow/dags/kube.py')
    print('Number of lines in people.txt = {0}'.format(df.count()))
    sc.stop()

t_main = PythonOperator(
    task_id = 'call_spark'
    , dag = dag
    , python_callable = run_spark,
    # on_failure_callback=notify_email,
    keep_failed_pod=False,
    provide_context=True
)
