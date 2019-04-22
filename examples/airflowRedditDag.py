from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

s3Bucket = 'sdl-file-store'
s3Key = 'user_artists.csv'
print(os.getcwd())
redditFile = '/tmp/artist.csv'
#can alternatively wrap methods in functions and use PythonOperator instead of BashOperator
srcDir = os.getcwd() + '/dags/repo/examples/src/python/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 10, 14, 16, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    's3Reddit', default_args=default_args, schedule_interval=timedelta(seconds=45))

downloadData= BashOperator(
    task_id='download-data',
    bash_command='python ' + srcDir + 's3-reddit.py ' + s3Bucket + ' ' + s3Key + ' ' + redditFile,
    dag=dag)

numUniqueAuthors = BashOperator(
    task_id='Unique-authors',
    bash_command='python ' + srcDir + 'numUniqueAuthors.py ' + redditFile,
    dag=dag)
numUniqueAuthors.set_upstream(downloadData)

averageUpvotes = BashOperator(
	task_id='average-upvotes',
	bash_command='python ' + srcDir + 'averageUpvote.py ' + redditFile,
	dag=dag)

averageUpvotes.set_upstream(downloadData)

