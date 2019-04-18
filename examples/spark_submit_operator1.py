from airflow import DAG

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils import timezone


DEFAULT_DATE = timezone.datetime(2017, 1, 1)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE
}
dag = DAG('test_dag_id', default_args=args)

_config = {
    'conf': {
        'parquet.compression': 'SNAPPY'
    },
    'files': 'hive-site.xml',
    'py_files': 'sample_library.py',
    'driver_classpath': 'parquet.jar',
    'jars': 'parquet.jar',
    'packages': 'com.databricks:spark-avro_2.11:3.2.0',
    'exclude_packages': 'org.bad.dependency:1.0.0',
    'repositories': 'http://myrepo.org',
    'total_executor_cores': 4,
    'executor_cores': 4,
    'executor_memory': '22g',
    'keytab': 'privileged_user.keytab',
    'principal': 'user/spark@airflow.org',
    'name': '{{ task_instance.task_id }}',
    'num_executors': 10,
    'verbose': True,
    'application': 'test_application.py',
    'driver_memory': '3g',
    'java_class': 'com.foo.bar.AppMain',
    'application_args': [
        '-f', 'foo',
        '--bar', 'bar',
        '--start', '{{ macros.ds_add(ds, -1)}}',
        '--end', '{{ ds }}',
        '--with-spaces', 'args should keep embdedded spaces',
    ]
}

operator = SparkSubmitOperator(
    task_id='spark_submit_job',
    dag=dag,
    **_config
)