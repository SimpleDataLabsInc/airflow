from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes import client, config

config.load_incluster_config()
v1 = client.CoreV1Api()
print("Listing pods with their IPs: kajari1")
ret = v1.list_pod_for_all_namespaces(watch=False)
for i in ret.items:
    print("%s\t%s\t%s" %
          (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
#config.list_kube_config_contexts()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kubernetes_hello_world', default_args=default_args, schedule_interval='@monthly',
    dagrun_timeout=timedelta(minutes=60))


start = DummyOperator(task_id='start', dag=dag)

passing = KubernetesPodOperator(namespace='airflow',
                          image="python:3.6",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="passing-test",
                          task_id="passing-task",
                          get_logs=True,
                          dag=dag
                          )

failing = KubernetesPodOperator(namespace='airflow',
                          image="ubuntu:16.04",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="fail",
                          task_id="failing-task",
                          get_logs=True,
                          dag=dag
                          )

end = DummyOperator(task_id='end', dag=dag)


passing.set_upstream(start)
failing.set_upstream(start)
passing.set_downstream(end)
failing.set_downstream(end)
