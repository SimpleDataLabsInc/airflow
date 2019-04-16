from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

start = DummyOperator(
    task_id='start',
    dag=dag)

tmp_slack_test_dag = PostgresOperator(pool=redshift_pool,
                                      task_id='tmp_slack_test_sql',
                                      postgres_conn_id=redshift_conn_id,
                                      sql="""sql/tmp_.sql""",
                                      parameters=None,
                                      autocommit=True,
                                      dag=dag
                                      )

success_dummy = DummyOperator(
    task_id='success_dummy',
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

alert_success_task = PythonOperator(
    task_id='alert_success',
    python_callable=lambda: send_slack(
        senderRole='airflow',
        receiverSubscribe='bot',
        level='info',
        text='success'+str(dt),
        X_CAG_AUTH='AG_CONSUMER_TOKEN access-key=500000000000'
    ),
    #depends_on_past=True,
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag)

start >> tmp_slack_test_dag >> success_dummy >> alert_success_task >> end