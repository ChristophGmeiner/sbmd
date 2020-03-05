from airflow import DAG
from datetime import datetime, timedelta
import logging
from airflow.operators.python_operator import PythonOperator


default_args = {
        "owner": "Christoph Gmeiner",
        "start_date": datetime(2020, 3, 4, 21, 0),
        "retries": 2,
        "retry_delay": timedelta(seconds=100),
        "email": "christoph.gmeiner@gmail.com",
        "email_on_retry": True,
        "email_on_success": True,
        "catchup": False,
        "depends_on_past": True,
        "trigger_rule": "all_done"
        }

def echo(n="W"):
    print(n)
    logging.info(n)

dag = DAG("test01",
          description="test",
          default_args=default_args,
          schedule_interval="0 * * * *",
          max_active_runs=1)

task1 = PythonOperator(
        task_id="task1",
        python_callable=echo,
        dag=dag)

task2 = PythonOperator(
        task_id="task2",
        python_callable=echo,
        dag=dag)

task1 >> task2


