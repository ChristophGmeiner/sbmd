from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import time

default_args = {
        "owner": "Christoph Gmeiner",
        "start_date": datetime(2020, 3, 7, 11, 40),
        "retries": 1,
        "retry_delay": timedelta(seconds=60),
        "email": "christoph.gmeiner@gmail.com",
        "email_on_retry": True,
        "email_on_success": True,
        "depends_on_past": False,
        "trigger_rule": "all_done",
        "sla": timedelta(minutes=59)
        }

def wait(n=240):
    time.sleep(n)

dag = DAG("sbmd0b1_rev_web_data_gathering",
          description="Gathers all reversed necessary web data",
          default_args=default_args,
          schedule_interval="40 * * * *",
          max_active_runs=1,
          catchup=False)

conn_task_1c = BashOperator(
        task_id="02c_connection_task1",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03c_reversed.py 0",
        dag=dag)

conn_task_2c = BashOperator(
        task_id="02c_connection_task2",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03c_reversed.py 1",
        dag=dag)

conn_task_3c = BashOperator(
        task_id="02c_connection_task3",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03c_reversed.py 2",
        dag=dag)

conn_task_4c = BashOperator(
        task_id="02c_connection_task4",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03c_reversed.py 3",
        dag=dag)

conn_task_5c = BashOperator(
        task_id="02c_connection_task5",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03c_reversed.py 4",
        dag=dag)

conn_task_6c = BashOperator(
        task_id="02c_connection_task6",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03c_reversed.py 5",
        dag=dag)

conn_task_7c = BashOperator(
        task_id="02c_connection_task7",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03c_reversed.py 6",
        dag=dag)

conn_task_8c = BashOperator(
        task_id="02c_connection_task8",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03c_reversed.py 7",
        dag=dag)

conn_task_9c = BashOperator(
        task_id="02c_connection_task9",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03c_reversed.py 8",
        dag=dag)

conn_task_10c = BashOperator(
        task_id="02c_connection_task10",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03c_reversed.py 9",
        dag=dag)

weather_taskc = BashOperator(
        task_id= "04c_weather_data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc05_weather.py",
        dag=dag)

t1c = PythonOperator(
    task_id='wait1c',
    python_callable=wait,
    dag=dag)

t2c = PythonOperator(
    task_id='wait2c',
    python_callable=wait,
    dag=dag)

t3c = PythonOperator(
    task_id='wait3c',
    python_callable=wait,
    dag=dag)

t4c = PythonOperator(
    task_id='wait4c',
    python_callable=wait,
    dag=dag)

t5c = PythonOperator(
    task_id='wait5c',
    python_callable=wait,
    dag=dag)

t6c = PythonOperator(
    task_id='wait6c',
    python_callable=wait,
    dag=dag)

t7c = PythonOperator(
    task_id='wait7c',
    python_callable=wait,
    dag=dag)

t8c = PythonOperator(
    task_id='wait8c',
    python_callable=wait,
    dag=dag)

t9c = PythonOperator(
    task_id='wait9c',
    python_callable=wait,
    dag=dag)

conn_task_1c >> t1c

t1c >> conn_task_2c
t1c >> t2c

t2c >> conn_task_3c
t2c >> t3c

t3c >> conn_task_4c
t3c >> t4c

t4c >> conn_task_5c
t4c >> t5c

t5c >> conn_task_6c
t5c >> t6c

t6c >> conn_task_7c
t6c >> t7c

t7c >> conn_task_8c
t7c >> t8c

t8c >> conn_task_9c
t8c >> t9c

t9c >> conn_task_10c
t9c >> weather_taskc
