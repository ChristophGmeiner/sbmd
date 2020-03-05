from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import time

default_args = {
        "owner": "Christoph Gmeiner",
        "start_date": datetime(2020, 3, 4, 22, 0),
        "retries": 1,
        "retry_delay": timedelta(seconds=60),
        "email": "christoph.gmeiner@gmail.com",
        "email_on_retry": True,
        "email_on_success": True,
        "depends_on_past": True,
        "trigger_rule": "all_done"
        }

def wait(n=300):
    time.sleep(n)

dag = DAG("sbmd01_web_data_gathering",
          description="Gathers all necessary web data",
          default_args=default_args,
          schedule_interval="0 3-23 * * *",
          max_active_runs=1)

create_stations_task = BashOperator(
        task_id="01_create_stations_task",
        bash_command=" python3 /home/ubuntu/sbmd/sbahnmuc02.py",
        dag=dag)

conn_task_1 = BashOperator(
        task_id="02_connection_task1",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03.py 0",
        dag=dag)

conn_task_2 = BashOperator(
        task_id="02_connection_task2",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03.py 1",
        dag=dag)

conn_task_3 = BashOperator(
        task_id="02_connection_task3",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03.py 2",
        dag=dag)

conn_task_4 = BashOperator(
        task_id="02_connection_task4",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03.py 3",
        dag=dag)

conn_task_5 = BashOperator(
        task_id="02_connection_task5",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03.py 4",
        dag=dag)

conn_task_6 = BashOperator(
        task_id="02_connection_task6",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03.py 5",
        dag=dag)

conn_task_7 = BashOperator(
        task_id="02_connection_task7",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03.py 6",
        dag=dag)

conn_task_8 = BashOperator(
        task_id="02_connection_task8",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03.py 7",
        dag=dag)

conn_task_9 = BashOperator(
        task_id="02_connection_task9",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03.py 8",
        dag=dag)

conn_task_10 = BashOperator(
        task_id="02_connection_task10",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03.py 9",
        dag=dag)

gmap_task = BashOperator(
        task_id="03_gmap_data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc04_gmaps.py",
        dag=dag)

weather_task = BashOperator(
        task_id= "04_weather_data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc05_weather.py",
        dag=dag)

t1 = PythonOperator(
    task_id='wait1',
    python_callable=wait,
    dag=dag)

t2 = PythonOperator(
    task_id='wait2',
    python_callable=wait,
    dag=dag)

t3 = PythonOperator(
    task_id='wait3',
    python_callable=wait,
    dag=dag)

t4 = PythonOperator(
    task_id='wait4',
    python_callable=wait,
    dag=dag)

t5 = PythonOperator(
    task_id='wait5',
    python_callable=wait,
    dag=dag)

t6 = PythonOperator(
    task_id='wait6',
    python_callable=wait,
    dag=dag)

t7 = PythonOperator(
    task_id='wait7',
    python_callable=wait,
    dag=dag)

t8 = PythonOperator(
    task_id='wait8',
    python_callable=wait,
    dag=dag)

t9 = PythonOperator(
    task_id='wait9',
    python_callable=wait,
    dag=dag)

create_stations_task >> conn_task_1
create_stations_task >> t1

t1 >> conn_task_2
t1 >> t2

t2 >> conn_task_3
t2 >> t3

t3 >> conn_task_4
t3 >> t4

t4 >> conn_task_5
t4 >> t5

t5 >> conn_task_6
t5 >> t6

t6 >> conn_task_7
t6 >> t7

t7 >> conn_task_8
t7 >> t8

t8 >> conn_task_9
t8 >> t9

t9 >> conn_task_10
t9 >> gmap_task

gmap_task >> weather_task