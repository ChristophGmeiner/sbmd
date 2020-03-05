from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeDeltaSensor

from helpers import main02
from helpers import main03
from helpers import main04
from helpers import main05

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

dag = DAG("sbmd01_web_data_gathering",
          description="Gathers all necessary web data",
          default_args=default_args,
          schedule_interval="0 * * * *",
          max_active_runs=1)

create_stations_task = PythonOperator(
        task_id="01_create_stations_task",
        python_callable=main02,
        dag=dag)

conn_task_1 = PythonOperator(
        task_id="02_connection_task1",
        python_callable=main03(0),
        dag=dag)

conn_task_2 = PythonOperator(
        task_id="02_connection_task2",
        python_callable=main03(1),
        dag=dag)

conn_task_3 = PythonOperator(
        task_id="02_connection_task3",
        python_callable=main03(2),
        dag=dag)

conn_task_4 = PythonOperator(
        task_id="02_connection_task4",
        python_callable=main03(3),
        dag=dag)

conn_task_5 = PythonOperator(
        task_id="02_connection_task5",
        python_callable=main03(4),
        dag=dag)

conn_task_6 = PythonOperator(
        task_id="02_connection_task6",
        python_callable=main03(5),
        dag=dag)

conn_task_7 = PythonOperator(
        task_id="02_connection_task7",
        python_callable=main03(6),
        dag=dag)

conn_task_8 = PythonOperator(
        task_id="02_connection_task8",
        python_callable=main03(7),
        dag=dag)

conn_task_9 = PythonOperator(
        task_id="02_connection_task9",
        python_callable=main03(8),
        dag=dag)

conn_task_10 = PythonOperator(
        task_id="02_connection_task10",
        python_callable=main03(9),
        dag=dag)

gmap_task = PythonOperator(
        task_id="03_gmap_data",
        python_callable=main04,
        dag=dag)

weather_task = PythonOperator(
        task_id= "04_weather_data",
        python_callable=main05,
        dag=dag)

t1 = TimeDeltaSensor(
    task_id='wait1',
    delta=timedelta(minutes=5),
    dag=dag)

t2 = TimeDeltaSensor(
    task_id='wait2',
    delta=timedelta(minutes=10),
    dag=dag)

t3 = TimeDeltaSensor(
    task_id='wait3',    
    delta=timedelta(minutes=15),
    dag=dag)

t4 = TimeDeltaSensor(
    task_id='wait4',
    delta=timedelta(minutes=20),
    dag=dag)

t5 = TimeDeltaSensor(
    task_id='wait5',
    delta=timedelta(minutes=25),
    dag=dag)

t6 = TimeDeltaSensor(
    task_id='wait6',
    delta=timedelta(minutes=30),
    dag=dag)

t7 = TimeDeltaSensor(
    task_id='wait7',
    delta=timedelta(minutes=35),
    dag=dag)

t8 = TimeDeltaSensor(
    task_id='wait8',
    delta=timedelta(minutes=40),
    dag=dag)

t9 = TimeDeltaSensor(
    task_id='wait9',
    delta=timedelta(minutes=45),
    dag=dag)

create_stations_task >> conn_task_1
create_stations_task >> t1
t1 >> conn_task_2
create_stations_task >> t2
t2 >> conn_task_3
create_stations_task >> t3
t3 >> conn_task_4
create_stations_task >> t4
t4 >> conn_task_5
create_stations_task >> t5
t5 >> conn_task_6
create_stations_task >> t6
t6 >> conn_task_7
create_stations_task >> t7
t7 >> conn_task_8
create_stations_task >> t8
t8 >> conn_task_9
create_stations_task >> t9
t9 >> conn_task_10

conn_task_10 >> gmap_task
gmap_task >> weather_task