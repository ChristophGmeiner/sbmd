from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
        "owner": "Christoph Gmeiner",
        "start_date": datetime(2020, 2, 23),
        "retries": 2,
        "retry_delay": timedelta(seconds=300),
        "email": "christoph.gmeiner@gmail.com",
        "email_on_retry": True,
        "email_on_success": True,
        "depends_on_past": True
        }

dag = DAG("sbmd02rawdatatoDB",
          description="Creates DBs and loads raw data from S3 to Postgres DB",
          default_args=default_args,
          schedule_interval="40 7 * * 0",
          max_active_runs=1)

create_DB_task = BashOperator(
        task_id="01_create_DB_task",
        bash_command=" python3 /home/ubuntu/sbmd/zzCreateDB.py",
        dag=dag)

#create_tables_task = BashOperator(
#        task_id="02_Create_Stage_Tables",
#        bash_command="python3 /home/ec2-user/sbmd/zz08_CreateEmptyDBTables.py",
#        dag=dag)

load_train_data = BashOperator(
        task_id="03a_Load_train_Data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03b_TransferDB.py",
        dag=dag)

load_gmap_data = BashOperator(
        task_id="03b_LoadgmapDB_Data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc04b_TransferDB.py",
        dag=dag)

load_weather_data = BashOperator(
        task_id="03c_LoadweatherDB_Data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc05b_TransferDB.py",
        dag=dag)

insert_live_data = BashOperator(
        task_id="04_load_liveDB_data",
        bash_command="python3 /home/ubuntu/sbmd/zz09_InsertLiveTables.py",
        trigger_rule="all_done",
        dag=dag)

archive_del_db = BashOperator(
        task_id="05b_Archive_and_Delete_DB",
        bash_command="python3 /home/ubuntu/sbmd/zzDelDB.py",
        trigger_rule="all_done",
        dag=dag)

#create_DB_task >> create_tables_task
create_DB_task >> load_train_data
create_DB_task >> load_gmap_data
create_DB_task >> load_weather_data

load_train_data >> insert_live_data
load_gmap_data >> insert_live_data
load_weather_data >> insert_live_data

insert_live_data >> archive_del_db
