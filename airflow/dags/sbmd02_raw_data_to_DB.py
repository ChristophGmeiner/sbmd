from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators import BashOperator

default_args = {
        "owner": "Christoph Gmeiner",
        "start_date": datetime.now(),
        "catchup": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=60),
        "email": "christoph.gmeiner@gmail.com",
        "email_on_retry": False,
        "depends_on_past": False,
        "trigger_rule": TriggerRule.ALL_DONE
        }

dag = DAG("sbmd02_raw data_to_DB",
          description="Creates DBs and loads raw data from S3 to Postgres DB",
          args=default_args,
          scheduleinterval="* * * * 1")

create_DB_task = BashOperator(
        task_id="01_create_DB_task",
        bash_command=" python3 /home/ec2-user/sbmd/zzCreateDB.py",
        dag=dag)

create_tables_task = BashOperator(
        task_id="02_Create_Stage_Tables",
        bash_command="python3 /home/ec2-user/sbmd/zz08_CreateEmptyDBTables.py",
        dag=dag)

load_train_data = BashOperator(
        task_id="03a_Load_train_Data",
        bash_command="python3 /home/ec2-user/sbmd/sbahnmuc03b_TransferDB.py",
        dag=dag)

load_gmap_data = BashOperator(
        task_id="03b_LoadgmapDB_Data",
        bash_command="python3 /home/ec2-user/sbmd/sbahnmuc04b_TransferDB.py",
        dag=dag)

load_weather_data = BashOperator(
        task_id="03c_LoadweatherDB_Data",
        bash_command="python3 /home/ec2-user/sbmd/sbahnmuc05b_TransferDB.py",
        dag=dag)

insert_live_data = BashOperator(
        task_id="04_load_liveDB_data",
        bash_command="python3 /home/ec2-user/sbmd/zz09_InsertLiveTables.py",
        dag=dag)

archive_del_db = BashOperator(
        task_id="05b_Archive_and_Delete_DB",
        bash_command="python3 /home/ec2-user/sbmd/zzDelDB.py",
        dag=dag)

create_DB_task >> create_tables_task
create_tables_task >> load_train_data
create_tables_task >> load_gmap_data
create_tables_task >> load_weather_data

load_train_data >> insert_live_data
load_gmap_data >> insert_live_data
load_weather_data >> insert_live_data

insert_live_data >> archive_del_db
