from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from helpers import CreateTables
from helpers import InsertTables

default_args = {
        "owner": "Christoph Gmeiner",
        "start_date": datetime(2020, 3, 9, 0, 1),
        "retries": 2,
        "retry_delay": timedelta(seconds=100),
        "email": "christoph.gmeiner@gmail.com",
        "email_on_retry": True,
        "email_on_success": True,
        "email_on_failure": False,
        "depends_on_past": False
        }

dag = DAG("sbmd02rawdatatoDB",
          description="Creates DBs and loads raw data from S3 to Postgres DB",
          default_args=default_args,
          schedule_interval="1 0 * * 1",
          max_active_runs=1,
          catchup=False)

create_DB_task = BashOperator(
        task_id="02_create_DB_task",
        bash_command=" python3 /home/ubuntu/sbmd/zzCreateDB.py",
        dag=dag)

drop_stage_tables = PostgresOperator(
        task_id="03_Drop_Old_Stage_Tables",
        sql=CreateTables.drop_table1 + CreateTables.drop_table2 + \
            CreateTables.drop_table3,
        postgres_conn_id="postgres_aws_capstone",
        autocommit=True,
        dag=dag)

load_train_data = BashOperator(
        task_id="01a_Load_train_Data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03b_TransferDB.py",
        dag=dag)

load_gmap_data = BashOperator(
        task_id="01b_LoadgmapDB_Data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc04b_TransferDB.py",
        dag=dag)

load_weather_data = BashOperator(
        task_id="01c_LoadweatherDB_Data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc05b_TransferDB.py",
        dag=dag)

transfer_train_data = S3ToRedshiftTransfer(
        task_id="04a_Transfer_train_CSV",
        schema="sbmd",
        table="t_db01_stagings",
        s3_bucket="sbmd1db2",
        s3_key="CSV2/" + str(date.today()) + "_DB_DF.csv",
        redshift_conn_id="postgres_aws_capstone",
        aws_conn_id="aws_credentials_s3",
        autocommit=True,
        trigger_rule="all_done",
        dag=dag
        )

transfer_gmap_data = S3ToRedshiftTransfer(
        task_id="04b_Transfer_gmap_CSV",
        schema="sbmd",
        table="t_gmap01_stagings",
        s3_bucket="sbmd2gmap3",
        s3_key="CSV2/" + str(date.today()) + "_Gmap_DF.csv",
        redshift_conn_id="postgres_aws_capstone",
        aws_conn_id="aws_credentials_s3",
        autocommit=True,
        trigger_rule="all_done",
        dag=dag
        )

transfer_weather_data = S3ToRedshiftTransfer(
        task_id="04c_Transfer_weather_CSV",
        schema="sbmd",
        table="t_w01_stagings",
        s3_bucket="sbmd3weather2",
        s3_key="CSV2/" + str(date.today()) + "_Weather_DF.csv",
        redshift_conn_id="postgres_aws_capstone",
        aws_conn_id="aws_credentials_s3",
        autocommit=True,
        trigger_rule="all_done",
        dag=dag
        )

insert_live_train_data = PostgresOperator(
        task_id="05a_Insert_Train_Live_Tables",
        sql=InsertTables.delsql1 + " " + InsertTables.inssql1,
        postgres_conn_id="postgres_aws_capstone",
        autocommit=True,
        trigger_rule="all_done",
        dag=dag)

insert_live_gmap_data = PostgresOperator(
        task_id="05b_Insert_Gmap_Live_Tables",
        sql=InsertTables.delsql2 + " " + InsertTables.inssql2,
        postgres_conn_id="postgres_aws_capstone",
        autocommit=True,
        trigger_rule="all_done",
        dag=dag)

insert_live_weather_data = PostgresOperator(
        task_id="05c_Insert_Weather_Live_Tables",
        sql=InsertTables.delsql3 + " " + InsertTables.inssql3,
        postgres_conn_id="postgres_aws_capstone",
        autocommit=True,
        trigger_rule="all_done",
        dag=dag)

archive_del_db = BashOperator(
        task_id="06bArchive_and_Delete_DB",
        bash_command="python3 /home/ubuntu/sbmd/zzDelDB.py without",
        trigger_rule="all_done",
        dag=dag)

archiv_del_db_fail = BashOperator(
        task_id="zz_Archive_and_Delete_DB_FailCase",
        bash_command="python3 /home/ubuntu/sbmd/zzDelDB.py with",
        trigger_rule="one_failed",
        dag=dag)

##add glue job afterwards

load_train_data >> create_DB_task
load_gmap_data >> create_DB_task
load_weather_data >> create_DB_task

create_DB_task >> drop_stage_tables

drop_stage_tables >> transfer_train_data
drop_stage_tables >> transfer_gmap_data
drop_stage_tables >> transfer_weather_data

transfer_train_data >> insert_live_train_data
transfer_gmap_data >> insert_live_gmap_data
transfer_weather_data >> insert_live_weather_data

insert_live_train_data >> archive_del_db
insert_live_gmap_data >> archive_del_db
insert_live_weather_data >> archive_del_db

#failover part

drop_stage_tables >> archiv_del_db_fail
drop_stage_tables >> archiv_del_db_fail
drop_stage_tables >> archiv_del_db_fail

transfer_train_data >> archiv_del_db_fail
transfer_gmap_data >> archiv_del_db_fail
transfer_weather_data >> archiv_del_db_fail
