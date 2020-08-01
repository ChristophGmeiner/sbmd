from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.sbmd_plugin import RunGlueCrawlerOperator 
from airflow.operators.sbmd_plugin import S3CSVToRedshiftOperator                             
from airflow.operators.sbmd_plugin import ModifyRedshift
from airflow.operators.sbmd_plugin import ArchiveCSVS3
from helpers import InsertTables
from helpers import DataModel
from helpers import CreateModelTables

default_args = {
        "owner": "Christoph Gmeiner",
        "start_date": datetime(2020, 7, 19, 7, 25),
        "retries": 0,
        "email": "christoph.gmeiner@gmail.com",
        "email_on_success": True,
        "email_on_failure":True,
        "depends_on_past": False
        }

dag = DAG("sbmd02rawdatatoDB",
          description="Creates DBs and loads raw data from S3 to AWS Redshift",
          default_args=default_args,
          #schedule_interval="25 7 */2 * *",
          max_active_runs=1,
          catchup=False)

load_train_data = BashOperator(
        task_id="01a_Load_train_Data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc03b_TransferDB.py",
        dag=dag)

#load_gmap_data = BashOperator(
#        task_id="01b_LoadgmapDB_Data",
#        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc04b_TransferDB.py",
#        dag=dag)

load_weather_data = BashOperator(
        task_id="01c_LoadweatherDB_Data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc05b_TransferDB.py",
        dag=dag)

create_DB_task = ModifyRedshift(
        task_id="02_create_DB_task",
        aws_creds="aws_credentials_s3",
        r_conn_id="redshift_modify",
        modtype="create",
        deltype="",
        VpcSID ="postgres_sec_id",
        dag=dag)

drop_stage_tables = PostgresOperator(
        task_id="03_Empty_Stage_Tables",
        sql="""
            TRUNCATE TABLE t_db01_stagings;
            TRUNCATE TABLE t_w01_stagings;
            """,
        postgres_conn_id="redshift_aws_capstone",
        autocommit=True,
        retries=2,
        retry_delay=timedelta(seconds=200),
        dag=dag)

transfer_train_data = S3CSVToRedshiftOperator(
        task_id="04a_Transfer_db_CSV",
        table="t_db01_stagings",
        s3_bucket="sbmd1db2",
        s3_key="CSV/",
        s3_region="eu-central-1",
        redshift_conn_id="redshift_aws_capstone",
        autocommit=True,
        aws_creds="aws_credentials_s3",
        include_cols=True,
        dag=dag)

#transfer_gmap_data = S3CSVToRedshiftOperator(
#        task_id="04b_Transfer_gmap_CSV",
#        table="t_gmap01_stagings",
#        s3_bucket="sbmd2gmap3",
#        s3_key="CSV/",
#        s3_region="eu-central-1",
#        redshift_conn_id="redshift_aws_capstone",
#        autocommit=True,
#        aws_creds="aws_credentials_s3",
#        include_cols=True,
#        dag=dag)

transfer_weather_data = S3CSVToRedshiftOperator(
        task_id="04c_Transfer_weather_CSV",
        table="t_w01_stagings",
        s3_bucket="sbmd3weather2",
        s3_key="CSV/",
        s3_region="eu-central-1",
        redshift_conn_id="redshift_aws_capstone",
        autocommit=True,
        aws_creds="aws_credentials_s3",
        include_cols=True,
        dag=dag)

insert_live_train_data = PostgresOperator(
        task_id="05a_Insert_Train_Live_Tables",
        sql=InsertTables.delsql1 + " " + InsertTables.inssql1,
        postgres_conn_id="redshift_aws_capstone",
        autocommit=True,
        dag=dag)

#insert_live_gmap_data = PostgresOperator(
#        task_id="05b_Insert_Gmap_Live_Tables",
#        sql=InsertTables.delsql2 + " " + InsertTables.inssql2,
#        postgres_conn_id="redshift_aws_capstone",
#        autocommit=True,
#        dag=dag)

insert_live_weather_data = PostgresOperator(
        task_id="05c_Insert_Weather_Live_Tables",
        sql=InsertTables.delsql3 + " " + InsertTables.inssql3,
        postgres_conn_id="redshift_aws_capstone",
        autocommit=True,
        dag=dag)

archive_del_db = ModifyRedshift(
        task_id="06aArchive_and_Delete_DB",
        r_conn_id="redshift_modify",
        aws_creds="aws_credentials_s3",
        modtype="delete",
        deltype="with",
        VpcSID="postgres_sec_id",
        retries=2,
        retry_delay=timedelta(seconds=300),
        dag=dag)

archiv_del_db_fail = ModifyRedshift(
        task_id="06bArchive_and_Delete_DB_FailCase",
        r_conn_id="redshift_modify",
        aws_creds="aws_credentials_s3",
        modtype="delete",
        deltype="without",
        VpcSID="postgres_sec_id",
        trigger_rule="one_failed",
        retries=2,
        retry_delay=timedelta(seconds=300),
        dag=dag)

#startglue_task = RunGlueCrawlerOperator(
#        task_id="08_StartGlueCrawler",
#        aws_creds="aws_credentials_s3",
#        region_name="eu-central-1",
#        crawler="sbmd",
#        retries=2,
#        retry_delay=timedelta(seconds=300),
#        dag=dag)

#archivecsv_gmap_task = ArchiveCSVS3(
#        task_id="07b_gmap_Archive_CSV_files",
#        aws_creds="aws_credentials_s3",
#        s3_bucket="sbmd2gmap3",
#        s3_source_key="CSV",
#        s3_dest_key="CSV_Archive/",
#        s3_region_name="eu-central-1",
#        dag=dag)

archivecsv_db_task = ArchiveCSVS3(
        task_id="07a_db_Archive_CSV_files",
        aws_creds="aws_credentials_s3",
        s3_bucket="sbmd1db2",
        s3_source_key="CSV",
        s3_dest_key="CSV_Archive/",
        s3_region_name="eu-central-1",
        dag=dag)

archivecsv_weather_task = ArchiveCSVS3(
        task_id="07c_weather_Archive_CSV_files",
        aws_creds="aws_credentials_s3",
        s3_bucket="sbmd3weather2",
        s3_source_key="CSV",
        s3_dest_key="CSV_Archive/",
        s3_region_name="eu-central-1",
        dag=dag)

load_data_model = PostgresOperator(
        task_id="09_DataModel",
        sql=CreateModelTables.wc_queries + " " 
                + CreateModelTables.t05_queries + " " 
            + DataModel.comm,
        postgres_conn_id="redshift_aws_capstone",
        autocommit=True,
        dag=dag)

load_train_data >> create_DB_task
#load_gmap_data >> create_DB_task
load_weather_data >> create_DB_task

create_DB_task >> drop_stage_tables

drop_stage_tables >> transfer_train_data
#drop_stage_tables >> transfer_gmap_data
drop_stage_tables >> transfer_weather_data

transfer_train_data >> insert_live_train_data
#transfer_gmap_data >> insert_live_train_data
transfer_weather_data >> insert_live_train_data

insert_live_train_data >> insert_live_weather_data
#insert_live_gmap_data >> insert_live_weather_data

insert_live_train_data >> archivecsv_db_task
#insert_live_gmap_data >> archivecsv_gmap_task
insert_live_weather_data >> archivecsv_weather_task

insert_live_train_data >> load_data_model
#insert_live_gmap_data >> load_data_model
insert_live_weather_data >> load_data_model

#archivecsv_gmap_task >> startglue_task
#archivecsv_db_task >> startglue_task
#archivecsv_weather_task >> startglue_task

load_data_model >> archive_del_db

#failover part

drop_stage_tables >> archiv_del_db_fail

transfer_train_data >> archiv_del_db_fail
#transfer_gmap_data >> archiv_del_db_fail
transfer_weather_data >> archiv_del_db_fail

insert_live_train_data >> archiv_del_db_fail
#insert_live_gmap_data >> archiv_del_db_fail
insert_live_weather_data >> archiv_del_db_fail

load_data_model >> archiv_del_db_fail
