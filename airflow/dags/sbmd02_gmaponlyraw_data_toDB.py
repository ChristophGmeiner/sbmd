from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.sbmd_plugin import RunGlueCrawlerOperator 
from airflow.operators.sbmd_plugin import S3CSVToRedshiftOperator                            
from airflow.operators.sbmd_plugin import ModifyRedshift
from airflow.operators.sbmd_plugin import ArchiveCSVS3
from helpers import InsertTables

default_args = {
        "owner": "Christoph Gmeiner",
        "start_date": datetime(2020, 3, 6, 9, 35),
        "retries": 0,
        "email": "christoph.gmeiner@gmail.com",
        "email_on_success": True,
        "email_on_failure":True,
        "depends_on_past": False
        }

dag = DAG("test_sbmd02rawdatatoDB",
          description="Creates DBs and loads raw data from S3 to Postgres DB",
          default_args=default_args,
          schedule_interval="35 9 * * 5",
          max_active_runs=1,
          catchup=False)

create_DB_task = ModifyRedshift(
        task_id="g02_create_DB_task",
        aws_creds="aws_credentials_s3",
        r_conn_id="redshift_modify",
        modtype="create",
        deltype="",
        VpcSID ="postgres_sec_id",
        dag=dag)

drop_stage_tables = PostgresOperator(
        task_id="g03_Drop_Old_Stage_Tables",
        sql="""
            
            TRUNCATE TABLE t_gmap01_stagings;
        
            """,
        postgres_conn_id="redshift_aws_capstone",
        autocommit=True,
	retries=2,
	retry_delay=timedelta(seconds=200),
        dag=dag)

load_gmap_data = BashOperator(
        task_id="g01b_LoadgmapDB_Data",
        bash_command="python3 /home/ubuntu/sbmd/sbahnmuc04b_TransferDB.py",
        dag=dag)

transfer_gmap_data = S3CSVToRedshiftOperator(
        task_id="g04b_Transfer_gmap_CSV",
        table="t_gmap01_stagings",
        s3_bucket="sbmd2gmap3",
        s3_key="CSV/",
        s3_region="'eu-central-1'",
        redshift_conn_id="redshift_aws_capstone",
        autocommit=True,
        aws_creds="aws_credentials_s3",
	dag=dag)

insert_live_gmap_data = PostgresOperator(
        task_id="g05b_Insert_Gmap_Live_Tables",
        sql=InsertTables.delsql2 + " " + InsertTables.inssql2,
        postgres_conn_id="redshift_aws_capstone",
        autocommit=True,
        dag=dag)


archive_del_db = ModifyRedshift(
        task_id="g06bArchive_and_Delete_DB",
        r_conn_id="redshift_modify",
        aws_creds="aws_credentials_s3",
        modtype="delete",
        deltype="with",
        VpcSID="postgres_sec_id",
	retries=2,
	retry_delay=timedelta(seconds=300),
        dag=dag)

archiv_del_db_fail = ModifyRedshift(
        task_id="g06bArchive_and_Delete_DB_FailCase",
        r_conn_id="redshift_modify",
        aws_creds="aws_credentials_s3",
        modtype="delete",
        deltype="without",
        VpcSID="postgres_sec_id",
        trigger_rule="one_failed",
	retries=2,
	retry_delay=timedelta(seconds=300),
        dag=dag)

startglue_task = RunGlueCrawlerOperator(
        task_id="gzz_StartGlueCrawler",
        region_name="eu-central-1",
        aws_creds="aws_credentials_s3",
        crawler="sbmd",
	retries=2,
	retry_delay=timedelta(seconds=300),
        dag=dag)

archivecsv_task = ArchiveCSVS3(
        task_id="gzz_Archive_CSV_files",
        aws_creds="aws_credentials_s3",
        s3_bucket="sbmd2gmap3",
        s3_source_key="CSV",
        s3_dest_key="CSV_Archive/",
        s3_region_name="eu-central-1",
        dag=dag)


load_gmap_data >> create_DB_task
load_gmap_data >> startglue_task
create_DB_task >> drop_stage_tables
drop_stage_tables >> transfer_gmap_data
transfer_gmap_data >> insert_live_gmap_data
insert_live_gmap_data >> archivecsv_task
insert_live_gmap_data >> archive_del_db

#failover part

drop_stage_tables >> archiv_del_db_fail
transfer_gmap_data >> archiv_del_db_fail
insert_live_gmap_data >> archiv_del_db_fail

