from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
        
class CSV_S3_PostgresOperator(BaseOperator):
    ui_color = "#F98866"
    
    copy_sql = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    region {}              
                    DELIMITER ',' CSV;
                """
    
    @apply_defaults
    def __init__(self,
                 aws_creds="",
                 postgres_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_region="",
                 *args, **kwargs):
        '''
        transfer a csv file from S3 to a Postgres DB
        : aws_creds - airflow connection for getting the credentials for 
          accessing S3
        : postgres_conn_id - airflow connection for getting the credentials for 
          accessing postgres DB
        : table - destination table in postgres
        : s3_bucket - bucket which contains source csv
        : s3_key - name of source csv file
        : s3_region - AWS region of S3 bucket
        '''
        
        super(CSV_S3_PostgresOperator, self).__init__(*args, **kwargs)
        self.aws_creds = aws_creds
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key,
        self.s3_region = s3_region

    def execute(self, context):
        
        if self.s3_key[0][-1] != "/":
            self.s3_key = self.s3_key[0] + "/"
        self.log.info(f"Use of bucket {self.s3_bucket}")
        self.log.info(f"Use of key {self.s3_key}")

        aws_hook = AwsHook(self.aws_creds)
        creds = aws_hook.get_credentials()
        pg_hook = PostgresHook(postgress_conn_id="postgres_aws_capstone")
        
        self.log.info("Start copying...")
        
        s3_path = "s3://" + self.s3_bucket + "/" + self.s3_key[0]
        formated_sql = CSV_S3_PostgresOperator.copy_sql.format(
                self.table,
                s3_path,
                creds.access_key,
                creds.secret_key,
                self.s3_region)
        pg_hook.run(formated_sql)
        
        self.log.info("Successfully copied!")
