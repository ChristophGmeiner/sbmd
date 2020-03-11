from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class S3CSVToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    region {} 
                    CSV                    
                """
       

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_creds="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_region="",
                 *args, **kwargs):
        '''
        initialises the StageToRedshiftOperator, this is an operator, which 
        transfers (json) from a S§ bucket to a Redshift datawarehouse or 
        Postgres database
        
        :redshift_conn_id - Airflow conection for Postgres or Redshft 
            connection
        :aws_creds - Credentials stored in Airflow connections for accessing 
            the S3 bucket
        :createsql - String indicating the SQL statement for creating the 
            relevant table
        :table - relevant table name as string
        :s3_bucket: String indicating the S3 bucket name, which contains the
            json raw data
        :s3_key - String indicating the file or path
        :s3_region: String indicating the relevant S3 AWS region
        '''

        super(S3CSVToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_creds = aws_creds
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region

    def execute(self, context):
        aws_hook = AwsHook(self.aws_creds)
        creds = aws_hook.get_credentials()
        rs_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Copying data from S3 to Redshift for " + self.table)
        s3_path = "s3://" + self.s3_bucket + "/" + self.s3_key
        formated_sql = S3CSVToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                creds.access_key,
                creds.secret_key,
                self.s3_region)
              
        rs_hook.run(formated_sql)
        
        self.log.info('StageToRedshiftOperator finished!')






