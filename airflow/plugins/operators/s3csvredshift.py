from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from io import StringIO
import pandas as pd
import boto3

class S3CSVToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
                    COPY {}
                    ({})
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    region {} 
                    CSV                    
                """

    copy_sql_excl = """
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
                 include_cols="",
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
        :include cols - True indicates that colnames are provided in the copy
                        statement with the CSV header text
        '''

        super(S3CSVToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_creds = aws_creds
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.include_cols = include_cols
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_creds)
        creds = aws_hook.get_credentials()
        rs_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Starting gathering CSV files from S3...")
        
        s3r = boto3.resource("s3", 
                             region_name=self.s3_region, 
                             aws_access_key_id=creds.access_key, 
                             aws_secret_access_key=creds.secret_key)
        bucket = s3r.Bucket(self.s3_bucket)
        csv_obj = bucket.objects.filter(Prefix=self.s3_key)
        csvlist = []
        for c in csv_obj:
            csvlist.append(c.key)
        csvlist = [x for x in csvlist if x.find(".csv") > -1]
        
        client = boto3.client("s3", 
                             region_name=self.s3_region, 
                             aws_access_key_id=creds.access_key, 
                             aws_secret_access_key=creds.secret_key)
                
        i = 1
        for c in csvlist:
            ind = str(i)
            csvo = client.get_object(Bucket=self.s3_bucket, Key=c)
            body = csvo["Body"]
            csv_string = body.read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_string))
            cols = list(df.columns)

            #for bringing sql fields in sql format
            if 'timestamp' in cols:
                cols[cols.index('timestamp')] = '"timestamp"'

            cols = ', '.join(cols)
                           
            self.log.info(f"Copying file {ind} from S3 to Redshift for " 
                          + self.table)
            s3_path = "s3://" + self.s3_bucket + "/" + c
            
            if self.include_cols == True:
                formated_sql = S3CSVToRedshiftOperator.copy_sql.format(
                        self.table,
                        cols,
                        s3_path,
                        creds.access_key,
                        creds.secret_key,
                        "'" + self.s3_region + "'")
            
            else:
                formated_sql = S3CSVToRedshiftOperator.copy_sql_excl.format(
                        self.table,
                        s3_path,
                        creds.access_key,
                        creds.secret_key,
                        "'" + self.s3_region + "'")
              
            rs_hook.run(formated_sql)
        
            self.log.info(f'StageToRedshiftOperator finished for file {ind}!')
            
            i += 1
        
        check_sql = f"SELECT COUNT(*) FROM {self.table}"
        
        checkno = rs_hook.get_records(check_sql)
        
        if checkno == 0:
            raise ValueError("No data transfered to Redshift!")
            
        
        self.log.info("Data Quality check passed successfully!")
        






