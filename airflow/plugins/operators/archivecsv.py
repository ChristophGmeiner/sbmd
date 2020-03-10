import boto3
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ArchiveCSVS3(BaseOperator):
    ui_color = "#357640"
    
    @apply_defaults
    def __init__(self,
                 aws_creds="",
                 s3_bucket="",
                 s3_source_key="",
                 s3_dest_key="",
                 s3_region_name="",
                 *args, **kwargs):
        '''
        Initialises an AWS S3 client and archives loaded csv files
        :aws_creds - Airflow AWS conn
        :s3_bucket - Name of the S3 bucket, where the archiving should take
                      place
        :s3_source_key - Name of the S3 key to be archived
        :s3_dest_key - Name of the archivinf destination S3 key
        's3_region_name: Region name of the S3 bucket
        '''
        
        super(ArchiveCSVS3, self).__init__(*args, **kwargs)
        self.aws_creds = aws_creds
        self.s3_bucket = s3_bucket
        self.s3_source_key = s3_source_key
        self.s3_dest_key = s3_dest_key
        self.s3_region_name = s3_region_name
        
        if self.s3_source_key[-1] != "/":
            self.s3_source_key = self.s3_source_key + "/"
            
        if self.s3_dest_key[-1] != "/":
            self.s3_dest_key = self.s3_dest_key + "/"
        
        def execute(self, context):
            aws_hook = AwsHook(self.aws_creds)
            creds = aws_hook.get_credentials()
            
            s3res = boto3.resource("s3", 
                                   region_name=self.region_name,
                                   aws_access_key_id=creds.access_key,
                                   aws_secret_access_key=creds.secret_key)
            
            bucket = s3res.Bucket(self.s3_bucket)
            objsr_all = bucket.objects.all()
            
            s3r_files = []
            
            for o in objsr_all:
                s3r_files.append(o)
             
            sea = self.s3_source_key    
            s3r_files = [x for x in s3r_files if x.find(sea) > -1]
            
            for f in s3r_files:
                      
                copy_source = {"BUCKET": self.s3_bucket, 
                               "Key": f}
                dest = s3res.Object(self.s3_bucket, self.s3_dest_key)
                dest.copy(CopySource=copy_source)
                response=s3res.Object(self.s3_bucket, f).delete()