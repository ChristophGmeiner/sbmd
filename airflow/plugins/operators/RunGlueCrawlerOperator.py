import boto3
import os
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
        
class RunGlueCrawlerOperator(BaseOperator):
    ui_color = "#358140"
    
    @apply_defaults
    def __init__(self,
                 aws_creds="",
                 region_name="",
                 crawler="",
                 *args, **kwargs):
        '''
        Initialises a AWS Glue crawler and runs it
        :aws_creds - name of Airflow connection for AWS credentials
        :region_name - region of AWS Glue
        :crawler - name of the AWS Glue crawler
        '''
        
        super(RunGlueCrawlerOperator, self).__init__(*args, **kwargs)
        self.aws_creds = aws_creds
        self.region_name = region_name
        self.crawler = crawler
        
        def execute(self, context):
            aws_hook = AwsHook(self.aws_creds)
            creds = aws_hook.get_credentials()
             
            self.log.info("Initialising the glue client")
            os.environ['AWS_ACCESS_KEY_ID']=creds.access_key
            os.environ['AWS_SECRET_ACCESS_KEY']=creds.secret_key
            
            glue = boto3.client("glue", region_name=self.region_name)
            glue.start_crawler(Name=self.crawler)
            
            state = glue.get_crawler(Name=self.crawler)
            state = state["Crawler"]["State"]
            
            self.log.info("Started Crawler...")
            
            while state != "READY":
                state = glue.get_crawler(Name=self.crawler)
                state = state["Crawler"]["State"]
            
            self.log.info(f"Crawler is {state}")


