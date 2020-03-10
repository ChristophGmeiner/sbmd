import boto3
from datetime import datetime, timedelta
import pytz
import time
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ModifyRDSPostgres(BaseOperator):
    ui_color = "8958140"
    
    @apply_defaults
    def __init__(self,
                 rds_conn_id="",
                 aws_creds="",
                 modtype="",
                 deltype="",
                 dbiclass="db.m5.4xlarge",
                 azone="eu-central-1b",
                 region_name="eu-central-1",
                 VpcSID="",
                 DelProtect=False,
                 Port=5432,
                 Engine="postgres",
                 *args, **kwargs):
        '''
        Initialises an AWS RDS client and creates (only from a snapshot or 
        deletes a RDS instance
        :rds_conn_id - Airflow conn ID of the database
        :aws_creds - Credentials for accessing AWS
        :modtype - "create" or "delete"
        :deltype - "with" (snapshot) or "without"
        :dbiclass - Instance type
        :azone - Availabiliy zone
        :region_name: AWS region
        :VpcSID - VPC security group id, contained in an Airflow
                  connection
        :DelProtect - Deletion Protection
        :Port - Port of DB
        :Engine - what engine to use
        '''
        
        super(ModifyRDSPostgres, self).__init__(*args, **kwargs)
        self.rds_conn_id = rds_conn_id
        self.aws_creds = aws_creds
        self.modtype = modtype
        self.deltype = deltype
        self.dbiclass = dbiclass
        self.azone = azone
        self.region_name = region_name
        self.VpcSID = VpcSID
        self.DelProtect = DelProtect
        self.Port = Port
        self.Engine = Engine
        
    def execute(self, context):
        
        utc=pytz.UTC
        
        aws_hook = AwsHook(self.aws_creds)
        awscreds = aws_hook.get_credentials()
        
        rds_hook = AwsHook(self.rds_conn_id)
        rdscreds = rds_hook.get_credentials()
        
        vpc_hook = AwsHook(self.VpcSID)
        vpccreds = vpc_hook.get_credentials()
        
        client = boto3.client("rds", 
                              region_name=self.region_name,
                               aws_access_key_id=awscreds.access_key,
                               aws_secret_access_key=awscreds.secret_key)
        
        rdsid = rdscreds.access_key
        snn_base = "sbmd-final-snapshot"
        snn = snn_base + datetime.now().strftime("%y-%m-%d-%H-%M")  
       
        if self.modtype == "create":
                            
            existing_sn = client.describe_db_snapshots(
                    DBInstanceIdentifier=rdsid)
            esn_list = existing_sn["DBSnapshots"]
            
            final_esn_list = list()
            final_esn_list.append("test")
            final_esn_list.append(
                    utc.localize(datetime.utcnow()
                        - timedelta(weeks=1000)))
            for s in esn_list:
                sn_name = s["DBSnapshotIdentifier"]
                sn_time = s["SnapshotCreateTime"]

                if sn_name.find(snn_base) > -1 and \
                        sn_time > final_esn_list[1]:
                    final_esn_list.pop()
                    final_esn_list.pop()
                    final_esn_list.append(sn_name)
                    final_esn_list.append(sn_time)
                
            response = client.restore_db_instance_from_db_snapshot(
                    DBInstanceIdentifier=rdsid,
                    DBSnapshotIdentifier=final_esn_list[0],
                    DBInstanceClass=self.dbiclass,
                    Port=self.Port,
                    AvailabilityZone=self.azone,
                    PubliclyAccessible=True,
                    Engine=self.Engine,
                    VpcSecurityGroupIds=[vpccreds.access_key],
                    DeletionProtection=self.DelProtect)
            
            dbdesc = client.describe_db_instances(
                    DBInstanceIdentifier=rdsid)
            dbstate = dbdesc["DBInstances"][0]["DBInstanceStatus"]

            while dbstate != "available":
                dbdesc = client.describe_db_instances(
                        DBInstanceIdentifier=rdsid)
                dbstate = dbdesc["DBInstances"][0]["DBInstanceStatus"]
                
            self.log.info("Succesfully created!")
            
        if self.modtype == "delete" and self.deltype == "with":
               response = client.delete_db_instance(
                    DBInstanceIdentifier=rdsid,
                    SkipFinalSnapshot=False,
                    FinalDBSnapshotIdentifier=snn,
                    DeleteAutomatedBackups=True
                    )
               time.sleep(600)
                    
        if self.modtype == "delete" and self.deltype == "without":
               response = client.delete_db_instance(
                    DBInstanceIdentifier=rdsid,
                    SkipFinalSnapshot=True,
                    DeleteAutomatedBackups=True
                    )
               time.sleep(600)