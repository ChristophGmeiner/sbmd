import boto3
from datetime import datetime, timedelta
import time
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pytz

utc=pytz.UTC

class ModifyRedshift(BaseOperator):
    ui_color = "8958140"
    
    @apply_defaults
    def __init__(self,
                 r_conn_id="",
                 aws_creds="",
                 modtype="",
                 deltype="",
                 azone="eu-central-1b",
                 region_name="eu-central-1",
                 VpcSID="",
                 Port=5439,
                 *args, **kwargs):
        '''
        Initialises an AWS Redshift client and creates 
        (only from a snapshot or deletes a redshift cluster instance
        :r_conn_id - Airflow conn ID of the redshift cluster
        :aws_creds - Credentials for accessing AWS
        :modtype - "create" or "delete"
        :deltype - "with" (snapshot) or "without"
        :azone - Availabiliy zone
        :region_name: AWS region
        :VpcSID - VPC security group id, contained in an Airflow
                  connection
        :Port - Port of DB
        '''
        super(ModifyRedshift, self).__init__(*args, **kwargs)
        self.r_conn_id = r_conn_id
        self.aws_creds = aws_creds
        self.modtype = modtype
        self.deltype = deltype
        self.azone = azone
        self.region_name = region_name
        self.VpcSID = VpcSID
        self.Port = Port
                
    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_creds)
        awscreds = aws_hook.get_credentials()
        
        r_hook = AwsHook(self.r_conn_id)
        rcreds = r_hook.get_credentials()
        
        vpc_hook = AwsHook(self.VpcSID)
        vpccreds = vpc_hook.get_credentials()
        
        client = boto3.client("redshift", 
                              region_name=self.region_name,
                              aws_access_key_id=awscreds.access_key,
                              aws_secret_access_key=awscreds.secret_key)
        
        cid = rcreds.access_key
        snn_base = "sbmd-final-snapshot"
        snn = snn_base + datetime.now().strftime("%y-%m-%d-%H-%M")  
       
        if self.modtype == "create":
            
            try:
            
                dbdesc = client.describe_clusters(ClusterIdentifier=cid)
                dbstate = dbdesc["Clusters"][0]["ClusterStatus"]
                
                if dbstate == "available":
                    self.log.info("Cluster already available")
                    
                    
            except:                    
                            
                existing_sn = client.describe_cluster_snapshots(
                                    ClusterIdentifier=cid)
                esn_list = existing_sn["Snapshots"]
                
                final_esn_list = list()
                final_esn_list.append("test")
                final_esn_list.append(
                        utc.localize(datetime.utcnow()
                            - timedelta(weeks=1000)))
                for s in esn_list:
                    sn_name = s["SnapshotIdentifier"]
                    sn_time = s["SnapshotCreateTime"]
    
                    if sn_name.find(snn_base) > -1 and \
                            sn_time > final_esn_list[1]:
                        final_esn_list.pop()
                        final_esn_list.pop()
                        final_esn_list.append(sn_name)
                        final_esn_list.append(sn_time)
                    
                response = client.restore_from_cluster_snapshot(
                    ClusterIdentifier=cid,
                    SnapshotIdentifier=final_esn_list[0],
                    Port=self.Port,
                    AvailabilityZone=self.azone,
                    PubliclyAccessible=True,
                    VpcSecurityGroupIds=[vpccreds.access_key])
                
                dbdesc = client.describe_clusters(ClusterIdentifier=cid)
                dbstate = dbdesc["Clusters"][0]["ClusterStatus"]
                
                while dbstate != "available":
                    dbdesc = client.describe_clusters(ClusterIdentifier=cid)
                    dbstate = dbdesc["Clusters"][0]["ClusterStatus"]
                time.sleep(100)                             
                self.log.info("Succesfully created!")
            
        if self.modtype == "delete" and self.deltype == "with":
               self.log.info(f"Started deleting with Snapshot: {snn}") 
               response = client.delete_cluster(
                            ClusterIdentifier=cid,
                            SkipFinalClusterSnapshot=False,
                            FinalClusterSnapshotIdentifier=snn,
                            FinalClusterSnapshotRetentionPeriod=7
                            )
               time.sleep(600)
               self.log.info(f"Succesfully deleted with Snapshot: {snn}")
                    
        if self.modtype == "delete" and self.deltype == "without":
               self.log.info("Started deleting without Snapshot!") 
               response = client.delete_cluster(
                        ClusterIdentifier=cid,
                        SkipFinalClusterSnapshot=True,
                        FinalClusterSnapshotRetentionPeriod=1
                        )
               time.sleep(600)
               self.log.info("Succesfully deleted without Snapshot!")
