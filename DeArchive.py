import boto3
import os
import configparser

def dearchive(BUCKET,
              archivname,
              index,
              cfg="/home/ubuntu/sbmd/dwh.cfg"):
    '''
    dearchives failed archives
    :BUCKET: relevant S3 bucket
    :archivname: Relevant archivfolder, make sure to have a "/" at the end
    :index: where does the file name start,
            train: 20,
            gmap: 22,
            weather: 25
    '''
   
    config = configparser.ConfigParser()
    config.read(cfg)
    
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']
    
    s3res = boto3.resource("s3")
    
    s3r = boto3.resource("s3")
    bucket = s3r.Bucket(BUCKET)
    objsr_all = bucket.objects.all()
    
    s3r_files = []
    for o in objsr_all:
        if o.key.find(archivname) > -1:
            s3r_files.append(o.key)
    
    s3r_files = s3r_files[1:]
    
    for file in s3r_files[1:]:
        #archiving back
        copy_source = {"Bucket": BUCKET, "Key": file}
        dest = s3res.Object(BUCKET, file[index:])
        dest.copy(CopySource=copy_source)
        response = s3res.Object(BUCKET, file).delete()

