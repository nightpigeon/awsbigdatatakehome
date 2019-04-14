import json
import boto3
import uuid
import xarray as xr
import datetime
import gzip
from io import BytesIO,TextIOWrapper
import time
import threading
import os

def main(event, context):
    
    # Read the SNS Message and extract Bucket details
    s3_client = boto3.client('s3')
    message = json.loads(event['Records'][0]['Sns']['Message'])
    bucket = message['Records'][0]['s3']['bucket']['name']
    key = message['Records'][0]['s3']['object']['key']
    
    print("Bucket :"+bucket+" Key :"+key)
    
    # Definining Common variables required for the Program
    d=datetime.datetime.today()
    load_dt=d.strftime('%Y-%m-%d')
    load_ts=d.strftime('%Y%m%d%H%M%S')
    download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
    s3_client.download_file(bucket, key, download_path)
    ds = xr.open_dataset(download_path)
    db=os.environ['DB_NAME']
    
    
    # Initializing the Parallel threads to load data to athena in Parallel
    t1=threading.Thread(target=run_s3_athena,args=(ds,bucket,'dew_point_temperature',load_dt,load_ts,db,'dew_point_temperature'))
    t2=threading.Thread(target=run_s3_athena,args=(ds,bucket,'air_temperature',load_dt,load_ts,db,'air_temperature'))
    t3=threading.Thread(target=run_s3_athena,args=(ds,bucket,'wind_speed_of_gust',load_dt,load_ts,db,'wind_speed_of_gust'))
    t4=threading.Thread(target=run_s3_athena,args=(ds,bucket,'visibility_in_air',load_dt,load_ts,db,'visibility_in_air'))
    
    
    # Starting Parallel Threads to load variables to Athena
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    
    # Waiting for Threads to complete
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    
    print("Lambda completed successfully!")
    
''' Function to load the NetCDF data to Athena

Arguments : 
    xarray dataset,
    variable name to be extracted from the dataset
    load date
    load timestamp
    Athena Database Name
    Athena Table Name
    
Step 1 : Read the arguments passed to the function
Step 2 : Extract the variable from NetCD4 xarray dataset
Step 3 : Convert the dataset variable to a Pandas dataframe
Step 4 : Convert the dataframe to a Gzip compressed byte stream
Step 5 : Write the Buffered stream to S3 bucket and Flush the buffer
Step 6 : Open Athena connection to update Partition Metadata using ALTER TABLE ADD PARTITION
Step 7 : Monitor the status of ALTER TABLE and exit the function

'''
def run_s3_athena(ds,*args):
    
    bucket=args[0]
    var=args[1]
    load_dt=args[2]
    load_ts=args[3]
    db=args[4]
    table=args[5]
    athena_output_s3_loc=os.environ['ATHENA_OP_LOC']

    df=ds["%s" % (var)].to_dataframe() 
    tgt_s3_key=var+"/load_dt="+load_dt+"/ts="+load_ts+"/{}{}".format(uuid.uuid4(),var+".gz")
    
    gz_buffer = BytesIO()
    
    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        df.to_csv(TextIOWrapper(gz_file, 'utf8'), index=True, header=False)
    
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket, tgt_s3_key)
    s3_object.put(Body=gz_buffer.getvalue())
    
    gz_buffer.flush()
    
    print("File %s uploaded to S3 bucket %s successfully" %(tgt_s3_key,bucket))
    
    print("Updating the Partition Metadata for Table %s in Athena" %(table))
    
    
    # Open Athena client
    athena_client = boto3.client('athena')
    query = "ALTER TABLE %s.%s ADD IF NOT EXISTS PARTITION(load_dt='%s',ts='%s');" % (db, table, load_dt, load_ts)
     
    # Execute the query
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': db
        },
        ResultConfiguration={
            'OutputLocation': athena_output_s3_loc
        }
    )
    
    # Get query execution id
    query_execution_id = response['QueryExecutionId']
    print("Query ID :%s" %(query_execution_id))
    
    # Get query execution status
    query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    
    query_execution_status = query_status['QueryExecution']['Status']['State']
    
    # Monitor the query status
    while(query_execution_status=="RUNNING"):
        time.sleep(5)
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']
        
    
    print("Status of Query ID:%s is: %s" % (query_execution_id, query_execution_status))
    
    print("Partition has been refreshed successfully in Athena!")
    
        


if __name__ == "__main__":
    main('', '')
