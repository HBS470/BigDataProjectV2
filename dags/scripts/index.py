import os
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
import boto3

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://localstack:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

def index_data(**kwargs):
    execution_date = kwargs['ds']
    spark = SparkSession.builder.appName("IndexData").getOrCreate()
    
    local_combined = f"/tmp/combined_{execution_date}"
    
    if not os.path.exists(local_combined):
        s3 = get_s3_client()
        os.makedirs(local_combined, exist_ok=True)
        objs = s3.list_objects_v2(Bucket='datalake', Prefix=f"data/combined/analytics/air_quality_prediction/{execution_date}/")
        for obj in objs.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                s3.download_file('datalake', obj['Key'], os.path.join(local_combined, os.path.basename(obj['Key'])))
                
    df = spark.read.parquet(local_combined)
    
    # Connect to Elasticsearch
    es = Elasticsearch("http://elasticsearch:9200")
    
    index_name = "air_quality_prediction"
    
    # Create index if not exists
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
        
    # Convert PySpark DataFrame to a list of dicts directly to avoid Pandas timezone issues
    # Note: collect() brings all data to driver. Fine for this small dataset.
    records = df.collect()
    
    count = 0
    for row in records:
        doc = row.asDict()
        
        # Convert datetime objects to ISO strings for Elasticsearch
        for k, v in doc.items():
            if hasattr(v, 'isoformat'):
                doc[k] = v.isoformat()
                
        es.index(index=index_name, document=doc)
        count += 1
        
    print(f"Indexed {count} records into Elasticsearch.")
    spark.stop()
