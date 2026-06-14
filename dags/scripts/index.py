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
        print(f"Downloading combined data from S3 for {execution_date}...")
        objs = s3.list_objects_v2(Bucket='datalake', Prefix=f"data/combined/analytics/air_quality_prediction/{execution_date}/")
        for obj in objs.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                s3.download_file('datalake', obj['Key'], os.path.join(local_combined, os.path.basename(obj['Key'])))
                
    df = spark.read.parquet(local_combined)
    
    # Connect to Elasticsearch with retries
    # Using a list for hosts is more robust in 8.x
    es = Elasticsearch(
        ["http://elasticsearch:9200"],
        request_timeout=30,
        max_retries=10,
        retry_on_timeout=True
    )
    
    index_name = "air_quality_prediction"
    
    # Wait for ES to be ready (up to 30s)
    import time
    for i in range(6):
        try:
            if es.ping():
                break
        except Exception:
            pass
        print("Waiting for Elasticsearch...")
        time.sleep(5)

    # Create index if not exists with geo-point mapping
    if not es.indices.exists(index=index_name):
        mapping = {
            "mappings": {
                "properties": {
                    "location": { "type": "geo_point" },
                    "timestamp_utc": { "type": "date" },
                    "weather_time": { "type": "date" },
                    "value": { "type": "float" },
                    "prediction": { "type": "float" },
                    "temperature": { "type": "float" },
                    "windspeed": { "type": "float" }
                }
            }
        }
        es.indices.create(index=index_name, body=mapping)
        
    # Convert PySpark DataFrame to a list of dicts
    records = df.collect()
    
    count = 0
    for row in records:
        doc = row.asDict()
        
        # Create geo_point field for Elasticsearch mapping
        if 'latitude' in doc and 'longitude' in doc and doc['latitude'] is not None and doc['longitude'] is not None:
            doc['location'] = {
                "lat": float(doc['latitude']),
                "lon": float(doc['longitude'])
            }
        
        # Convert objects to JSON-friendly types
        for k, v in doc.items():
            if hasattr(v, 'isoformat'):
                doc[k] = v.isoformat()
            elif hasattr(v, 'to_eng_string'):
                doc[k] = float(v)
            elif hasattr(v, 'item'):
                doc[k] = v.item()
                
        es.index(index=index_name, document=doc)
        count += 1
        
    print(f"Successfully indexed {count} records into Elasticsearch.")
    spark.stop()
