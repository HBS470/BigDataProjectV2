import os
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://localstack:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

def download_s3_file(bucket, key, local_path):
    s3 = get_s3_client()
    s3.download_file(bucket, key, local_path)

def upload_s3_dir(bucket, s3_prefix, local_dir):
    s3 = get_s3_client()
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            s3.upload_file(local_path, bucket, s3_key)

def format_data(**kwargs):
    execution_date = kwargs['ds']
    spark = SparkSession.builder.appName("FormatData").getOrCreate()
    
    # 1. Format OpenAQ (Now using Open-Meteo Air Quality)
    raw_openaq_key = f"data/raw/environment/openaq/{execution_date}/openaq.json"
    local_openaq_raw = f"/tmp/openaq_raw_{execution_date}.json"
    local_openaq_fmt = f"/tmp/openaq_fmt_{execution_date}"
    
    download_s3_file('datalake', raw_openaq_key, local_openaq_raw)
    
    df_openaq = spark.read.option("multiline", "true").json(local_openaq_raw)
    # Normalize
    if "current" in df_openaq.columns:
        df_openaq_clean = df_openaq.select(
            col("city_name"),
            col("latitude"),
            col("longitude"),
            col("current.pm2_5").alias("value"),
            col("current.pm10").alias("pm10"),
            to_timestamp(col("current.time")).alias("timestamp_utc")
        )
    else:
        df_openaq_clean = df_openaq
    
    df_openaq_clean.write.mode("overwrite").parquet(local_openaq_fmt)
    upload_s3_dir('datalake', f"data/formatted/environment/openaq/{execution_date}/", local_openaq_fmt)
    
    # 2. Format Weather
    raw_weather_key = f"data/raw/weather/openmeteo/{execution_date}/weather.json"
    local_weather_raw = f"/tmp/weather_raw_{execution_date}.json"
    local_weather_fmt = f"/tmp/weather_fmt_{execution_date}"
    
    download_s3_file('datalake', raw_weather_key, local_weather_raw)
    df_weather = spark.read.option("multiline", "true").json(local_weather_raw)
    
    # Open-Meteo returns a nested 'current_weather' object
    if "current_weather" in df_weather.columns:
        df_weather_clean = df_weather.select(
            col("city_name"),
            col("current_weather.temperature").alias("temperature"),
            col("current_weather.windspeed").alias("windspeed"),
            col("current_weather.time").alias("weather_time")
        )
    else:
        df_weather_clean = df_weather
        
    df_weather_clean.write.mode("overwrite").parquet(local_weather_fmt)
    upload_s3_dir('datalake', f"data/formatted/weather/openmeteo/{execution_date}/", local_weather_fmt)
    
    spark.stop()


def combine_and_ml(**kwargs):
    execution_date = kwargs['ds']
    spark = SparkSession.builder.appName("CombineAndML").getOrCreate()
    
    # Download formatted data
    local_openaq_fmt = f"/tmp/openaq_fmt_{execution_date}"
    local_weather_fmt = f"/tmp/weather_fmt_{execution_date}"
    local_combined = f"/tmp/combined_{execution_date}"
    
    # Note: normally we download from S3, but we might have them locally from previous step 
    # if running on same worker. To be safe, we re-download or just assume they are in /tmp if local executor.
    # In Airflow LocalExecutor, they are in /tmp. Let's just read from /tmp to simplify, or download if missing.
    if not os.path.exists(local_openaq_fmt):
        s3 = get_s3_client()
        os.makedirs(local_openaq_fmt, exist_ok=True)
        objs = s3.list_objects_v2(Bucket='datalake', Prefix=f"data/formatted/environment/openaq/{execution_date}/")
        for obj in objs.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                s3.download_file('datalake', obj['Key'], os.path.join(local_openaq_fmt, os.path.basename(obj['Key'])))

    if not os.path.exists(local_weather_fmt):
        s3 = get_s3_client()
        os.makedirs(local_weather_fmt, exist_ok=True)
        objs = s3.list_objects_v2(Bucket='datalake', Prefix=f"data/formatted/weather/openmeteo/{execution_date}/")
        for obj in objs.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                s3.download_file('datalake', obj['Key'], os.path.join(local_weather_fmt, os.path.basename(obj['Key'])))

    df_aq = spark.read.parquet(local_openaq_fmt)
    df_w = spark.read.parquet(local_weather_fmt)
    
    # Combine (Join on city_name)
    df_combined = df_aq.join(df_w, on="city_name", how="inner")
    
    # ML: Predict PM2.5 value based on temperature and windspeed
    # Clean data (drop nulls)
    df_ml = df_combined.filter(col("value").isNotNull() & col("temperature").isNotNull() & col("windspeed").isNotNull())
    
    if df_ml.count() > 0:
        assembler = VectorAssembler(inputCols=["temperature", "windspeed"], outputCol="features")
        df_ml_features = assembler.transform(df_ml)
        
        rf = RandomForestRegressor(featuresCol="features", labelCol="value", numTrees=10)
        model = rf.fit(df_ml_features)
        
        predictions = model.transform(df_ml_features)
        final_df = predictions.drop("features")
    else:
        # If not enough data, just add a dummy prediction column
        final_df = df_combined.withColumn("prediction", lit(0.0))
        
    final_df.write.mode("overwrite").parquet(local_combined)
    upload_s3_dir('datalake', f"data/combined/analytics/air_quality_prediction/{execution_date}/", local_combined)
    
    spark.stop()
