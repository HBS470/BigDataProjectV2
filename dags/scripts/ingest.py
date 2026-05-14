import requests
import json
import boto3
from datetime import datetime

CITIES = [
    {"name": "Tokyo", "lat": 35.6895, "lon": 139.6917},
    {"name": "Delhi", "lat": 28.6139, "lon": 77.2090},
    {"name": "Shanghai", "lat": 31.2304, "lon": 121.4737},
    {"name": "Sao Paulo", "lat": -23.5505, "lon": -46.6333},
    {"name": "Mexico City", "lat": 19.4326, "lon": -99.1332},
    {"name": "Cairo", "lat": 30.0444, "lon": 31.2357},
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"name": "Beijing", "lat": 39.9042, "lon": 116.4074},
    {"name": "Dhaka", "lat": 23.8103, "lon": 90.4125},
    {"name": "Osaka", "lat": 34.6937, "lon": 135.5023},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Karachi", "lat": 24.8607, "lon": 67.0011},
    {"name": "Buenos Aires", "lat": -34.6037, "lon": -58.3816},
    {"name": "Chongqing", "lat": 29.5332, "lon": 106.5050},
    {"name": "Istanbul", "lat": 41.0082, "lon": 28.9784},
    {"name": "Kolkata", "lat": 22.5726, "lon": 88.3639},
    {"name": "Manila", "lat": 14.5995, "lon": 120.9842},
    {"name": "Lagos", "lat": 6.5244, "lon": 3.3792},
    {"name": "Rio de Janeiro", "lat": -22.9068, "lon": -43.1729},
    {"name": "Tianjin", "lat": 39.0842, "lon": 117.2009},
    {"name": "Kinshasa", "lat": -4.4419, "lon": 15.2663},
    {"name": "Guangzhou", "lat": 23.1291, "lon": 113.2644},
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
    {"name": "Moscow", "lat": 55.7558, "lon": 37.6173},
    {"name": "Shenzhen", "lat": 22.5431, "lon": 114.0579},
    {"name": "Lahore", "lat": 31.5204, "lon": 74.3587},
    {"name": "Bangalore", "lat": 12.9716, "lon": 77.5946},
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "Berlin", "lat": 52.5200, "lon": 13.4050},
]

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://localstack:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

def init_s3_bucket(bucket_name='datalake'):
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        s3.create_bucket(Bucket=bucket_name)

def ingest_openaq(**kwargs):
    s3 = get_s3_client()
    execution_date = kwargs['ds']
    all_data = []

    for city in CITIES:
        # Fetch PM2.5 data for the city using Open-Meteo Air Quality API
        url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={city['lat']}&longitude={city['lon']}&current=pm10,pm2_5"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data['city_name'] = city['name']
            all_data.append(data)

    # Save to S3
    s3_key = f"data/raw/environment/openaq/{execution_date}/openaq.json"
    s3.put_object(
        Bucket='datalake',
        Key=s3_key,
        Body=json.dumps(all_data)
    )
    print(f"Uploaded OpenAQ data to s3://datalake/{s3_key}")

def ingest_weather(**kwargs):
    s3 = get_s3_client()
    execution_date = kwargs['ds']
    all_data = []

    for city in CITIES:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current_weather=true"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data['city_name'] = city['name']
            all_data.append(data)

    s3_key = f"data/raw/weather/openmeteo/{execution_date}/weather.json"
    s3.put_object(
        Bucket='datalake',
        Key=s3_key,
        Body=json.dumps(all_data)
    )
    print(f"Uploaded Weather data to s3://datalake/{s3_key}")
