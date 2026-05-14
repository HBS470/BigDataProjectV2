from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import our scripts
from scripts.ingest import init_s3_bucket, ingest_openaq, ingest_weather
from scripts.process import format_data, combine_and_ml
from scripts.index import index_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'air_quality_weather_pipeline',
    default_args=default_args,
    description='A Big Data pipeline to correlate Air Quality and Weather',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 5, 12),
    catchup=False,
    tags=['bigdata', 'project'],
) as dag:

    # 0. Initialize S3 Bucket
    task_init_s3 = PythonOperator(
        task_id='init_s3_bucket',
        python_callable=init_s3_bucket,
    )

    # 1. Ingestion
    task_ingest_openaq = PythonOperator(
        task_id='ingest_openaq_data',
        python_callable=ingest_openaq,
        provide_context=True,
    )

    task_ingest_weather = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=ingest_weather,
        provide_context=True,
    )

    # 2. Formatting (Spark)
    task_format_data = PythonOperator(
        task_id='format_data_spark',
        python_callable=format_data,
        provide_context=True,
    )

    # 3. Combination & ML (Spark)
    task_combine_and_ml = PythonOperator(
        task_id='combine_and_ml_spark',
        python_callable=combine_and_ml,
        provide_context=True,
    )

    # 4. Indexing (Elasticsearch)
    task_index_data = PythonOperator(
        task_id='index_elasticsearch',
        python_callable=index_data,
        provide_context=True,
    )

    # Define Dependencies
    task_init_s3 >> [task_ingest_openaq, task_ingest_weather]
    [task_ingest_openaq, task_ingest_weather] >> task_format_data
    task_format_data >> task_combine_and_ml
    task_combine_and_ml >> task_index_data
