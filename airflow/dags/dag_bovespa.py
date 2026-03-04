import os
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from scripts.bovespa import extract_bovespa, upload_to_s3


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.now('UTC').subtract(days=1),
    'retries': False,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_bovespa',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['bovespa']
) as dag:
    
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION')

    DEST_LOCAL_FOLDER_PATH = '/home/platero/postech-ml-engineering-fase-2-tech-challenge-airflow/data'

    DEST_BUCKET_NAME = 'postech-ml-engineering-fase-2-tech-challenge-bucket'
    PROCESS_DATE = '{{ ds }}'
    FILE_NAME = 'bovespa.parquet'
    
    DEST_S3_FOLDER_PATH = '{s3_folder}/{process_date}/{file_name}'
    
    task_1 = PythonOperator(
        task_id='extract_bovespa',
        python_callable=extract_bovespa,
        op_kwargs={
            'dest_folder_path': DEST_LOCAL_FOLDER_PATH,
            'process_date': PROCESS_DATE
        }
    )

    task_2 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'aws_access_key_id': AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
            'region': AWS_REGION,
            'src_folder_path': DEST_LOCAL_FOLDER_PATH,
            'dest_bucket_name': DEST_BUCKET_NAME,
            'dest_s3_folder_path': DEST_S3_FOLDER_PATH.format(
                s3_folder='bronze',
                process_date=PROCESS_DATE,
                file_name='bovespa.parquet'
            )
        }
    )