import os
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from scripts.utils_bovespa import extract_bovespa, upload_to_s3, submit_glue_job
from airflow.models import Variable


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": "2026-03-01",
    "retries": False,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="dag_bovespa",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["bovespa"]
) as dag:
    
    AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = "us-east-1"

    DEST_LOCAL_FOLDER_PATH = os.path.abspath("data")

    DEST_BUCKET_NAME = "postech-ml-engineering-fase-2-tech-challenge-bucket"
    PROCESS_DATE = "{{ data_interval_start.in_timezone('America/Sao_Paulo').strftime('%Y-%m-%d') }}"
    FILE_NAME = "bovespa.parquet"
    
    DEST_S3_FOLDER_PATH = "{s3_folder}/extract_date={process_date}/{file_name}"
    
    task_1 = PythonOperator(
        task_id="web_scraping",
        python_callable=extract_bovespa,
        op_kwargs={
            "dest_folder_path": DEST_LOCAL_FOLDER_PATH,
            "process_date": PROCESS_DATE
        }
    )

    task_2 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "region": AWS_REGION,
            "src_folder_path": DEST_LOCAL_FOLDER_PATH,
            "dest_bucket_name": DEST_BUCKET_NAME,
            "dest_s3_folder_path": DEST_S3_FOLDER_PATH.format(
                s3_folder="bronze/bovespa/pregao",
                process_date=PROCESS_DATE,
                file_name=""
            ),
            "process_date": PROCESS_DATE
        }
    )

    task_3 = PythonOperator(
        task_id="submit_glue_job",
        python_callable=submit_glue_job,
        op_kwargs={
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "region": AWS_REGION,
            "job_name": "job_bronze_to_silver",
            "script_args": {
                "--input_path": f"s3://{DEST_BUCKET_NAME}/bronze/bovespa/pregao/extract_date={PROCESS_DATE}/*.csv",
                "--output_path": f"s3://{DEST_BUCKET_NAME}/silver/bovespa/pregao",
                "--process_date": PROCESS_DATE
            }
        }
    )

    task_1 >> task_2 >> task_3