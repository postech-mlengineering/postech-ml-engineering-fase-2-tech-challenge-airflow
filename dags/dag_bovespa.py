import os
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from scripts.utils_bovespa import web_scraping, upload_to_bronze, submit_glue_job, submit_glue_crawlers
from airflow.models import Variable


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.now(tz="America/Sao_Paulo").subtract(days=1),
    "retries": False,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="dag_bovespa",
    default_args=default_args,
    schedule="0 0 * * 1-5", #seg a sex, meia noite 
    max_active_runs=1,
    catchup=False,
    tags=["bovespa"]
) as dag:

    AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = "us-east-1"

    PROCESS_DATE = "{{ data_interval_start.in_timezone('America/Sao_Paulo').strftime('%Y-%m-%d') }}"
    DEST_LOCAL_FOLDER_PATH = os.path.abspath(f"data/raw/extract_date={PROCESS_DATE}")

    DEST_BUCKET_NAME = "postech-ml-engineering-fase-2-tech-challenge-bucket"
    DEST_S3_FOLDER_PATH = "{s3_folder}/extract_date={process_date}/{file_name}"
    
    CRAWLERS= ["index_composition", "asset_moving_average", "sector_market_share"]
    ATHENA_TABLES= ["tb_index_composition", "tb_asset_moving_average", "tb_sector_market_share"]

    task_1 = PythonOperator(
        task_id="web_scraping",
        python_callable=web_scraping,
        op_kwargs={
            "dest_folder_path": DEST_LOCAL_FOLDER_PATH,
            "process_date": PROCESS_DATE
        }
    )

    task_2 = PythonOperator(
        task_id="upload_to_bronze",
        python_callable=upload_to_bronze,
        op_kwargs={
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "region": AWS_REGION,
            "src_folder_path": DEST_LOCAL_FOLDER_PATH,
            "dest_bucket_name": DEST_BUCKET_NAME,
            "dest_s3_folder_path": DEST_S3_FOLDER_PATH.format(
                s3_folder="bronze/bovespa/index_composition",
                process_date=PROCESS_DATE,
                file_name=""
            ),
            "process_date": PROCESS_DATE
        }
    )

    task_3 = PythonOperator(
        task_id="submit_glue_job_silver",
        python_callable=submit_glue_job,
        op_kwargs={
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "region": AWS_REGION,
            "job_name": "job_bronze_to_silver",
            "script_args": {
                "--input_path": f"s3://{DEST_BUCKET_NAME}/bronze/bovespa/index_composition/extract_date={PROCESS_DATE}/*.csv",
                "--output_path": f"s3://{DEST_BUCKET_NAME}/silver/bovespa/index_composition",
                "--process_date": PROCESS_DATE
            },
            "process_date": PROCESS_DATE
        }
    )

    task_4 = PythonOperator(
        task_id="submit_glue_job_gold",
        python_callable=submit_glue_job,
        op_kwargs={
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "region": AWS_REGION,
            "job_name": "job_silver_to_gold",
            "script_args": {
                "--input_path": f"s3://{DEST_BUCKET_NAME}/silver/bovespa/index_composition",
                "--output_path": f"s3://{DEST_BUCKET_NAME}/gold/bovespa/index_composition",
                "--process_date": PROCESS_DATE
            },
            "process_date": PROCESS_DATE
        }
    )

    task_5 = PythonOperator(
        task_id="submit_glue_crawler",
        python_callable=submit_glue_crawlers,
        op_kwargs={
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "region": AWS_REGION,
            "crawlers": CRAWLERS,
            "process_date": PROCESS_DATE
        }
    )
    
    task_1 >> task_2 >> task_3 >> task_4 >> task_5