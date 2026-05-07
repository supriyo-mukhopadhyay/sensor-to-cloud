import pandas as pd
import sys
import boto3
import logging

from io import StringIO
from dotenv import load_dotenv
import os
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

# from airflow.utils.context import Context # type: ignore
from datetime import datetime, timedelta
from transformation import transform

################################# Logging ###############################################
# All application logs are saved in producer.log file in project directory
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("producer.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

################################# Definitions ###########################################

UNSIGNED_CHAR = 0
SIGNED_CHAR = 16
UNSIGNED_SHORT = 1
SIGNED_SHORT = 17
t_form = transform()
datalist = []
json_data = ""
################################# AWS ###################################################
load_dotenv()

# AWS Credentials -->
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRETE_ACCESS_KEY = os.getenv("SECRETE_ACCESS_KEY")
REGION = "eu-west-1"


# Bucket="ep011-808429836131-eu-north-1-staging-bucket",
# Key="rnd/staging_raw/mqtt/2026-05-07 13:31:02.699288",


def __key__() -> str:
    time = datetime.now()
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    starttime = datetime.timestamp(datetime.strptime(time, "%Y-%m-%d %H:%M:%S"))
    starttime = starttime - 60
    starttime = str(
        datetime.strptime(str(datetime.fromtimestamp(starttime)), "%Y-%m-%d %H:%M:%S")
    )
    return starttime


def __add_second__key__(time: str) -> str:
    nexttime = datetime.timestamp(datetime.strptime(time, "%Y-%m-%d %H:%M:%S"))
    nexttime = nexttime + 1
    nexttime = str(
        datetime.strptime(str(datetime.fromtimestamp(nexttime)), "%Y-%m-%d %H:%M:%S")
    )
    return nexttime


def read_data_s3(bucket: str, key: str):
    s3Client = boto3.client("s3")

    s3Client.download_file(
        Filename="./temp.txt",
        Bucket=bucket,
        Key=key,
    )

    file = open("./test.txt", "r")

    lines = file.readlines()
    datalist = []
    for line in lines:
        datalist.append(int(line))

    return datalist


def bucketNameValidation(BucketName):
    val = 0
    try:
        for bucket in s3Resource.buckets.all():  # type: ignore
            # s3BucketList.append(bucket.name)
            # print(bucket)
            if BucketName == bucket.name:
                val = 1
                break
            else:
                continue
        return val
    except Exception as e:
        logging.error(e)


def transfomation_script():
    json_data = t_form.datasource_transformation(datalist)
    print(json_data)


default_args = {"owner": "your-name", "retries": 3, "retry_delay": timedelta(minutes=1)}
output_dir = "/opt/airflow/tmp"
raw_file = "raw_events.csv"
transformed_file = "transformed_events.csv"
raw_path = os.path.join(output_dir, raw_file)
transformed_path = os.path.join(output_dir, transformed_file)
starttime = __key__()


# # Task 3: Upload to S3
# def upload_to_s3(**kwargs):
#     run_date = kwargs['ds']
#     bucket_name = 'your-bucket-name'
#     s3_key = f'your-directory-name/events_transformed_{run_date}.csv'
#     s3 = boto3.client('s3')
#     s3.upload_file(transformed_path, bucket_name, s3_key)
#     print(f"Uploaded to s3://{bucket_name}/{s3_key}")

# DAG setup
with DAG(
    dag_id="etl_pipeline_transform_quality_check",
    default_args=default_args,
    description="Simulate a daily ETL flow with transformation and S3 upload",
    start_date=starttime,
    # schedule="@daily",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
) as dag:

    start_task = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=read_data_s3,
        op_kwargs={
            "bucket": "ep011-808429836131-eu-north-1-staging-bucket",
            "key": f"rnd/staging_raw/mqtt/{starttime}",
        },
    )

    task_transform = PythonOperator(
        task_id="transform", python_callable=transfomation_script
    )

    end_task = EmptyOperator(task_id="end")
    start_task >> extract >> task_transform >> end_task
