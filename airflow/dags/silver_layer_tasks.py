import pandas as pd
import sys
import boto3
import logging
import json
from io import StringIO
from dotenv import load_dotenv
import os
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# from airflow.utils.context import Context # type: ignore
from datetime import datetime, timedelta, timezone
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
try:
    file = open("./dags/load.txt", "x")
    file = open("./dags/transform.txt", "x")
except Exception as e:
    logging.error(
        {
        "Message": f"error : {e}" ,
        }
    )

# AWS Credentials -->
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRETE_ACCESS_KEY = os.getenv("SECRETE_ACCESS_KEY")
REGION = "eu-west-1"
# Bucket="ep011-808429836131-eu-north-1-staging-bucket",
# Key="rnd/staging_raw/mqtt/2026-05-07 13:31:02.699288",


def __key__() -> datetime:
    time = datetime.now(timezone.utc)
    year = time.year
    month = time.month
    day = time.day
    hour = time.hour
    min = time.minute
    sec = time.second
    min = min
    time = datetime(year=year, month=month, day = day, hour=hour, minute=min, second=sec)
    return time



def read_data_s3(bucket: str, key: str):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_con")
        bool = s3_hook.check_for_key(key=key, bucket_name=bucket)
        if bool:
            logging.info(
                {
                    "Message": "Extracting data ",
                }
            )
            response_file_1 = s3_hook.read_key(
                key=key, bucket_name=bucket
            )
            file  = open("./dags/load.txt", "w")
            file.write(response_file_1)
            logging.info(response_file_1)
            logging.info(
                    {
                        "Message": f"s3_read response{response_file_1} ",
                    }
                )
        else:
            logging.info(
                    {
                        "Message": f"s3_read: key not present ",
                    }
                )
    except Exception as e:
        logging.error(
            {
            "Message": f"error extracting: {e}" ,
            }
        )



def transfomation_script(bucket: str, key: str):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_con")
        bool = s3_hook.check_for_key(key=key, bucket_name=bucket)
        if bool:
            logging.info(
                {
                    "Message": "transforming data ",
                }
            )
            datalist = []
            file = open("./dags/load.txt", "r")
            lines = file.readlines()
            for line in lines:
                datalist.append(int(line))
            json_data = t_form.datasource_transformation(datalist)
            logging.info(
                    {
                        "Message": f"transform data{json_data} ",
                    }
                )
            file = open("./dags/transform.txt", "w")
            file.write(str(json_data))
        else:
            logging.info(
                    {
                        "Message": f"transfomation_script: s3_read: key not present ",
                    }
                )
    except Exception as e:
        logging.error(
            {
            "Message": f"error transforming: {e}" ,
            }
        )




# Task 3: Upload to S3
def upload_to_s3(bucket_s: str, key_s: str, bucket: str, key: str):
    try: 
        # s3_key = f"your-directory-name/events_transformed_{run_date}.csv"
        s3_hook = S3Hook(aws_conn_id="aws_con")
        bool = s3_hook.check_for_key(key=key_s, bucket_name=bucket_s)
        if bool:
            logging.info(
                {
                    "Message": "uploading data ",
                }
            )
            s3_hook.load_file(
                    filename="./dags/transform.txt",
                    key=key,
                    bucket_name=bucket,
                    replace=True
                )
        else:
            logging.info(
                    {
                        "Message": f"upload_to_s3: s3_read: key not present ",
                    }
                )
            # s3.put_object(Body=json.dumps(json_data), Bucket=bucket, Key=key)
    except Exception as e:
        logging.error(
            {
            "Message": f"error uploading: {e}" ,
            }
        )


def delete_data():
    # s3 = boto3.client(
    s3.Object("your-bucket", "your-key").delete()

starttime = __key__()
# DAG setup
with DAG(
    dag_id="etl_pipeline_transform_quality_check",
    description="Simulate a daily ETL flow with transformation and S3 upload",
    start_date=starttime,
    schedule=timedelta(seconds=1),
    # schedule_interval=timedelta(seconds=1),
    catchup=False,
) as dag:
    
    starttime = __key__()
    
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
        task_id="transform", python_callable=transfomation_script,
        op_kwargs={
            "bucket": "ep011-808429836131-eu-north-1-staging-bucket",
            "key": f"rnd/staging_raw/mqtt/{starttime}",
        }
    )

    

    upload_data = PythonOperator(
        task_id="upload",
        python_callable=upload_to_s3,
        op_kwargs={
            "bucket_s": "ep011-808429836131-eu-north-1-staging-bucket",
            "key_s": f"rnd/staging_raw/mqtt/{starttime}",
            "bucket": "ep011-808429836131-eu-north-1-processed-bucket",
            "key": f"rnd/processes/json/{starttime}",
        },
    )
    end_task = EmptyOperator(task_id="end")
    start_task >> extract >> task_transform >> upload_data >> end_task
