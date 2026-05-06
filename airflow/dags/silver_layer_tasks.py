import pandas as pd
import sys
import boto3
import logging
# from transformation import transform
from io import StringIO
from dotenv import load_dotenv
import os
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator 
# from airflow.utils.context import Context # type: ignore
import datetime as dt


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
# t_form = transform()

################################# AWS ###################################################
load_dotenv()

# AWS Credentials -->
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRETE_ACCESS_KEY = os.getenv("SECRETE_ACCESS_KEY")
REGION = "eu-west-1"

# Create AWS session with credentials. using boto3 lib
# session = boto3.Session(
#     aws_access_key_id=ACCESS_KEY,
#     aws_secret_access_key=SECRETE_ACCESS_KEY,
#     region_name=REGION,
# )

# Get arguments that are passed when running the script
# s3Resource = boto3.resource(
#     "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRETE_ACCESS_KEY
# )
s3Client = boto3.client(
    "s3"
)


# =============================================================================
# Internal method to vaidate BucketName
# =============================================================================
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


# object = s3Client.get_object(
#     Bucket="ep011-808429836131-eu-north-1-staging-bucket",
#     Key="rnd/staging_raw/mqtt/2026-05-03 10:34:25.770386",
# )
# print(object["Body"].read())
# print(t_form.msgExtraction(object["Body"].read()))
_payloadHexArray_ = []
# _payloadHexArray_ = pd.DataFrame()
# print(_payloadHexArray_)
# for x in range(0, len(object["Body"].read())):
#     _payloadHexArray_.append(object["Body"].read()[x])

# csv_string = object["Body"].read().decode("utf-8")
# dataframe = pd.read_csv(StringIO(csv_string))
# # print(_payloadHexArray_)
# print(dataframe)

from datetime import datetime, timedelta
import random
default_args = {
    'owner': 'your-name',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}
output_dir = '/opt/airflow/tmp'
raw_file = 'raw_events.csv'
transformed_file = 'transformed_events.csv'
raw_path = os.path.join(output_dir, raw_file)
transformed_path = os.path.join(output_dir, transformed_file)
# Task 1: Generate dynamic event data
def generate_fake_events():
    events = [
        "Solar flare near Mars", "New AI model released", "Fusion milestone","Celestial event tonight", "Economic policy update", "Storm in Nairobi",
        "New particle at CERN", "NASA Moon base plan", "Tremors in Tokyo", "Open-source boom"
    ]
    sample_events = random.sample(events, 5)
    data = {
        "timestamp": [datetime.now().strftime("%Y-%m-%d %H:%M:%S") for _ in sample_events],
        "event": sample_events,
        "intensity_score": [round(random.uniform(1, 10), 2) for _ in sample_events],
        "category": [random.choice(["Science", "Tech", "Weather", "Space", "Finance"]) for _ in sample_events]
    }
    df = pd.DataFrame(data)
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(raw_path, index=False)
    print(f"[RAW] Saved to {raw_path}")

# Task 2: Transform data and save new CSV
def transform_and_save_csv():
    df = pd.read_csv(raw_path)
    # Sort by intensity descending
    df_sorted = df.sort_values(by="intensity_score", ascending=False)
    # Save transformed CSV
    df_sorted.to_csv(transformed_path, index=False)
    print(f"[TRANSFORMED] Sorted and saved to {transformed_path}")

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
    dag_id="daily_etl_pipeline_with_transform",
    default_args=default_args,
    description='Simulate a daily ETL flow with transformation and S3 upload',
    start_date=datetime(2025, 5, 24),
    schedule='@daily',
    catchup=False,
) as dag:
    task_generate = PythonOperator(
        task_id='generate_fake_events',
        python_callable=generate_fake_events
    )
    task_transform = PythonOperator(
        task_id='transform_and_save_csv',
        python_callable=transform_and_save_csv
    )
    # task_upload = PythonOperator(
    #     task_id='upload_to_s3',
    #     python_callable=upload_to_s3,

    # )
    # Task flow
    task_generate >> task_transform




   
    
