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
import csv
# from airflow.utils.context import Context # type: ignore
from datetime import datetime, timedelta, timezone
from transformation import transform
from airflow.sdk import Variable
from airflow.sdk.execution_time.xcom import XCom

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
file_urls = []
seconds_timer = 0
################################# AWS ###################################################
load_dotenv()



# Bucket="ep011-808429836131-eu-north-1-staging-bucket",
# Key="rnd/staging_raw/mqtt/2026-05-07 13:31:02.699288",

def create_local_file():
    logging.info(
            {
                "Message": "Creating local files",
            }
        )
    try:
        data = {'sensor_reading':[] }
        if os.path.exists("./dags/transform.json"):
            os.remove("./dags/transform.json")
        # transform = {}
        file = open("./dags/transform.json", "x")
        with open("./dags/transform.json", "w") as file:
            file.write(json.dumps(data))
        
    except Exception as e:
        logging.error(
            {
                "Message": f"error : {e}",
            }
        )
    try:
        if os.path.exists("./dags/manifest.json"):
            os.remove("./dags/manifest.json")
        file = open("./dags/manifest.json", "x")
        
    except Exception as e:
        logging.error(
            {
                "Message": f"error : {e}",
            }
        )

def create_csv_file():
    headers = ["Record_Id","Device_Id","Data_Length","AC_Input_0","AC_Input_1","AC_Input_2","T1","T2","T3","T4","T5","T6","T7", 
             "PCB_NTC", 
             "Flow_1", 
             "Flow_2", 
             "Output_Flag_1", 
             "Output_Flag_2", 
             "Output_Flag_3", 
             "Fan_Tach", 
             "Stepper_Position", 
             "Flow_Rate", 
             "Time_stamp"
            ]
    with open("./dags/processed_data.csv", "w") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)
        
        
def create_manifest_file():
    json_manifest_data = {
    "fileLocations": [{"URIs": []}],
    "globalUploadSettings": {
        "format": "JSON",
        "delimiter": ",",
        "textqualifier": "\"",
        "containsHeader": "true"
        }
    }
    with open("./dags/manifest.json", "w") as file:
        file.write(json.dumps(json_manifest_data))


    

def __key__(time: str, sec) -> datetime:
    starttime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    starttime = starttime + timedelta(seconds=sec)
    return starttime


def start_time_calculation(time: str) -> datetime:
    SERVERTIME = datetime.timestamp(datetime.strptime(time, "%Y-%m-%d %H:%M:%S"))
    starttime = datetime.strptime(
        str(datetime.fromtimestamp(SERVERTIME)), "%Y-%m-%d %H:%M:%S"
    )
    return starttime






starttime = datetime.strptime(str(Variable.get("starttime")), "%Y-%m-%d %H:%M:%S")





'''
CREATE LOCAL FILES
'''


def create_csv_manifest_file():
    create_local_file()
    create_manifest_file()
    create_csv_file()
    
    
with DAG(
    dag_id="create_local_csv_file",
    description="create_local_csv_file",
    # start_date=starttime,
    schedule="@once",
    # schedule_interval=timedelta(seconds=1),
    catchup=False,
) as dag:
    
    start_task = EmptyOperator(task_id="start")
    create = PythonOperator(
        task_id="create_csv",
        python_callable=create_csv_manifest_file
    )
    end_task = EmptyOperator(task_id="end")
    
    start_task >> create >> end_task
    
    
    
    
    
'''

ETL EXTRACT TRANSFORM 

'''

    
    

def read_data_s3(bucket: str, key: str, ti):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_con")
        bool = s3_hook.check_for_key(key=key, bucket_name=bucket)
        if bool:
            logging.info(
                {
                    "Message": "Extracting data ",
                }
            )
            response_file_1 = s3_hook.read_key(key=key, bucket_name=bucket)
            response_file_1 = response_file_1.split()
            ti.xcom_push(key="s3_read_key", value=response_file_1)
            # file = open("./dags/load.txt", "w")
            # file.write(response_file_1)
            logging.info(response_file_1)
            logging.info(
                {
                    "Message": f"s3_read response{type(response_file_1)} ",
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
                "Message": f"error extracting: {e}",
            }
        )

    
def transfomation_script(bucket: str, key: str, ti):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_con")
        logging.info(
            {
                "Message": "transforming data ",
            }
        )
        datalist = []
        sensor_data = ti.xcom_pull(key="s3_read_key", task_ids = "extract_data")
        for data in sensor_data:
            datalist.append(int(data))
        json_data, data_list = t_form.datasource_transformation(datalist)
        logging.info(
            {
                "Message": f"transform data {json_data}, writing to csv file ",
            }
        )
        file = open("./dags/transform.json", "r+")
        file_data = json.load(file)
        file_data["sensor_reading"].append(json_data)
        file.seek(0)
        json.dump(file_data, file, indent=4)
        
        # file.write(json.dumps(json_data))
        with open("./dags/processed_data.csv", 'a', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(data_list)
            # outfile.close()
        

    except Exception as e:
        logging.error(
            {
                "Message": f"error transforming: {e}",
            }
        )
    

with DAG(
    
    dag_id="etl_pipeline_transform_quality_check",
    description="Simulate a daily ETL flow with transformation and S3 upload",
    start_date=starttime,
    schedule=timedelta(seconds=10),
    # schedule_interval=timedelta(seconds=1),
    catchup=False,
) as dag:

    seconds_timer = seconds_timer + 10

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
        task_id="transform",
        python_callable=transfomation_script,
        op_kwargs={
            "bucket": "ep011-808429836131-eu-north-1-staging-bucket",
            "key": f"rnd/staging_raw/mqtt/{starttime}",
        },
    )

    

    end_task = EmptyOperator(task_id="end")
    
    starttime = __key__(starttime.strftime("%Y-%m-%d %H:%M:%S"), seconds_timer)
    start_task >> extract >> task_transform >> end_task




'''
UPLOAD DATA DAG
'''






# Task 3: Upload to S3
def upload_to_s3(bucket: str, key: str):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_con")
        logging.info(
            {
                "Message": "uploading data ",
            }
        )
        s3_hook.load_file(
            filename="./dags/processed_data.csv",
            key=key,
            bucket_name=bucket,
            replace=True,
        )

        logging.info(
            {
                "Message": "updating manifest file ",
            }
        )
        
        manifest = open("./dags/manifest.json", "r")
        data = json.load(manifest)
        filelocation = data["fileLocations"]
        file_urls = filelocation[0]
        file_urls = file_urls["URIs"]
        file_urls.append("https://"+bucket + ".s3.amazonaws.com/"+key+".csv")
        data["fileLocations"][0]["URIs"] = file_urls
        manifest = open("./dags/manifest.json", "w")
        manifest.write(json.dumps(data))
        logging.info(
            {
                "Message": f"updated manifest file {data}",
            }
        )
        
        logging.info(
            {
                "Message": "cleaning csv",
            }
        )
        create_csv_file()
    except Exception as e:
        logging.error(
            {
                "Message": f"error uploading: {e}",
            }
        )


def delete_data():
    s3_hook = S3Hook(aws_conn_id="aws_con")

with DAG(
    dag_id="upload_csv",
    description="Upload csv file to s3",
    start_date=starttime,
    schedule=timedelta(hours=2),
    catchup=False,
) as dag:
    
    start_task = EmptyOperator(task_id="start")
    upload_data = PythonOperator(
        task_id="upload",
        python_callable=upload_to_s3,
        op_kwargs={
            "bucket": "ep011-808429836131-eu-north-1-processed-bucket",
            "key": f"rnd/processes/csv/{datetime.now(timezone.utc)}",
        },
    )
    end_task = EmptyOperator(task_id="end")
    
    start_task >> upload_data >> end_task