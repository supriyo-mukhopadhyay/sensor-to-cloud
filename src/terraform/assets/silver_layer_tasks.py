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

# from airflow.utils.context import Context # type: ignore
from datetime import datetime, timedelta
from datetime import datetime, timezone
import hashlib


class transform:

    def __init__(self, **kwargs):

        logging.info(
            {
                "Message": "transform class initialised: ",
            }
        )

    ################################# AWS ###################################################

    def generate_event_id(self, deviceid, timestamp):
        raw = f"{deviceid}-{timestamp}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def datasource_transformation(self, dataList) -> dict:
        deviceidAscii = [dataList[val] for val in range(2, len(dataList) - 19)]
        deviceid = "".join(map(chr, deviceidAscii))
        json_data = {}
        timestamp = datetime.now()
        try:
            json_data = {
                "Record_Id": f"{str(timestamp.microsecond) + deviceid}",
                "Device_Id": f"{deviceid}",
                "Data_Length": dataList[1],
                "AC_Input_0": dataList[14],
                "AC_Input_1": dataList[15],
                "AC_Input_2": dataList[16],
                "T1": dataList[17],
                "T2": dataList[18],
                "T3": dataList[19],
                "T4": dataList[20],
                "T5": dataList[21],
                "T6": dataList[22],
                "T7": dataList[23],
                "PCB_NTC": dataList[24],
                "Flow_1": dataList[25],
                "Flow_2": dataList[26],
                "Output_Flag_1": dataList[27],
                "Output_Flag_2": dataList[28],
                "Output_Flag_3": dataList[29],
                "Fan_Tach": dataList[30],
                "Stepper_Position": dataList[31],
                "Flow_Rate": dataList[32],
                "Time_stamp": f"{timestamp}",
                "Event_Id": f"{self.generate_event_id(deviceid, timestamp)}",
            }
            
            return json_data
        
        except Exception as e:
            logging.error(
                {
                    "Message": "failed to receive complete dataframe: ",
                    "error": str(e),
                    "line": format(sys.exc_info()[-1].tb_lineno), # type: ignore
                }
            )

            logging.warning(
                {
                    "Message": "failed to receive complete dataframe: ",
                    "error": str(e),
                    "line": format(sys.exc_info()[-1].tb_lineno), # type: ignore
                }
            )
            return {}

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
s3 = boto3.client(
    "s3"
)

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
    min = min - 1
    time = datetime(year=year, month=month, day = day, hour=hour, minute=min, second=sec)
    return time


def __add_second__key__(time) -> datetime:
    # time = datetime.now()
    year = time.year
    month = time.month
    day = time.day
    hour = time.hour
    min = time.minute
    sec = time.second + 2
    min = min
    time = datetime(year=year, month=month, day = day, hour=hour, minute=min, second=sec)
    return time


def read_data_s3(bucket: str, key: str):
    # s3Client = boto3.client(

    response  = s3.get_object(
        Bucket=bucket,
        Key=key,
    )
    
    return response["Body"].read()

    # file = open("./temp.txt", "r")

    # lines = file.readlines()
    # datalist = []
    # for line in lines:
    #     datalist.append(int(line))

    # return datalist



def transfomation_script():
    json_data = t_form.datasource_transformation(datalist)
    print(json_data)


starttime = __key__()


# Task 3: Upload to S3
def upload_to_s3(bucket: str, key: str):
    # s3_key = f"your-directory-name/events_transformed_{run_date}.csv"
    
    s3.put_object(Body=json.dumps(json_data), Bucket=bucket, Key=key)


def delete_data():
    # s3 = boto3.client(
    s3.Object("your-bucket", "your-key").delete()

import time
starttime_ = __key__()
# while 1:
#     try:
#         key = f"rnd/staging_raw/mqtt/{starttime_}"
#         print(key)
#         print(type(read_data_s3("ep011-808429836131-eu-north-1-staging-bucket", key)))
#         starttime_ = __key__()
#     except Exception as e:
#         starttime_ = __key__()
#     time.sleep(1)
data = read_data_s3("ep011-808429836131-eu-north-1-staging-bucket", "rnd/staging_raw/mqtt/2026-05-07 19:52:55")
print(data)