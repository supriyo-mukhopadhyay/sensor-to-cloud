import sys
import boto3
import logging
from datetime import datetime, timedelta

# from transformation import transform
from io import StringIO
from dotenv import load_dotenv
import os

load_dotenv()
# AWS Credentials -->
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRETE_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
REGION = "eu-west-1"

# s3Client = boto3.client(
#     "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRETE_ACCESS_KEY
# )

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
# time = datetime.now()
# print(time)
# year = time.year
# month = time.month
# day = time.day
# hour = time.hour
# min = time.minute
# sec = time.second
# min = min - 1
# time = datetime(year=year, month=month, day = day, hour=hour, minute=min, second=sec)
# print(time)
# time = time.strftime("%Y-%m-%d %H:%M:%S")
# print(time)
# starttime = datetime.timestamp(datetime.strptime(time, "%Y-%m-%d %H:%M:%S"))
# print(starttime)
# starttime = starttime - 60
# print(starttime)
# starttime = str(
#     datetime.strptime(str(datetime.fromtimestamp(starttime)), "%Y-%m-%d %H:%M:%S")
# )
# print(starttime)

# object = s3Client.get_object(
#     Bucket="ep011-808429836131-eu-north-1-staging-bucket",
#     Key="rnd/staging_raw/mqtt/2026-05-07 13:31:02.699288",
# )

# s3Client.download_file(
#     Filename="./test.txt",
#     Bucket="ep011-808429836131-eu-north-1-staging-bucket",
#     Key="rnd/staging_raw/mqtt/2026-05-07 13:31:02.699288",
# )
# # raw = object["Body"].read()
# # print(int.from_bytes(raw))
# file = open("./test.txt", "r")
# lines = file.readlines()
# datalist = []
# for line in lines:
#     datalist.append(int(line))
# #     print(raw[x])
# # print(int(datalist[0]))
# data = tr.datasource_transformation(datalist)
# print(data)
# s3 = boto3.client(
#     "s3"
# )


# Bucket="ep011-808429836131-eu-north-1-staging-bucket",
# Key="rnd/staging_raw/mqtt/2026-05-07 13:31:02.699288"

# s3.download_file(
#     Filename="./temp.txt",
#     Bucket=Bucket,
#     Key=Key,
# )

# file = open("./temp.txt", "r")
# lines = file.readlines()
# datalist = []
# for line in lines:
#     datalist.append(int(line))


# date = datetime("2026-05-07 18:24:54")


# SERVERTIME = datetime.timestamp(
#     datetime.strptime("2026-05-07 18:24:54", "%Y-%m-%d %H:%M:%S")
# )
# # SERVERTIME = SERVERTIME + timedelta(seconds=5)


# starttime = datetime.strptime(
#     "2026-05-07 18:24:54", "%Y-%m-%d %H:%M:%S"
# )
# print(starttime)
# starttime = starttime + timedelta(seconds=10)
# print(starttime)


# import awswrangler as wr
# from botocore.exceptions import ClientError
# from dotenv import load_dotenv
# import os
# import boto3
# import json

# AWS Credentials -->
# ACCESS_KEY = os.getenv("ACCESS_KEY")
# SECRETE_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
# print(ACCESS_KEY)
# REGION = "eu-west-1"
# BUCKET_NAME = "ep011-808429836131-eu-north-1-staging-bucket"
# load_dotenv()

# # Create AWS session with credentials. using boto3 lib
# session = boto3.Session(
#     aws_access_key_id=ACCESS_KEY,
#     aws_secret_access_key=SECRETE_ACCESS_KEY,
#     region_name=REGION,
# )

# databases = wr.catalog.databases(boto3_session=session)
# print(databases)

# DATABASE_NAME = "de-c3w2lab1-aws-reviews"

# if DATABASE_NAME not in databases.values.tolist():
#     wr.catalog.create_database(DATABASE_NAME)
#     print(wr.catalog.databases())
# else:
#     print(f"Database {DATABASE_NAME} already exists")

# json_file = {
#     "fileLocations": [{"URIs": ["uri1", "uri2", "uri3"]}],
#     "globalUploadSettings": {
#         "format": "JSON",
#         "delimiter": ",",
#         "textqualifier": "'",
#         "containsHeader": "true",
#     },
# }
# # file = open("manifest.json", "x")
# with open("manifest.json", "w") as file:
#     file.write(json.dumps(json_file))
#     file.close()

# with open("manifest.json", "r") as file:
#     data = json.load(file)
# urls = []
# filelocation = data["fileLocations"]
# urls = filelocation[0]
# urls = urls["URIs"]
# # urls.append(filelocation[0]["URIs"])
# print(urls)
# urls.append("hello1")
# data["fileLocations"][0]["URIs"] = urls
# with open("manifest.json", "w") as file:
#     file.write(json.dumps(data))
#     file.close()
# print()

import pandas as pd
import csv
json_data = ["Record_Id","Device_Id","Data_Length","AC_Input_0","AC_Input_1","AC_Input_2","T1","T2","T3","T4","T5","T6","T7", 
             "PCB_NTC", 
             "Flow_1", 
             "Flow_2", 
             "Output_Flag_1", 
             "Output_Flag_2", 
             "Output_Flag_3", 
             "Fan_Tach", 
             "Stepper_Position", 
             "Flow_Rate", 
             "Time_stamp",
             "Event_Id"
]

with open("file.csv", "w") as outfile:
    writer = csv.writer(outfile)
    writer.writerow(json_data)

# pd.read_csv("file.csv", header=None, names=json_data).


json_data = {"Record_Id": "379210095pr85bd000", 
             "Device_Id": "095pr85bd000", 
             "Data_Length": 77, 
             "AC_Input_0": 1, 
             "AC_Input_1": 2, 
             "AC_Input_2": 3, 
             "T1": 2550, 
             "T2": 2545, 
             "T3": 4318, 
             "T4": 3480, 
             "T5": 2544, 
             "T6": 2543, 
             "T7": -4398, 
             "PCB_NTC": 2912, 
             "Flow_1": 0, 
             "Flow_2": 0, 
             "Output_Flag_1": 0, 
             "Output_Flag_2": 0, 
             "Output_Flag_3": 0, 
             "Fan_Tach": 0, 
             "Stepper_Position": 0, 
             "Flow_Rate": 0, 
             "Time_stamp": "2026-05-14 18:58:29.379210", 
             "Event_Id": "0f42cb0e88ef0f034e88355e75ba9733359693518c19fcd2e005c8232059f84a"
             }

# print(json_data["AC_Input_0"])

# file = open("./Airflow/dags/load.txt", "r")
# lines = file.readlines()
# print(type(lines))

                