import sys
import boto3
import logging
from datetime import datetime

# from transformation import transform
from io import StringIO
from dotenv import load_dotenv
import os
from transformation import transform

tr = transform()
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
s3 = boto3.client(
    "s3"
)


Bucket="ep011-808429836131-eu-north-1-staging-bucket",
Key="rnd/staging_raw/mqtt/2026-05-07 13:31:02.699288"

s3.download_file(
    Filename="./temp.txt",
    Bucket=Bucket,
    Key=Key,
)

file = open("./temp.txt", "r")
lines = file.readlines()
datalist = []
for line in lines:
    datalist.append(int(line))
