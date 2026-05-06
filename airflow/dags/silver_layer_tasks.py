import pandas as pd
import sys
import boto3
import logging
from transformation import transform
from io import StringIO

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

################################# AWS ###################################################
# AWS Credentials -->
ACCESS_KEY = "AKIA3YORMK5R7FV4XE43"
SECRETE_ACCESS_KEY = "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"
REGION = "eu-west-1"

# Create AWS session with credentials. using boto3 lib
session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRETE_ACCESS_KEY,
    region_name=REGION,
)

# Get arguments that are passed when running the script
s3Resource = boto3.resource(
    "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRETE_ACCESS_KEY
)
s3Client = boto3.client(
    "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRETE_ACCESS_KEY
)


# =============================================================================
# Internal method to vaidate BucketName
# =============================================================================
def bucketNameValidation(BucketName):
    val = 0
    try:
        for bucket in s3Resource.buckets.all():
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


object = s3Client.get_object(
    Bucket="ep011-808429836131-eu-north-1-staging-bucket",
    Key="rnd/staging_raw/mqtt/2026-05-03 10:34:25.770386",
)
# print(object["Body"].read())
# print(t_form.msgExtraction(object["Body"].read()))
_payloadHexArray_ = []
# _payloadHexArray_ = pd.DataFrame()
# print(_payloadHexArray_)
# for x in range(0, len(object["Body"].read())):
#     _payloadHexArray_.append(object["Body"].read()[x])

csv_string = object["Body"].read().decode("utf-8")
dataframe = pd.read_csv(StringIO(csv_string))
# print(_payloadHexArray_)
print(dataframe[0])
