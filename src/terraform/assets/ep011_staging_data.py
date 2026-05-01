from paho.mqtt import client as mqtt_client
import random
import logging
import sys
import boto3
import datetime
from botocore.exceptions import ClientError
import pandas as pd

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

# AWS Credentials -->
ACCESS_KEY = "AKIARKQBUNIWJ3TZT4W3"
SECRETE_ACCESS_KEY = "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"
REGION = "eu-north-1"
BUCKET_NAME = "ep011"
#################################### AWS ################################################

# Create AWS session with credentials. using boto3 lib
session = boto3.Session(
    aws_access_key_id="ACCESS_KEY",
    aws_secret_access_key="SECRETE_ACCESS_KEY",
    region_name=REGION,
)

# Specify mqtt details.
broker = "ec2-51-20-252-192.eu-north-1.compute.amazonaws.com"
port = 1883
topic = "NTC/data/transmit-EP03"
client_id = f"python-mqtt-{random.randint(0, 1000)}"

# Get arguments that are passed when running the script

s3Resource = boto3.resource(
    "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRETE_ACCESS_KEY
)
s3Client = boto3.client(
    "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRETE_ACCESS_KEY
)

sec_counter = 0


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


def createBucket(bucketName, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).
    """
    BucketName = bucketName
    # Create bucket
    try:
        if bucketNameValidation(BucketName) == 0:
            if region is None:
                s3Client.create_bucket(Bucket=BucketName)
            else:
                location = {"LocationConstraint": region}
                s3Client.create_bucket(
                    Bucket=BucketName, CreateBucketConfiguration=location
                )
        else:
            logging.info("Bucket exists !!!")
    except ClientError as e:
        logging.error(format(sys.exc_info()[-1].tb_lineno))
        logging.error(e)
        return False
    return True


def on_message(client, userdata, msg):
    global sec_counter
    sec_counter = sec_counter + 1
    key = f"rnd/staging/{datetime.datetime.now()}"
    payload = msg.payload
    _payloadHexArray_ = []
    payload = payload.hex()
    for j in range(0, len(payload), 2):
        _payloadHexArray_.append(payload[j : j + 2])
    df = pd.DataFrame(_payloadHexArray_)
    df = df.to_csv()
    if sec_counter == 2:
        s3Client.put_object(Body=df, Bucket=BUCKET_NAME, Key=key)
        sec_counter = 0
    # dataList = []


def connect_mqtt():
    # For paho-mqtt 2.0.0, you need to set callback_api_version. and also set the client ID
    _mqtt_client = mqtt_client.Client(
        callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
        client_id=client_id,
    )
    # if MQTT is setup with username and password then use the below line
    # client.username_pw_set(username, password)
    # client.on_connect = on_connect
    _mqtt_client.connect(broker, port)
    return _mqtt_client


def main():
    logging.info("Start of staging process")
    createBucket(BUCKET_NAME, REGION)
    _mqtt_client = connect_mqtt()
    _mqtt_client.subscribe(topic=topic)
    _mqtt_client.on_message = on_message
    logging.info("---- Producer starts sending data -----")
    # logging.disable(logging.INFO)
    # logging.basicConfig(level=logging.ERROR)
    _mqtt_client.loop_forever()


main()
