from paho.mqtt import client as mqtt_client
import random
import logging
import sys
import pandas as pd
import pandas as pd
import boto3
import time
import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

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

load_dotenv()

# AWS Credentials -->
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRETE_ACCESS_KEY = os.getenv("SECRETE_ACCESS_KEY")
REGION = "eu-west-1"
BUCKET_NAME = "ep011-808429836131-eu-north-1-staging-bucket"

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


        
#################################### MQTT ################################################
broker = "ec2-51-20-252-192.eu-north-1.compute.amazonaws.com"
port = 1883
topic = "NTC/data/transmit-EP03"
client_id = f"python-mqtt-{random.randint(0, 1000)}"
sec_counter = 0
pandas_df = 0
# mytransport = 'websockets' # or 'tcp'
mytransport = 'tcp'
_mqtt_client = mqtt_client.Client(
        # callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
        transport=mytransport,
        client_id=client_id,
        protocol=mqtt_client.MQTTv311,
        clean_session=True,
    )

def on_message(client, userdata, message, properties=None):
    global sec_counter
    sec_counter = sec_counter + 1
    payload = message.payload
    key = f"rnd/staging_raw/mqtt/{datetime.datetime.now()}"
    _payloadHexArray_ = []
    payload = payload.hex()
    for j in range(0, len(payload), 2):
        _payloadHexArray_.append(payload[j : j + 2])
    df = pd.DataFrame(_payloadHexArray_)
    df = df.to_csv()
    if sec_counter == 2:
        s3Client.put_object(Body=df, Bucket=BUCKET_NAME, Key=key)

        sec_counter = 0

##########################################################################################


logging.info("Start of staging process")
_mqtt_client.connect(broker, port)
_mqtt_client.subscribe(topic, 2)
_mqtt_client.on_message = on_message
_mqtt_client.loop_forever()
