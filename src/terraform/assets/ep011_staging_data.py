from paho.mqtt import client as mqtt_client
import random
import logging
import sys
import pandas as pd
import pandas as pd
import boto3
import time
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

UNSIGNED_CHAR = 0
SIGNED_CHAR = 16
UNSIGNED_SHORT = 1
SIGNED_SHORT = 17


class extraction:

    def __init(self):
        logging.info(
            {
                "Message": "extraction class initialised: ",
            }
        )

    # @staticmethod
    def hex_to_signed_char(self, s: str) -> int:
        __signedIntArray__ = []
        __signedNumber__ = 0
        for j in range(0, len(s), 2):
            __signedIntArray__.append(s[j : j + 2])
        __signedNumber__ = int(__signedIntArray__[0], 16)
        if __signedNumber__ > 32768:
            __signedNumber__ = __signedNumber__ - 65536
        return __signedNumber__

    # @staticmethod
    def hex_to_signed_number(self, s: str) -> int:
        __signedIntArray__ = []
        __signedNumber__ = 0
        for j in range(0, len(s), 2):
            __signedIntArray__.append(s[j : j + 2])
        __lsb__ = int(__signedIntArray__[1], 16)
        __msb__ = int(__signedIntArray__[0], 16)
        __signedNumber__ = __lsb__ + (__msb__ << 8)
        if __signedNumber__ > 32768:
            __signedNumber__ = __signedNumber__ - 65536
        return __signedNumber__

    # @staticmethod
    def hex_to_unsigned_char(self, s: str) -> int:
        __unsignedIntArray__ = []
        __unsignedNumber__ = 0
        for j in range(0, len(s), 2):
            __unsignedIntArray__.append(s[j : j + 2])
        __unsignedNumber__ = int(__unsignedIntArray__[0], 16)
        return __unsignedNumber__

    # @staticmethod
    def hex_to_unsigned_number(self, s: str) -> int:
        __unsignedIntArray__ = []
        __unsignedNumber__ = 0
        for j in range(0, len(s), 2):
            __unsignedIntArray__.append(s[j : j + 2])
        __lsb__ = int(__unsignedIntArray__[1], 16)
        __msb__ = int(__unsignedIntArray__[0], 16)
        __unsignedNumber__ = __lsb__ + (__msb__ << 8)
        return __unsignedNumber__

    def extract_deviceid(self, ascii_array, dataList):
        for i in range(0, len(ascii_array)):
            if ascii_array[i] != 0:
                dataList.append(ascii_array[i])
        return dataList

    def msgExtraction(self, _payloadHexArray_):
        _usignedCharFlag__ = 0
        _usignedShortFlag__ = 0
        _signedCharFlag__ = 0
        _signedShortFlag__ = 0
        dataList = []
        ascii_array = []
        __signedString__ = ""
        __usignedString__ = ""
        __operationModeString__ = ""
        __hexCollectFlag__ = 0
        byteCounter = 0
        try:
            for x in range(0, len(_payloadHexArray_)):
                if byteCounter < 26:
                    if byteCounter < 2:
                        dataList.append(int(_payloadHexArray_[x], 16))
                    else:
                        ascii_array.append(int(_payloadHexArray_[x], 16))
                else:
                    if byteCounter == 26:
                        dataList = self.extract_deviceid(ascii_array, dataList)
                    if (
                        int(_payloadHexArray_[x], 16) == SIGNED_CHAR
                    ) and __hexCollectFlag__ == 0:
                        _signedCharFlag__ = 1
                        _usignedCharFlag__ = 0
                        __hexCollectFlag__ = 1
                    elif (
                        int(_payloadHexArray_[x], 16) == UNSIGNED_CHAR
                    ) and __hexCollectFlag__ == 0:
                        _usignedCharFlag__ = 1
                        _signedCharFlag__ = 0
                        __hexCollectFlag__ = 1
                    elif (
                        int(_payloadHexArray_[x], 16) == SIGNED_SHORT
                    ) and __hexCollectFlag__ == 0:
                        _signedShortFlag__ = 1
                        _usignedShortFlag__ = 0
                        __hexCollectFlag__ = 1
                    elif (
                        int(_payloadHexArray_[x], 16) == UNSIGNED_SHORT
                    ) and __hexCollectFlag__ == 0:
                        _usignedShortFlag__ = 1
                        _signedShortFlag__ = 0
                        __hexCollectFlag__ = 1
                    else:
                        if _usignedCharFlag__ == 1:
                            __usignedString__ = __usignedString__ + _payloadHexArray_[x]
                            if len(__usignedString__) == 2:
                                dataList.append(
                                    self.hex_to_unsigned_char(__usignedString__)
                                )
                                __usignedString__ = ""
                                _usignedCharFlag__ = 0
                                __hexCollectFlag__ = 0

                        elif _signedCharFlag__ == 1:
                            __signedString__ = __signedString__ + _payloadHexArray_[x]
                            if len(__signedString__) == 2:
                                dataList.append(
                                    self.hex_to_signed_char(__signedString__)
                                )
                                __signedString__ = ""
                                _signedCharFlag__ = 0
                                __hexCollectFlag__ = 0

                        elif _usignedShortFlag__ == 1:
                            __usignedString__ = __usignedString__ + _payloadHexArray_[x]
                            if len(__usignedString__) == 4:
                                dataList.append(
                                    self.hex_to_unsigned_number(__usignedString__)
                                )
                                __usignedString__ = ""
                                _usignedShortFlag__ = 0
                                __hexCollectFlag__ = 0

                        elif _signedShortFlag__ == 1:
                            __signedString__ = __signedString__ + _payloadHexArray_[x]
                            if len(__signedString__) == 4:
                                dataList.append(
                                    self.hex_to_signed_number(__signedString__)
                                )
                                __signedString__ = ""
                                _signedShortFlag__ = 0
                                __hexCollectFlag__ = 0

                        elif byteCounter == (dataList[2] - 2):
                            dataList.append(int(x, 16))  # type: ignore

                byteCounter = byteCounter + 1
            return dataList

        except Exception as e:
            logging.error(
                {
                    "Message": "Failed to transform hex to decimal: ",
                    "error": str(e),
                    "line": format(sys.exc_info()[-1].tb_lineno),  # type: ignore
                }
            )

            logging.warning(
                {
                    "Message": "Failed to transform hex to decimal: ",
                    "error": str(e),
                    "line": format(sys.exc_info()[-1].tb_lineno),  # type: ignore
                }
            )

        # if len(dataList) > 4:
        #     try:
        #         print(dataList)
        #         _payloadHexArray_ = []
        #     except Exception as e:
        #         logging.error(
        #             {"Message": "Failed to send message to topic, error: ", "error": str(e)}
        #         )


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
ext = extraction()
# AWS Credentials -->
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRETE_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
REGION = "eu-west-1"
BUCKET_NAME = "ep011-808429836131-eu-north-1-staging-bucket"
print(ACCESS_KEY)
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
mytransport = "tcp"
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
    time = datetime.now(timezone.utc)
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    key = f"rnd/staging_raw/mqtt/{time}"
    _payloadHexArray_ = []
    payload = payload.hex()
    for j in range(0, len(payload), 2):
        _payloadHexArray_.append(payload[j : j + 2])
    datalist = ext.msgExtraction(_payloadHexArray_)
    with open("myfile.txt", "w") as f:
        # f.write(str(datalist))
        f.write("\n".join(str(data) for data in datalist))
    if sec_counter == 10:
        s3Client.put_object(
            Body=open("./myfile.txt", "rb"), Bucket=BUCKET_NAME, Key=key
        )

        sec_counter = 0


##########################################################################################


logging.info("Start of staging process")
try:
    file = open("myfile.txt", "x")
except Exception as e:
    print(e)
_mqtt_client.connect(broker, port)
_mqtt_client.subscribe(topic, 2)
_mqtt_client.on_message = on_message
_mqtt_client.loop_forever()
# _mqtt_client.loop_start()
