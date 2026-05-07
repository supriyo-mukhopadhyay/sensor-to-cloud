import logging
import datetime
import hashlib
import sys

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
        timestamp = datetime.datetime.now()
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
                    "line": format(sys.exc_info()[-1].tb_lineno),
                }
            )

            logging.warning(
                {
                    "Message": "failed to receive complete dataframe: ",
                    "error": str(e),
                    "line": format(sys.exc_info()[-1].tb_lineno),
                }
            )
            return {}
