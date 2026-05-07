import logging
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
                            dataList.append(int(x, 16))

                byteCounter = byteCounter + 1
            return dataList

        except Exception as e:
            logging.error(
                {
                    "Message": "Failed to transform hex to decimal: ",
                    "error": str(e),
                    "line": format(sys.exc_info()[-1].tb_lineno),
                }
            )

            logging.warning(
                {
                    "Message": "Failed to transform hex to decimal: ",
                    "error": str(e),
                    "line": format(sys.exc_info()[-1].tb_lineno),
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
