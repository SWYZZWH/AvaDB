from enum import Enum


class ErrorCode(Enum):
    OK = 0
    START_FAILED = 1
    STOP_FAILED = 2
    CALCULATE_PARAM_CNT_ERROR = 3
    CALCULATE_PARAM_TYPE_ERROR = 4
