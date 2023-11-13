import app.common.error.error_codes as codes


class Status:

    def __init__(self, error_code: codes.ErrorCode, msg: str = ""):
        self.error_code = error_code
        self.msg = msg

    def ok(self) -> bool:
        return self.error_code == codes.ErrorCode.OK

    def msg(self) -> str:
        return self.msg

    def code(self) -> codes.ErrorCode:
        return self.error_code

    def __str__(self) -> str:
        return " status code: {}, msg: {}".format(self.error_code.name, self.msg)

    def __repr__(self) -> str:
        return self.__str__()


OK: Status = Status(codes.ErrorCode.OK, "ok")
START_FAILED: Status = Status(codes.ErrorCode.START_FAILED, "failed to start database engine")
STOP_FAILED: Status = Status(codes.ErrorCode.STOP_FAILED, "failed to stop database engine")

CALCULATE_PARAM_CNT_ERROR: Status = Status(codes.ErrorCode.CALCULATE_PARAM_CNT_ERROR, "mismatch of param count of the operator")
CALCULATE_PARAM_TYPE_ERROR: Status = Status(codes.ErrorCode.CALCULATE_PARAM_TYPE_ERROR, "mismatch of param type of the operator")

DUPLICATED_TABLE_CREATION_REQUEST: Status = Status(codes.ErrorCode.DUPLICATED_TABLE_CREATION_REQUEST, "failed to create a table which is already existed")

INVALID_ARGUMENT: Status = Status(codes.ErrorCode.INVALID_ARGUMENT, "invalid argument")
FILE_NOT_EXIST: Status = Status(codes.ErrorCode.FILE_NOT_EXIST, "file doesn't exist")
FILE_EXIST: Status = Status(codes.ErrorCode.FILE_EXIST, "file is already existed")
UNKNOWN: Status = Status(codes.ErrorCode.UNKNOWN, "unknown")
UNEXPECTED_EMPTY: Status = Status(codes.ErrorCode.UNEXPECTED_EMPTY, "should not be empty")
UNSUPPORTED: Status = Status(codes.ErrorCode.UNSUPPORTED, "unsupported")
INCONSISTENT: Status = Status(codes.ErrorCode.INCONSISTENT, "inconsistent")
INTERNAL: Status = Status(codes.ErrorCode.INTERNAL, "internal error")
NOT_IMPLEMENTED: Status = Status(codes.ErrorCode.NOT_IMPLEMENTED, "not implemented")
TYPE_ERROR: Status = Status(codes.ErrorCode.TYPE_ERROR, "type error")

if __name__ == "__main__":
    print(CALCULATE_PARAM_TYPE_ERROR)
