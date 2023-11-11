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


OK: Status = Status(codes.ErrorCode.OK)
START_FAILED: Status = Status(codes.ErrorCode.OK, "failed to start database engine")
STOP_FAILED: Status = Status(codes.ErrorCode.OK, "failed to stop database engine")
