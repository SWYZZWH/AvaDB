import operator

import constant
from app.common.error.status import Status, OK, CALCULATE_PARAM_CNT_ERROR, CALCULATE_PARAM_TYPE_ERROR


class Operator:

    def __init__(self, op_name: str, func):
        self.op_name = op_name
        self.func = func

    def get_param_cnt(self):
        return 0

    def get_name(self):
        return self.op_name

    def calculate(self, *args: any) -> (any, Status):
        # always fail for base class
        if self.get_param_cnt() != len(args):
            return 0, CALCULATE_PARAM_CNT_ERROR
        return self.func(*args), OK


class UniOperator(Operator):

    def get_param_cnt(self):
        return 1


class BiOperator(Operator):

    def get_param_cnt(self):
        return 2


class ArithOperator(Operator):

    def calculate(self, *args: int | float) -> (int | float, Status):
        if self.get_param_cnt() != len(args):
            return 0, CALCULATE_PARAM_CNT_ERROR
        for arg in args:
            if type(arg) is not int and type(arg) is not float:
                return 0, CALCULATE_PARAM_TYPE_ERROR
        return self.func(*args), OK


class LogicOperator(Operator):

    def calculate(self, *args: bool) -> (bool, Status):
        if self.get_param_cnt() != len(args):
            return 0, CALCULATE_PARAM_CNT_ERROR
        for arg in args:
            if type(arg) is not bool:
                return 0, CALCULATE_PARAM_TYPE_ERROR
        return self.func(*args), OK


class ComparisonOperator(Operator):

    def calculate(self, *args: bool) -> (bool, Status):
        if self.get_param_cnt() != len(args):
            return 0, CALCULATE_PARAM_CNT_ERROR
        if not all(isinstance(arg, type(args[0])) for arg in args):
            return 0, CALCULATE_PARAM_TYPE_ERROR

        return self.func(*args), OK


class ArithBiOperator(BiOperator, ArithOperator):
    pass


class LogicUniOperator(UniOperator, LogicOperator):
    pass


class LogicBiOperator(BiOperator):
    pass


class EqualityBiOperator(BiOperator):

    def calculate(self, *args: bool) -> (bool, Status):
        if self.get_param_cnt() != len(args):
            return 0, CALCULATE_PARAM_CNT_ERROR

        # 1 should not be considered the same as True
        # even if it's True in Python
        if not isinstance(args[0], type(args[1])):
            if self.get_name() == constant.OP_NAME_NE:
                return True, OK
            return 0, CALCULATE_PARAM_TYPE_ERROR  # for EQ

        return self.func(*args), OK


class ComparisonBiOperator(BiOperator, ComparisonOperator):
    pass


operator_map = {
    constant.OP_NAME_NOT: LogicUniOperator(constant.OP_NAME_NOT, operator.not_),
    constant.OP_NAME_AND: LogicBiOperator(constant.OP_NAME_AND, operator.and_),
    constant.OP_NAME_OR: LogicBiOperator(constant.OP_NAME_OR, operator.or_),

    constant.OP_NAME_LT: ComparisonBiOperator(constant.OP_NAME_LT, operator.lt),
    constant.OP_NAME_LE: ComparisonBiOperator(constant.OP_NAME_LE, operator.le),
    constant.OP_NAME_GT: ComparisonBiOperator(constant.OP_NAME_GT, operator.gt),
    constant.OP_NAME_GE: ComparisonBiOperator(constant.OP_NAME_GE, operator.ge),

    constant.OP_NAME_EQ: EqualityBiOperator(constant.OP_NAME_EQ, operator.eq),
    constant.OP_NAME_NE: EqualityBiOperator(constant.OP_NAME_NE, operator.ne),

    constant.OP_NAME_ADD: ArithBiOperator(constant.OP_NAME_ADD, operator.add),
    constant.OP_NAME_SUB: ArithBiOperator(constant.OP_NAME_SUB, operator.sub),
    constant.OP_NAME_MUL: ArithBiOperator(constant.OP_NAME_DIV, operator.mul),
    constant.OP_NAME_DIV: ArithBiOperator(constant.OP_NAME_DIV, operator.truediv),
}

if __name__ == "__main__":
    assert operator_map["!"].calculate(False)[0]
    assert not operator_map["!"].calculate(False, True)[1].ok()
    assert not operator_map["!"].calculate(1)[1].ok()

    assert operator_map["&&"].calculate(True, False)[1].ok()
    assert not operator_map["&&"].calculate(True, False)[0]
    assert not operator_map["&&"].calculate(True)[1].ok()

    assert operator_map["<"].calculate(1, 2)[1].ok()
    assert operator_map["<"].calculate(1, 2)[0]
    assert not operator_map["<"].calculate(1, 2, 3)[1].ok()
    assert not operator_map["<"].calculate(1)[1].ok()
    assert not operator_map["<"].calculate("1", 1)[1].ok()
    assert not operator_map["<"].calculate(1, 1)[0]

    assert operator_map[">"].calculate(1, 2)[1].ok()
    assert operator_map[">"].calculate(2, 1)[0]
    assert not operator_map[">"].calculate(1, 2, 3)[1].ok()
    assert not operator_map[">"].calculate(1)[1].ok()
    assert not operator_map[">"].calculate("1", 1)[1].ok()
    assert not operator_map[">"].calculate(1, 1)[0]

    assert not operator_map["=="].calculate(1, True)[0]
    assert operator_map["!="].calculate(1, True)[0]

    assert operator_map["*"].calculate(1, 2)[0] == 2
    assert operator_map["/"].calculate(1, 2)[0] == 0.5
    assert not operator_map["/"].calculate(True, 2)[1].ok()
    assert not operator_map["/"].calculate(2)[1].ok()
