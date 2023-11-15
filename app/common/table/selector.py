from app.common.error.status import Status, INTERNAL, INVALID_ARGUMENT, TYPE_ERROR
from app.common.query.expression_tree import ExprTree


class Selector:

    def __init__(self, expression: any):
        """
        return True only when the expression is True
        use relative prefix
        :param expression: expression object in json format
        """
        self.expression = expression
        self.expr_tree = ExprTree(expression)

    # all entries used in the expression should be offered.
    def is_match(self, entries: list) -> (bool, Status):
        if not self.expr_tree.is_valid():
            return False, INVALID_ARGUMENT

        res, status = self.expr_tree.valuate(entries)
        if type(res) is not bool:
            return False, TYPE_ERROR
        return res, status
