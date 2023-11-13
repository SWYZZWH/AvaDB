import constant

from app.common.error.status import Status, INVALID_ARGUMENT, UNSUPPORTED, OK, NOT_IMPLEMENTED, INTERNAL, CALCULATE_PARAM_TYPE_ERROR
from app.common.query.operators import operator_map, Operator


class RuntimeRef:

    def __init__(self, idx: int, name: str):
        self.idx = idx
        self.name = name


class Node:

    # for sql and nosql, entries share the same format
    def valuate(self, entries: list[dict[str: object]]) -> (any, Status):
        return None, NOT_IMPLEMENTED


class ExprNode(Node):

    def __init__(self, operator: Operator):
        self.op = operator
        self.vals: list[Node] = []

    def append_child(self, node: Node):
        self.vals.append(node)

    def valuate(self, entries) -> (any, Status):
        vals = []
        for val in self.vals:
            res, status = val.valuate(entries)
            if not status.ok():
                return None, status
            vals.append(res)
        return self.op.valuate(*vals)


class LiteralNode(Node):

    def __init__(self, val):
        self.val = val

    def valuate(self, entries) -> (any, Status):
        return self.val, OK


class RuntimeRefNode(Node):

    def __init__(self, ref: RuntimeRef):
        self.ref = ref

    def valuate(self, entries) -> (any, Status):
        if self.ref.idx >= len(entries):
            return None, INVALID_ARGUMENT
        entry = entries[self.ref.idx]
        if self.ref.name not in entry:
            return None, INVALID_ARGUMENT
        return entry[self.ref.name], OK


# The complicated operation expression tree can be evaluated recursively
# e.g. for expression:
# {
#   op: "+",
#   val1: {
#       op: "*",
#       val1: 1,
#       val2: 2,
#   },
#   val2: 3
# }
# we could evaluate it by dfs
# val1 and val2 could be literals, reference to a column/property, or another expression tree
# TODO: print more logs
class ExprTree:

    def __init__(self, expression: any):
        self.expr = expression
        self.root, _ = self.build_node()
        print(self.expr)

    def is_valid(self) -> bool:
        return self.root is not None

    def check_table_ref(self, table_ref: str) -> bool:
        try:
            idx = int(table_ref)
            return idx >= 0
        except Exception as e:
            return False

    # a field could be:
    #  1. literal i.e. 123, "ABC", True, [1, 2, 3], None
    #  2. a reference to field (Notice, must use ${idx_in_src_tables}::${field_name} to reference a field in query!)
    def build_leaf(self, field: any) -> (Node, Status):
        if type(field) is not str or constant.QUERY_FIELD_REF_SYM not in field:
            return LiteralNode(field), OK

        field_components = field.split(constant.QUERY_FIELD_REF_SYM)
        if len(field_components) != 2:
            return None, INVALID_ARGUMENT

        table, field_name = field_components[0], field_components[1]
        if not self.check_table_ref(table):
            return None, INVALID_ARGUMENT

        return RuntimeRefNode(RuntimeRef(int(table), field_name)), OK

    # for SQL, it's possible to check validate the query when building reference tree, as we have all field infos in metadata
    # but for Nosql, it's impossible to know until runtime
    # for consistency between sql and nosql, don't validate the query here.
    def build_node(self) -> (Node, Status):
        if type(self.expr) is not dict:
            return self.build_leaf(self.expr)

        if constant.QUERY_OP_KEY not in self.expr:
            return None, INVALID_ARGUMENT

        op: Operator = self.expr[constant.QUERY_OP_KEY]
        if op not in operator_map:
            return None, UNSUPPORTED
        operator = operator_map[op]
        node = ExprNode(operator)

        val1_obj = self.expr[constant.QUERY_VAR1_KEY]
        # if val1_obj is None:
        #     return None, INVALID_ARGUMENT
        node1 = ExprTree(val1_obj)
        node.append_child(node1)

        if operator.get_param_cnt() == 2:
            val2_obj = self.expr[constant.QUERY_VAR2_KEY]
            # if val2_obj is None:
            #     return None, INVALID_ARGUMENT
            node2 = ExprTree(val2_obj).root
            node.append_child(node2)

        return node, OK

    def valuate(self, entries: list[dict[str: object]]) -> (any, Status):
        if self.root is None:
            return None, INTERNAL

        return self.root.valuate(entries)


if __name__ == "__main__":
    def test(expr, expected_value, expected_status, entries=None):
        print("\ntest parse {}:".format(expr))
        if entries is None:
            entries = []
        tree = ExprTree(expr)
        val, status = tree.valuate(entries)
        print(status)
        print(" actual value ", val)
        assert status == expected_status
        assert val == expected_value


    test(
        {
            "op": "+",
            "v1": "1",
            "v2": "2"
        },
        0,
        CALCULATE_PARAM_TYPE_ERROR
    )

    test(
        {
            "op": "+",
            "v1": 1,
            "v2": 2
        },
        3,
        OK
    )

    test(
        {
            "op": "*",
            "v1": 1,
            "v2": {
                "op": "-",
                "v1": 2,
                "v2": 3
            }
        },
        -1,
        OK
    )

    test(
        {
            "op": "!",
            "v1": False,
        },
        True,
        OK
    )

    test(
        {
            "op": "+",
            "v1": "0::a",
            "v2": "1::b",
        },
        3,
        OK,
        [
            {
                "a": 1
            },
            {
                "b": 2
            }
        ]
    )

    test(
        {
            "op": "!",
            "v1": {
                "op": "&&",
                "v1": {
                    "op": "!=",
                    "v1": None,
                    "v2": "0::a"
                },
                "v2": {
                    "op": "!=",
                    "v1": None,
                    "v2": "1::b"
                }
            },
        },
        False,
        OK,
        [
            {
                "a": 1
            },
            {
                "b": 2
            }
        ]
    )
