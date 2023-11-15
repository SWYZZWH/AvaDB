import json

import config
import logger as logger

import constant

from app.common.context.context import Context
from app.common.error.status import Status, INVALID_ARGUMENT, NOT_IMPLEMENTED, OK
from app.common.table.field import FieldNameProcessor
from app.common.table.manipulator import TableManipulator, JoinOption, SortOption, GroupByOption, ReduceOption, ReduceOperation
from app.common.table.metadata import Metadata
from app.common.table.selector import Selector, AlwaysTrueSelector
from app.common.table.table import Table
from app.common.table.table_manager import get_table_manager
from app.services.database.sql.db_factory import DBFactory as SQLDBFactory


class QueryEngine:
    """
    query will be parsed by dfs
    src_tables will be calculated first
    TableManipulator will be used for query operations including: filtering, projection, ..., group by
    query result will be a single table (may be empty)
    """

    def __init__(self, ctx: Context):
        self.cfg = ctx.get_cfg()
        self.logger = ctx.get_logger()
        self.tm = ctx.get_table_manager()
        self.prefix_map = {}

    def handle_src_table(self, q: dict) -> (Table | None, Status):
        if constant.QUERY_SRC_TABLE_KEY not in q:
            self.logger.error("at least one src table should be offered in query {}".format(q))
            return None, INVALID_ARGUMENT

        src_table = q[constant.QUERY_SRC_TABLE_KEY]
        if type(src_table) is str:
            table = self.tm.get_table(src_table)
            if table is None:
                self.logger.error("unable to join not existed table {}, should create it first".format(src_table))
                return None, INVALID_ARGUMENT
            return table, OK

        if constant.QUERY_JOIN_TABLE_LEFT_KEY not in src_table and constant.QUERY_SRC_TABLE_KEY not in src_table:
            self.logger.error("invalid subquery format in query {}".format(q))
            return None, INVALID_ARGUMENT

        if constant.QUERY_JOIN_TABLE_LEFT_KEY in src_table:
            left_table, right_table = src_table[constant.QUERY_JOIN_TABLE_LEFT_KEY], src_table[constant.QUERY_JOIN_TABLE_RIGHT_KEY]
            self.logger.info("multiple src tables are detected, try to join them: {}".format([left_table, right_table]))
            l, r = self.tm.get_table(left_table), self.tm.get_table(right_table)
            res: Table = TableManipulator.join(l, r, Selector(src_table[constant.QUERY_JOIN_CONDITION_KEY]), src_table[constant.QUERY_JOIN_TYPE_KEY])
            l_info, r_info = res.metadata.get_all_field_names()[0], res.metadata.get_all_field_names()[-1]
            self.prefix_map["0"], self.prefix_map["1"] = FieldNameProcessor.get_prefix(l_info), FieldNameProcessor.get_prefix(r_info)
            return res, OK

        table, status = self.handle_sub_query(src_table)
        if not status.ok():
            self.logger.error("unable to join due to failure to handle sub query {}".format(src_table))
            return None, status
        return table, OK

    def handle_sub_query(self, sub_query: dict) -> (Table | None, Status):
        table, status = self.handle_query(sub_query)
        if not status.ok():
            return table, status

        # append table name to each field name
        new_table = TableManipulator.rename_fields(table, {name: FieldNameProcessor.add_prefix(name, table.name) for name in table.metadata.get_all_field_names()})
        return new_table, OK

    def parse_query_str(self, query_str: str) -> (dict, Status):
        try:
            q = json.loads(query_str)
        except Exception as e:
            self.logger.error("failed to pares query {} as json".format(query_str))
            return {}, INVALID_ARGUMENT
        return q, OK

    def handle_query(self, q: dict) -> (Table | None, Status):
        # 1. handle src table, may contain subquery and joining
        src_table, status = self.handle_src_table(q)
        if not status.ok():
            self.logger.error("failed to handle query {} due to failed to parse src_tables".format(q))
            return None, INVALID_ARGUMENT

        res_table = src_table
        # 2. handle group by
        if constant.QUERY_GROUP_BY in q:
            # only support group by one column now
            # TODO remove this check
            columns = q[constant.QUERY_GROUP_BY]
            if len(columns) > 1:
                self.logger.error("failed to handle more than one group by in query {}!".format(q))
                return None, NOT_IMPLEMENTED
            reduce_options = []
            for column in q[constant.QUERY_DESIRED_COLUMNS_KEY]:
                if FieldNameProcessor.get_suffix(column) != "":
                    reduce_options.append(ReduceOption(column, ReduceOperation[FieldNameProcessor.get_suffix(column)]))
            self.logger.info("grouping by table {}".format(res_table.name))
            res_table = TableManipulator.group_by(res_table, GroupByOption(columns[0], reduce_options))
            self.logger.info("group by on table {} is finished".format(res_table.name))

        # 3. filter data
        if constant.QUERY_FILTER_KEY in q:
            expression = q[constant.QUERY_FILTER_KEY]
            self.logger.info("filtering table {}".format(res_table.name))
            res_table = TableManipulator.filter(res_table, Selector(expression))
            self.logger.info("table is filtered, new_table is {}".format(res_table.name))

        # 4. sorting
        if constant.QUERY_ORDER_BY_KEY in q:
            sort_options: list[dict] = q[constant.QUERY_ORDER_BY_KEY]
            # only support order by one column now
            # TODO remove this check
            if len(sort_options) > 1:
                self.logger.error("doesn't support order by 2 columns".format(q[constant.QUERY_ORDER_BY_KEY]))
                return None, NOT_IMPLEMENTED
            self.logger.info("sorting table {}".format(res_table.name))
            res_table = TableManipulator.sort(res_table, [
                SortOption(
                    option.get(constant.QUERY_ORDER_BY_COLUMN_KEY),
                    option.get(constant.QUERY_ORDER_BY_ASC_KEY)) if constant.QUERY_ORDER_BY_ASC_KEY in option else True  # default is asc
                for option in sort_options
            ])
            self.logger.info("table is sorted, new table: {}".format(res_table.name))

        # 5. projection
        # keep all columns by default
        if constant.QUERY_DESIRED_COLUMNS_KEY in q:
            columns = q[constant.QUERY_DESIRED_COLUMNS_KEY]
            modified_columns = []
            for column in columns:
                if FieldNameProcessor.get_inner_prefix(column) in self.prefix_map:
                    modified_columns.append(FieldNameProcessor.replace_inner_prefix(column, self.prefix_map[FieldNameProcessor.get_inner_prefix(column)]))
                else:
                    modified_columns.append(column)
            if len(modified_columns) == 0:
                self.logger.warn("empty table due to empty desired_columns field in query: {} ".format(q))
                return None, INVALID_ARGUMENT
            self.logger.info("projecting table {} to columns: {}".format(res_table.name, modified_columns))
            res_table = TableManipulator.projection(res_table, modified_columns)
            self.logger.info("table is projected to columns: {}, new table is {}".format(modified_columns, res_table))

        return res_table, OK

    def run(self, query: str) -> (Table | None, Status):
        q, status = self.parse_query_str(query)
        if not status.ok():
            return None, INVALID_ARGUMENT

        return self.handle_query(q)


if __name__ == "__main__":
    logger, cfg = logger.get_logger(constant.DB_TYPE_SQL), config.config_map[constant.DB_TYPE_SQL]
    ctx = Context(logger, cfg, SQLDBFactory.instance())
    tm = get_table_manager(ctx)
    ctx.set_table_manager(tm)
    status = tm.start()
    assert status.ok()
    qe = QueryEngine(ctx)

    table_name_1 = "test_query_engine_select"
    tm.drop_table(table_name_1)
    table1, status = tm.create_table(table_name_1, Metadata(table_name_1, constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "str"}]))
    assert status.ok()
    table1.insert({"col1": 1, "col2": "a"})
    table1.insert({"col1": 3, "col2": "a"})
    table1.insert({"col1": 1, "col2": "b"})
    table1.insert({"col1": 3, "col2": "b"})

    query = {
        "src_table": "test_query_engine_select",
        "row_filter": {
            "op": "&&",
            "v1": {
                "op": ">",
                "v1": "0::col1",
                "v2": 1
            },
            "v2": {
                "op": "==",
                "v1": "0::col2",
                "v2": "b"
            }
        }
    }
    t, status = qe.run(json.dumps(query))
    assert status.ok()
    chunk, status = t.chunk_manager.get_fist_chunk()
    assert status.ok()
    assert chunk[0] == {"col1": 3, "col2": "b"}

    table_name_1 = "test_query_engine_filter"
    tm.drop_table(table_name_1)
    table1, status = tm.create_table(table_name_1, Metadata(table_name_1, constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "str"}]))
    assert status.ok()
    table1.insert({"col1": 1, "col2": "a"})
    table1.insert({"col1": 3, "col2": "a"})
    table1.insert({"col1": 1, "col2": "b"})
    table1.insert({"col1": 3, "col2": "b"})

    query = {
        "src_table": "test_query_engine_filter",
        "desired_columns": ["::col2"]
    }

    t, status = qe.run(json.dumps(query))
    assert status.ok()
    chunk, status = t.chunk_manager.get_fist_chunk()
    assert status.ok()
    assert chunk == [{"col2": "a"}, {"col2": "a"}, {"col2": "b"}, {"col2": "b"}]

    table_name_1 = "test_query_engine_group_by"
    tm.drop_table(table_name_1)
    table1, status = tm.create_table(table_name_1, Metadata(table_name_1, constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "str"}]))
    assert status.ok()
    table1.insert({"col1": 1, "col2": "a"})
    table1.insert({"col1": 3, "col2": "a"})
    table1.insert({"col1": 1, "col2": "b"})
    table1.insert({"col1": 3, "col2": "b"})

    query = {
        "src_table": "test_query_engine_filter",
        "desired_columns": ["::col2"]
    }

    t, status = qe.run(json.dumps(query))
    assert status.ok()
    chunk, status = t.chunk_manager.get_fist_chunk()
    assert status.ok()
    assert chunk == [{"col2": "a"}, {"col2": "a"}, {"col2": "b"}, {"col2": "b"}]
