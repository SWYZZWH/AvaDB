import csv
import json
import os
import tempfile

import constant

from app.common.context.context import Context
from app.common.error.status import Status, START_FAILED, OK, INTERNAL, INVALID_ARGUMENT
from app.common.query.result import QueryResult
from app.common.table.metadata import load_from_json
from app.common.table.selector import Selector
from app.common.table.table import Table


class DBInterface:

    def __init__(self):
        self.ctx: Context = None
        self.started = False
        self.logger = None
        self.cfg = None

    def start(self, ctx) -> Status:
        if self.started:
            return START_FAILED
        self.ctx = ctx
        self.logger = self.ctx.logger
        self.cfg = self.ctx.cfg
        self.started = True
        return OK

    # query is a json string
    def on_query(self, query: str) -> (QueryResult | None, Status):
        if not self.started:
            return None, START_FAILED

        table, status = self.ctx.qe.run(query)
        if not status.ok():
            return None, INTERNAL

        return QueryResult(self.format_output(table, self.ctx.cfg.get_tables_dir())), OK

    def on_insert(self, query_str: str) -> Status:
        query = json.loads(query_str)

        if constant.INSERT_TABLE_NAME_KEY not in query:
            self.logger.error("invalid insertion request: {}".format(query))
            return INVALID_ARGUMENT

        if constant.INSERT_RECORDS_KEY not in query or len(query[constant.INSERT_RECORDS_KEY]) == 0:
            self.logger.warn("empty insertion by query {}".format(query))
            return OK

        table_name, records = query[constant.INSERT_TABLE_NAME_KEY], query[constant.INSERT_RECORDS_KEY]
        table = self.ctx.table_manager.get_table(table_name)
        if table is None:
            self.logger.error("unable to find table {}, query {}".format(table_name, query))
            return INVALID_ARGUMENT

        return table.insert_bulk(records)

    def on_update(self, query_str: str) -> Status:
        query = json.loads(query_str)
        if constant.UPDATE_TABLE_NAME_KEY not in query or constant.UPDATE_EXPR_KEY not in query or constant.UPDATE_NEW_RECORD_KEY not in query:
            self.logger.error("missing necessary params in query {} ".format(query))
            return INVALID_ARGUMENT
        table_name, expr, record = query[constant.UPDATE_TABLE_NAME_KEY], query[constant.UPDATE_EXPR_KEY], query[constant.UPDATE_NEW_RECORD_KEY]
        table = self.ctx.get_table_manager().get_table(table_name)
        if table is None:
            self.logger.error("try to update on not existed table {}".format(table_name))
            return INVALID_ARGUMENT
        return table.update(Selector(expr), record)

    def on_delete(self, query_str: str) -> Status:
        query = json.loads(query_str)
        if constant.DELETE_TABLE_NAME_KEY not in query or constant.DELETE_EXPR_KEY not in query:
            self.logger.error("missing necessary params in query {} ".format(query))
            return INVALID_ARGUMENT
        table_name, expr = query[constant.DELETE_TABLE_NAME_KEY], query[constant.DELETE_EXPR_KEY]
        table = self.ctx.get_table_manager().get_table(table_name)
        if table is None:
            self.logger.error("try to delete on not existed table {}".format(table_name))
            return INVALID_ARGUMENT
        return table.delete(Selector(expr))

    def on_drop(self, query_str: str) -> Status:
        query = json.loads(query_str)
        if constant.DROP_TABLE_NAME_KEY not in query:
            self.logger.error("missing necessary params in query {} ".format(query))
        table_name = query[constant.DROP_TABLE_NAME_KEY]
        self.logger.warn("try to drop table {}".format(table_name))
        status = self.ctx.table_manager.drop_table(table_name)
        if not status.ok():
            self.logger.info("failed to drop table {}".format(table_name))
            return status
        self.logger.info("table {} is dropped".format(table_name))
        return OK

    def on_create(self, query_str: str) -> Status:
        query = json.loads(query_str)

        if constant.CREATE_TABLE_NAME_KEY not in query:
            self.logger.warn("failed to create table, query {}".format(query))
            return INVALID_ARGUMENT

        table_name = query[constant.CREATE_TABLE_NAME_KEY]
        metadata_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', suffix=".json")
        json.dump(query, metadata_file)
        metadata_file.close()
        metadata, status = load_from_json(metadata_file.name, self.ctx)
        if not status.ok():
            self.logger.error("failed to load metadata from json, msg {}, q {}".format(status.msg, query))
            return INVALID_ARGUMENT
        table, status = self.ctx.get_table_manager().create_table(table_name, metadata)
        if not status.ok():
            self.logger.error("failed to register table {} to table manager, q {}".format(table.name, query))
            return INTERNAL
        self.logger.info("table {} is successfully created by q {}".format(table.name, query))
        return OK

    def format_output(self, table: Table, tables_dir: str) -> str:
        return ""

    def merge_files(self, data_dir: str, output_file_path: str):
        return

    def format_input(self, query_str: str) -> str:
        return ""


class DBFactoryInterface:

    @classmethod
    def instance(cls) -> DBInterface:
        pass
