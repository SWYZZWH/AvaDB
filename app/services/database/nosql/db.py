import json
import os
import tempfile

import constant

from app.common.query.result import QueryResult
from app.common.table.table import Table
from app.services.database.interface import DBInterface
from app.common.context.context import Context
from app.common.error.status import Status, OK, START_FAILED, INTERNAL, INVALID_ARGUMENT


def merge_json_files(data_dir: str, output_file_path: str):
    with open(output_file_path, 'w') as output_file:
        for file_name in os.listdir(data_dir):
            with open(os.path.join(data_dir, file_name), 'r') as input_file:
                json_list = json.load(input_file)
                for obj in json_list:
                    json.dump(obj, output_file, indent=4)
                    output_file.write('\n')


def format_output(table: Table, tables_dir: str) -> str:
    table_path = os.path.join(tables_dir, table.name)
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', suffix=".json")
    merge_json_files(table_path, temp_file.name)
    return temp_file.name


class DB(DBInterface):

    def __init__(self):
        self.ctx: Context = None
        self.started = False
        self.logger = None
        self.cfg = None

    def start(self, ctx: Context) -> Status:
        if self.started:
            return START_FAILED
        self.ctx = ctx
        self.logger = self.ctx.logger
        self.cfg = self.ctx.cfg
        self.started = True
        return OK

    def on_query(self, query: str) -> (QueryResult | None, Status):
        if not self.started:
            return None, START_FAILED
        table, status = self.ctx.qe.run(query)
        if not status.ok():
            return None, INTERNAL

        return QueryResult(format_output(table, self.ctx.cfg.get_tables_dir())), OK

    def on_insert(self, query_str: str) -> Status:
        pass
