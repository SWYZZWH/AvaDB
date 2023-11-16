import json
import os
import tempfile

import constant

from app.common.error.status import Status, INVALID_ARGUMENT, OK
from app.common.table.selector import Selector
from app.common.table.table import Table
from app.services.database.interface import DBInterface
from app.services.database.nosql.converter.converter import NestedJsonConverter


class DB(DBInterface):

    def merge_files(self, data_dir: str, output_file_path: str):
        with open(output_file_path, 'w') as output_file:
            for file_name in os.listdir(data_dir):
                with open(os.path.join(data_dir, file_name), 'r') as input_file:
                    json_list = json.load(input_file)
                    for obj in json_list:
                        json.dump(NestedJsonConverter.nest_to_json_obj(obj), output_file, indent=4)
                        output_file.write('\n')

    def format_output(self, table: Table, tables_dir: str) -> str:
        table_path = os.path.join(tables_dir, table.name)
        temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', suffix=".json")
        self.merge_files(table_path, temp_file.name)
        return temp_file.name

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

        return table.insert_bulk([NestedJsonConverter.flatten_json_obj(record) for record in records])

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
        return table.update(Selector(expr), NestedJsonConverter.flatten_json_obj(record))
