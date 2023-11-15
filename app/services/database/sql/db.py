import csv
import os
import tempfile

from app.common.query.result import QueryResult
from app.common.table.table import Table
from app.services.database.interface import DBInterface
from app.common.context.context import Context
from app.common.error.status import Status, OK, START_FAILED, INTERNAL


def merge_csv_files(data_dir: str, output_file_path: str):
    csv_files = os.listdir(data_dir)

    with open(output_file_path, "a") as output_file:
        writer = csv.writer(output_file)
        for file_name in csv_files:
            with open(os.path.join(data_dir, file_name), 'r', newline='') as file:
                reader = csv.reader(file)
                for row in reader:
                    writer.writerow(row)


def format_output(table: Table, tables_dir: str) -> str:
    table_path = os.path.join(tables_dir, table.name)
    output_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', suffix=".json")
    with open(output_file.name, "w") as f:
        writer = csv.writer(f)
        writer.writerow(table.metadata.get_all_field_names())
    merge_csv_files(table_path, output_file.name)
    return output_file.name


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
