import csv
import os
import tempfile

from app.common.table.table import Table
from app.services.database.interface import DBInterface


class DB(DBInterface):

    def format_output(self, table: Table, tables_dir: str) -> str:
        table_path = os.path.join(tables_dir, table.name)
        output_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', suffix=".json")
        with open(output_file.name, "w") as f:
            writer = csv.writer(f)
            writer.writerow(table.metadata.get_all_field_names())
        self.merge_files(table_path, output_file.name)
        return output_file.name

    def merge_files(self, data_dir: str, output_file_path: str):
        csv_files = os.listdir(data_dir)

        with open(output_file_path, "a") as output_file:
            writer = csv.writer(output_file)
            for file_name in csv_files:
                with open(os.path.join(data_dir, file_name), 'r', newline='') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        writer.writerow(row)

    def format_input(self, query_str: str) -> str:
        return query_str
