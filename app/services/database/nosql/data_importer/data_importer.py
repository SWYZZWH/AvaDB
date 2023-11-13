import os
import csv
import json
import sys

import constant
import config


def detect_type(value):
    try:
        float(value)
        return 'float' if '.' in value else 'int'
    except ValueError:
        return 'str'


def process_csv(file_path):
    database_name = os.path.splitext(os.path.basename(file_path))[0]
    table_path = os.path.join(config.config_map[constant.DB_TYPE_NOSQL].get_tables_dir(), database_name)
    metadata_path = os.path.join(config.config_map[constant.DB_TYPE_NOSQL].get_metadata_dir(), database_name + '.json')

    if os.path.exists(table_path):
        print(f"Table directory {table_path} already exists. Please delete it manually.")
        return

    if os.path.exists(metadata_path):
        print(f"Metadata file {metadata_path} already exists. Please delete it manually.")
        return

    os.makedirs(table_path)
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

    with open(file_path, newline='') as csv_file:
        reader = csv.reader(csv_file)
        columns = next(reader)

        for row in reader:
            if not row:
                continue

            data = {columns[i]: row[i] for i in range(len(row))}
            json_file = os.path.join(table_path, f"{row[0]}.json")

            with open(json_file, 'w') as f:
                json.dump(data, f, indent=4)

    metadata = {constant.METADATA_TABLE_NAME_KEY: database_name, constant.METADATA_FIELDS_KEY: {}}
    with open(file_path, newline='') as csv_file:
        reader = csv.reader(csv_file)
        columns = next(reader)

        # determine the data type for each column
        sample_row = next(reader, [])
        for col in columns:
            metadata[constant.METADATA_FIELDS_KEY][col] = detect_type(sample_row[columns.index(col)])

        # Save metadata
        with open(metadata_path, 'w') as meta_file:
            json.dump(metadata, meta_file, indent=4)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python data_importer.py ${csv_location}")
        exit(1)

    process_csv(sys.argv[1])
