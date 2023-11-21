import csv

import json
import os
import sys

import requests

import config


def detect_column_info_from_csv(file_path):
    res = []
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

        if len(rows) < 2:
            raise ValueError("CSV file must have at least two rows")

        field_names = rows[0]
        second_row = rows[1]

        for i, item in enumerate(second_row):
            field_info = {field_names[i]: 'str'}
            if item.replace('.', '', 1).isdigit():
                # if '.' in item:
                field_info[field_names[i]] = 'float'
                # else:
                #     field_info[field_names[i]] = 'int'
            elif item in ['True', 'False']:
                field_info[field_names[i]] = 'bool'

            res.append(field_info)

    return res


def get_records_from_csv(file_path, field_info):
    res = []
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)[1:]
        for row in data:
            record = {}
            for i, item in enumerate(row):
                field_name, field_type = list(field_info[i].keys())[0], list(field_info[i].values())[0]
                record[field_name] = eval(field_type)(item if item != "" and item != "N/A" else config.default_field_type_value[field_type])
            res.append(record)
    return res


usage = "usage: python3 import_data.py ${file_path}, only support csv or json"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(usage)
        exit(1)
    file_path = sys.argv[1]
    file_dir, file_name = os.path.dirname(file_path), os.path.basename(file_path)
    table_name, file_type = os.path.splitext(file_name)[0], os.path.splitext(file_name)[1]
    field_info = []
    records = []

    base_url = "http://localhost:{}"
    if file_type == ".csv":
        base_url = base_url.format("9528")
        print("csv file detected, will be inserted into sql database")
        field_info = detect_column_info_from_csv(file_path)
        records = get_records_from_csv(file_path, field_info)
    elif file_type == ".json":
        base_url = base_url.format("9527")
        with open(file_path) as f:
            records = json.load(f)
        print("json file detected, will be inserted into nosql database")
    else:
        print(usage)
        exit(1)

    drop_url = "/".join([base_url, "drop"])
    create_url = "/".join([base_url, "create"])
    insert_url = "/".join([base_url, "insert"])

    print("dropping current table: {}".format(table_name))
    response = requests.get(drop_url, json=json.dumps({"table_name": table_name}))
    print(response.text)
    print("creating new table: {}, field_info: {}".format(table_name, field_info))
    requests.get(create_url, json=json.dumps({"table_name": table_name, "fields": field_info}))
    print(response.text)
    print("inserting #{} records into table {}".format(len(records), table_name))
    requests.get(insert_url, json=json.dumps({"table_name": table_name, "records": records}))
    print(response.text)
