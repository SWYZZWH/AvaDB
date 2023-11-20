import csv
import json

import json
import urllib

import requests
import config
import constant
import logger
import tempfile

logger = logger.get_logger(constant.CLI_NAME)


def print_file(file_name: str):
    with open(file_name, 'r', encoding='utf-8') as file:
        line = file.readline()
        while line:
            print(line, end='')
            line = file.readline()
        print("\n")


supported_apis = [constant.REQUEST_KEY_QUERY, constant.REQUEST_KEY_CREATE, constant.REQUEST_KEY_DROP, constant.REQUEST_KEY_DELETE, constant.REQUEST_KEY_UPDATE, constant.REQUEST_KEY_INSERT]


def is_request_valid(request_json) -> bool:
    if request_json.get(constant.REQUEST_KEY_TYPE) is None:
        print("Please specify the type of database with: {{'{}': ${{database_type}}}}, supported values are: {}".format(constant.REQUEST_KEY_TYPE, config.supported_database_types))
        return False

    if request_json.get(constant.REQUEST_KEY_TYPE) not in [constant.DB_TYPE_SQL, constant.DB_TYPE_NOSQL]:
        print("Please specify the correct type of database with: {{'{}': ${{database_type}}}}, supported values are: {}".format(constant.REQUEST_KEY_TYPE, config.supported_database_types))
        return False

    if all(api not in request_json for api in supported_apis):
        print("Please offer the query: {{'{}': ${{database_type}}}}, supported values are: {}".format("method", config.supported_database_types))
        return False

    return True


def parse_json_input(user_input: str):
    try:
        request_json = json.loads(user_input)
        if not is_request_valid(request_json):
            return None
        return request_json
    except Exception as e:
        logger.error("Invalid json format: {}".format(user_input))
        print("Please offer a valid json object as request with the format: {{'{}': ${{database_type}}, '{}': ${{query}} }}"
              .format(constant.REQUEST_KEY_TYPE, constant.REQUEST_KEY_QUERY))
        return None


def get_cfg(request_json) -> config.DBConfig | None:
    if not is_request_valid(request_json):
        return None
    return config.sql_cfg if request_json.get(constant.REQUEST_KEY_TYPE) == constant.DB_TYPE_SQL else config.nosql_cfg


def send_bulk_insert_commands(commands):
    base_url = "http://localhost:9527/insert"
    for cmd in commands:
        try:

            encoded_json = urllib.parse.quote(json.dumps(cmd))
            full_url = f"{base_url}"
            response = requests.get(full_url, json=encoded_json)

            if response.status_code == 200:
                print("Command executed successfully:", response.text)
            else:
                print("Failed to execute command:", response.text)
        except Exception as e:
            logger.error(f"Error sending command to {full_url}: {e}")


# CSV 文件路径

def test(commands):
    base_url = "http://localhost:9527"
    drop_url = "/".join([base_url, "drop"])
    create_url = "/".join([base_url, "create"])
    insert_url = "/".join([base_url, "insert"])

    requests.get(drop_url, json=json.dumps({"table_name": "allsales"}))
    requests.get(create_url, json=json.dumps({"table_name": "allsales"}))
    response = requests.get(insert_url, json=json.dumps(commands))
    if response.status_code == 200:
        print("Command executed successfully:", response.text)
    else:
        print("Failed to execute command:", response.text)


csv_file_path = 'dataset/allSales.csv'


# 读取 CSV 文件并转换成 Python 字典列表
def read_csv_to_dict(csv_file_path):
    data = []
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            data.append(row)
    return data


# 从 CSV 读取数据
csv_data = read_csv_to_dict(csv_file_path)


# 将 Python 字典列表转换为目标 JSON 格式
def convert_data(data):
    new_records = []
    for item in data:
        new_record = {}
        for key, value in item.items():
            new_record[key] = value
        new_records.append(new_record)

    return {
        "type": "nosql",
        "insert": {
            "table_name": "allsales",
            "records": new_records
        }
    }


# 执行转换
converted_data = convert_data(csv_data)


def generate_nosql_commands(json_data):
    commands = []
    for record in json_data['insert']['records']:
        command = {
            "table_name": json_data['insert']['table_name'],
            "records": [record]
        }
        commands.append(command)
    return commands


# 生成命令
commands = generate_nosql_commands(converted_data)
# for cmd in commands:
#     print(cmd)
cmd = commands[0]
print(cmd)
test(cmd)
# send_bulk_insert_commands(commands)
# 打印命令或将它们保存到文件中
# for cmd in commands:
#     print(json.dumps(cmd, indent=4))

# def main():
#     send_bulk_insert_commands(commands)
#
# if __name__ == "__main__":
#     main()
