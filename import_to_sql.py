import csv
import json

import json

import requests


def test(commands):
    base_url = "http://localhost:9528"
    insert_url = "/".join([base_url, "insert"])

    response = requests.get(insert_url, json=json.dumps(commands))
    print("*************************************")
    print("now running on : {}".format(commands))
    print("*************************************")
    if response.status_code == 200:
        print("Command executed successfully:", response.text)
    else:
        print("Failed to execute command:", response.text)


csv_file_path = 'dataset/xboxSales_new.csv'


# 读取 CSV 文件并转换成 Python 字典列表
def read_csv_to_dict(csv_file_path):
    data = []
    with open(csv_file_path, mode='r', encoding='ISO-8859-1') as file:
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
        "type": "sql",
        "insert": {
            "table_name": "xboxSales",
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


# {"type": "sql", "create": {"table_name": "allsales", "fields": [{"Name": "str"}, {"Platform": "str"}, {"Year_of_Release": "str"},{"Genre": "str"},{"Publisher": "str"},{"NA_Sales": "float"}，{"EU_Sales": "float"}，{"JP_Sales": "float"}，{"Other_Sales": "float"}，{"Global_Sales": "float"}，{"Critic_Score": "float"}，{"Critic_Count": "str"}，{"User_Score": "float"}，{"User_Count": "str"}，{"Developer": "str"}，{"Rating": "float"}]}}
base_url = "http://localhost:9528"
drop_url = "/".join([base_url, "drop"])
create_url = "/".join([base_url, "create"])
requests.get(drop_url, json=json.dumps({"table_name": "xboxSales"}))
requests.get(create_url, json=json.dumps({"table_name": "xboxSales", "fields": [{"Game": "str"}, {"Year": "str"}, {"Genre": "str"},{"Publisher": "str"},{"North America": "str"},{"Europe": "str"},{"Japan": "str"},{"Rest of World": "str"},{"Global": "str"}]}))
# 生成命令
commands = generate_nosql_commands(converted_data)
# for cmd in commands:
#     print(cmd)
for cmd in commands:
    # print(cmd)
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
