import json
import requests
import config
import constant
import logger
import tempfile

logger = logger.get_logger()
supported_database_types = [constant.CONST_DB_TYPE_SQL, constant.CONST_DB_TYPE_NOSQL]


def download_file(url, params) -> str:
    try:
        logger.info("Try to download from url {} ...".format(url))
        with requests.get(url, params=params, stream=True) as r:
            logger.info("Receive response from url {}, status code: {}".format(url, r.status_code))
            r.raise_for_status()

            temp_file = tempfile.NamedTemporaryFile(mode='w+t', delete=False)
            logger.info("Tmp file {} is created to save file from {}".format(temp_file.name, url))
            for chunk in r.iter_content(chunk_size=constant.CONST_DOWNLOAD_CHUNK_SIZE):
                temp_file.write(chunk.decode('utf-8'))
                temp_file.flush()

            logger.info("Successfully download file from {} and save to {}".format(url, temp_file.name))
            temp_file.close()
            return temp_file.name
    except Exception as e:
        logger.error("Download file from {} failed, due to: {}".format(url, e))

    return ""


def print_file(file_name: str):
    with open(file_name, 'r', encoding='utf-8') as file:
        line = file.readline()
        while line:
            print(line, end='')
            line = file.readline()
        print("\n")


def is_request_valid(request_json) -> bool:
    if request_json.get(constant.CONST_REQUEST_KEY_TYPE) is None:
        print("Please specify the type of database with: {{'{}': ${{database_type}}}}, supported values are: {}".format(constant.CONST_REQUEST_KEY_TYPE, supported_database_types))
        return False

    if request_json.get(constant.CONST_REQUEST_KEY_TYPE) not in [constant.CONST_DB_TYPE_SQL, constant.CONST_DB_TYPE_NOSQL]:
        print("Please specify the correct type of database with: {{'{}': ${{database_type}}}}, supported values are: {}".format(constant.CONST_REQUEST_KEY_TYPE, supported_database_types))
        return False

    if request_json.get(constant.CONST_REQUEST_KEY_QUERY) is None:
        print("Please offer the query: {{'{}': ${{database_type}}}}, supported values are: {}".format(constant.CONST_REQUEST_KEY_QUERY, supported_database_types))
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
              .format(constant.CONST_REQUEST_KEY_TYPE, constant.CONST_REQUEST_KEY_QUERY))
        return None


def get_cfg(request_json):
    if not is_request_valid(request_json):
        return None
    return config.sql_cfg if request_json.get(constant.CONST_REQUEST_KEY_TYPE) == constant.CONST_DB_TYPE_SQL else config.nosql_cfg


def main():
    logger.info("User connects")
    print(constant.CONST_CLI_WELCOME)
    while True:
        user_input = input(constant.CONST_CLI_PROMPT)
        if user_input.lower() == 'exit':
            logger.info("User exits")
            break
        try:
            request_json = parse_json_input(user_input)
            if request_json is None:
                logger.error("Invalid user input: {}".format(user_input))
                continue
            cfg = get_cfg(request_json)
            tmp_file = download_file('http://localhost:{}'.format(cfg.get(constant.CONST_PORT)), request_json.get(constant.CONST_REQUEST_KEY_QUERY))
            if tmp_file == "":
                print("Get result from database failed")
                continue
            print_file(tmp_file)
        except Exception as e:
            logger.error("An unexpected error occurred: {}".format(e))
            continue


if __name__ == "__main__":
    main()
