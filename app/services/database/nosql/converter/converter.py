import json

import constant


class NestedJsonConverter:

    @staticmethod
    def flatten_json_str(json_str: str) -> dict:
        return NestedJsonConverter.flatten_json_obj(json.loads(json_str))

    @staticmethod
    def flatten_json_obj(json_obj: dict, parent_key: str = '') -> dict:
        """
        Flattens a nested JSON object (dictionary).

        :param json_obj: The JSON object to flatten.
        :param parent_key: The base key to use for the current level of depth.
        :return: Flattened dictionary.
        """
        items = []
        for k, v in json_obj.items():
            new_key = f"{parent_key}{constant.NOSQL_LEVEL_SEP}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(NestedJsonConverter.flatten_json_obj(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def nest_to_json_obj(record: dict) -> dict:
        """
        Converts a flat dictionary with compound keys into a nested JSON object.

        :param record: The flat dictionary to be converted into a nested JSON object.
        :return: Nested JSON object (dictionary).
        """
        result = {}
        for compound_key, value in record.items():
            parts = compound_key.split(constant.NOSQL_LEVEL_SEP)
            current_level = result
            for i, part in enumerate(parts):
                if i == len(parts) - 1:
                    current_level[part] = value
                else:
                    if part not in current_level:
                        current_level[part] = {}
                    current_level = current_level[part]
        return result

    @staticmethod
    def nest_to_json_str(record: dict) -> str:
        return json.dumps(NestedJsonConverter.nest_to_json_obj(record))


if __name__ == "__main__":
    flattened = NestedJsonConverter.flatten_json_str('{"a": {"b": {"c": "f"}}, "d": "e"}')
    print(flattened)
    print(NestedJsonConverter.nest_to_json_obj(flattened))
