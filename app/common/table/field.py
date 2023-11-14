import constant


class FieldInfo:

    def __init__(self, name: str, value_type: str):
        self.name = name
        self.value_type = value_type

    def get_name(self) -> str:
        return self.name

    def get_value_type(self) -> str:
        return self.value_type

    def __str__(self) -> str:
        return "name: {}, type: {}".format(self.get_name(), self.get_value_type())

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other: 'FieldInfo') -> bool:
        return self.name == other.name and self.value_type == other.value_type

    def __ne__(self, other: 'FieldInfo') -> bool:
        return not self.__eq__(other)


def extract_column_name(field_name: str) -> str:
    if constant.QUERY_FIELD_REF_SYM in field_name:
        return field_name.split(constant.QUERY_FIELD_REF_SYM)[1]
    return field_name
