from app.common.table.metadata import Metadata


def row_to_object(row: list[object], metadata: Metadata) -> dict[str: object]:
    res = {}
    for i, field_value in enumerate(row):
        field_name = metadata.get_all_field_names()[i]
        field_type = metadata.get_field_type(field_name)
        res[field_name] = eval(field_type)(field_value)
    return res


def object_to_row(obj: dict[str: object], metadata: Metadata):
    return [obj[field_name] for field_name in metadata.get_all_field_names()]
