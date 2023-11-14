# meta data storing names and types of fields
import json
import os.path

import config
import constant
import logger
from app.common.context.context import Context

from app.common.error.status import Status, FILE_NOT_EXIST, UNKNOWN, UNEXPECTED_EMPTY, UNSUPPORTED, OK, FILE_EXIST, INCONSISTENT
from app.common.table.field import FieldInfo
from app.services.database.sql.db_factory import DBFactory as SQLDBFactory


# Metadata only works for sql
# For consistency, nosql also creates metadata files under ${project_roo}/metadata/nosql
class Metadata:

    def __init__(self, table_name: str, db_type: str, field_info: list[dict[str, str]]):
        self.fields: list[FieldInfo] = [FieldInfo(list(field.keys())[0], list(field.values())[0]) for field in field_info]
        # TODO add a self.fields_map to speed up
        self.db_type = db_type
        self.table_name = table_name

    # keep fields sequence
    def get_all_fields(self) -> list[FieldInfo]:
        return self.fields

    # keep fields sequence
    def get_all_field_names(self) -> list[str]:
        return [info.get_name() for info in self.get_all_fields()]

    def get_table_name(self) -> str:
        return self.table_name

    def get_field_info(self, name: str) -> FieldInfo | None:
        for info in self.fields:
            if info.get_name() == name:
                return info
        return None

    def is_field_existed(self, field_name: str) -> bool:
        return self.get_field_info(field_name) is not None

    def get_field_type(self, field_name: str) -> str | None:
        if not self.is_field_existed(field_name):
            return None
        return self.get_field_info(field_name).get_value_type()

    def __str__(self) -> str:
        return "table name: {}\tfields are: {}".format(self.get_table_name(), "\n".join([info.__str__() for info in self.fields]))

    def __repr__(self) -> str:
        return self.__str__()

    def to_json_obj(self):
        if self.db_type == constant.DB_TYPE_NOSQL:
            return {constant.METADATA_TABLE_NAME_KEY: self.table_name}
        return {constant.METADATA_TABLE_NAME_KEY: self.table_name, constant.METADATA_FIELDS_KEY: [{info.get_name(): info.get_value_type()} for info in self.fields]}


def load_from_json(file_path: str, ctx: Context) -> (Metadata | None, Status):
    if not os.path.exists(file_path):
        return None, FILE_NOT_EXIST

    supported_types = ctx.get_cfg().get_supported_field_types()
    table_name = os.path.splitext(os.path.basename(file_path))[0]

    db_type = ctx.get_cfg().get_db_type()
    with open(file_path, "r") as f:
        metadata = json.load(f)
        if constant.METADATA_TABLE_NAME_KEY not in metadata:
            return None, UNKNOWN
        if table_name != metadata[constant.METADATA_TABLE_NAME_KEY]:
            return None, INCONSISTENT
        if db_type == constant.DB_TYPE_NOSQL:
            return Metadata(metadata[constant.METADATA_TABLE_NAME_KEY], db_type, {})

        # only read field info for SQL
        # SQL table with 0 column is not allowed
        if constant.METADATA_FIELDS_KEY not in metadata or type(metadata[constant.METADATA_FIELDS_KEY]) is not list or len(metadata[constant.METADATA_FIELDS_KEY]) == 0:
            return None, UNEXPECTED_EMPTY
        # notice that the field info must be sorted, so we must store them as a list
        for field_info in metadata[constant.METADATA_FIELDS_KEY]:
            field_name, field_type = list(field_info.keys())[0], list(field_info.values())[0]
            if field_name == "":
                return None, UNEXPECTED_EMPTY
            if field_type not in supported_types:
                ctx.get_logger().error("unsupported type: {} for field {}".format(field_type, field_name))
                return None, UNSUPPORTED

        return Metadata(metadata[constant.METADATA_TABLE_NAME_KEY], db_type, metadata[constant.METADATA_FIELDS_KEY]), OK


def save_as_json(metadata: Metadata, ctx: Context) -> Status:
    meta_dir = ctx.get_cfg().get_metadata_dir()
    meta_file_path = os.path.join(meta_dir, metadata.table_name + ".json")

    if os.path.exists(meta_file_path):
        ctx.get_logger().error("try to override existed metadata file {}, which is not supposed to happen".format(meta_file_path))
        return FILE_EXIST

    os.makedirs(meta_dir, exist_ok=True)
    with open(meta_file_path, "w") as f:
        json.dump(metadata.to_json_obj(), f)

    return OK


if __name__ == "__main__":
    logger, cfg = logger.get_logger(constant.DB_TYPE_SQL), config.config_map[constant.DB_TYPE_SQL]
    ctx = Context(logger, cfg, SQLDBFactory.instance())

    print(ctx.get_cfg().get_metadata_dir() + "/test.json")
    metadata, status = load_from_json(ctx.get_cfg().get_metadata_dir() + "/test.json", ctx)
    print(status)
    assert status.ok()
    print(metadata)

    metadata.table_name = "test2"
    os.remove(ctx.get_cfg().get_metadata_dir() + "/test2.json")
    status = save_as_json(metadata, ctx)
    print(status)
    assert status.ok()

    metadata2, status = load_from_json(ctx.get_cfg().get_metadata_dir() + "/test2.json", ctx)
    assert status.ok()

    assert metadata.to_json_obj() == metadata2.to_json_obj()

    assert FieldInfo("a", "b") == FieldInfo("a", "b")
    assert [FieldInfo("a", "b"), FieldInfo("c", "d")] == [FieldInfo("a", "b"), FieldInfo("c", "d")]
