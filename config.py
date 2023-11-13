import constant
import os

supported_field_types = ['str', 'int', 'float', 'bool']
default_field_type_value = {
    'str': "",
    'int': 0,
    'float': 0,
    'bool': False
}
supported_database_types = [constant.DB_TYPE_SQL, constant.DB_TYPE_NOSQL]
metadata_ext = ".json"
merge_sort_chunks = 100

class DBConfig:

    def __init__(self, db_type: str, port: int, tables_dir: str, metadata_dir: str, supported_types: list[str], chunk_size: int, file_ext: str):
        self.db_type = db_type
        self.port = port
        self.tables_dir = tables_dir
        self.metadata_dir = metadata_dir
        self.supported_field_types = supported_types
        self.max_chunk_size = chunk_size
        self.file_extension = file_ext

    def is_sql(self) -> bool:
        return self.get_db_type() == constant.DB_TYPE_SQL

    def is_nosql(self) -> bool:
        return self.get_db_type() == constant.DB_TYPE_NOSQL

    def get_db_type(self):
        return self.db_type

    def get_port(self) -> int:
        return self.port

    def get_tables_dir(self) -> str:
        return self.tables_dir

    def get_metadata_dir(self) -> str:
        return self.metadata_dir

    def get_supported_field_types(self) -> list[str]:
        return self.supported_field_types

    def get_max_chunk_size(self) -> int:
        return self.max_chunk_size

    def get_file_extension(self) -> str:
        return self.file_extension


nosql_cfg = DBConfig(
    db_type=constant.DB_TYPE_NOSQL,
    port=9527,
    tables_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), constant.TABLES_SUB_DIR, constant.DB_TYPE_NOSQL),
    metadata_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), constant.METADATA_SUB_DIR, constant.DB_TYPE_NOSQL),
    supported_types=supported_field_types,
    chunk_size=1024,
    file_ext=".json"
)

sql_cfg = DBConfig(
    db_type=constant.DB_TYPE_SQL,
    port=9528,
    tables_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), constant.TABLES_SUB_DIR, constant.DB_TYPE_SQL),
    metadata_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), constant.METADATA_SUB_DIR, constant.DB_TYPE_SQL),
    supported_types=supported_field_types,
    chunk_size=1024,
    file_ext=".csv"
)

config_map = {
    constant.DB_TYPE_NOSQL: nosql_cfg,
    constant.DB_TYPE_SQL: sql_cfg,
}
