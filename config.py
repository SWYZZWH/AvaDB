import constant
import os


class DBConfig:

    def __init__(self, db_type: str, port: int, tables_dir: str, metadata_dir: str):
        self.db_type = db_type
        self.port = port
        self.tables_dir = tables_dir
        self.metadata_dir = metadata_dir

    def is_sql(self) -> bool:
        return self.get_db_type() == constant.DB_TYPE_SQL

    def is_nosql(self) -> bool:
        return self.get_db_type() == constant.DB_TYPE_NOSQL

    def get_db_type(self):
        return self.db_type

    def get_port(self) -> int:
        return self.port

    def get_tables_dir(self):
        return self.tables_dir

    def get_metadata_dir(self):
        return self.metadata_dir


nosql_cfg = DBConfig(
    db_type=constant.DB_TYPE_NOSQL,
    port=9527,
    tables_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), constant.TABLES_SUB_DIR, constant.DB_TYPE_NOSQL),
    metadata_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), constant.METADATA_SUB_DIR, constant.DB_TYPE_NOSQL),
)

sql_cfg = DBConfig(
    db_type=constant.DB_TYPE_SQL,
    port=9528,
    tables_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), constant.TABLES_SUB_DIR, constant.DB_TYPE_SQL),
    metadata_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), constant.METADATA_SUB_DIR, constant.DB_TYPE_NOSQL),
)

config_map = {
    constant.DB_TYPE_NOSQL: nosql_cfg,
    constant.DB_TYPE_SQL: sql_cfg,
}

supported_database_types = [constant.DB_TYPE_SQL, constant.DB_TYPE_NOSQL]
