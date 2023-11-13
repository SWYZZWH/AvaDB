from config import DBConfig
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.database.interface import DBInterface

if TYPE_CHECKING:
    from app.common.table.table_manager import TableManager


class Context:

    def __init__(self, logger, cfg: DBConfig, db: 'DBInterface', table_manager: 'TableManager' = None):
        self.logger = logger
        self.cfg = cfg
        self.db = db
        self.table_manager = table_manager

    def get_cfg(self) -> DBConfig:
        return self.cfg

    def get_logger(self):
        return self.logger

    def get_db(self) -> 'DBInterface':
        return self.db

    def get_table_manager(self) -> 'TableManager':
        return self.table_manager

    def set_table_manager(self, table_manager: 'TableManager'):
        self.table_manager = table_manager
