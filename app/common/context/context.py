from config import DBConfig
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.database.interface import DBInterface


class Context:

    def __init__(self, logger, cfg: DBConfig, db: 'DBInterface'):
        self.logger = logger
        self.cfg = cfg
        self.db = db

    def get_cfg(self) -> DBConfig:
        return self.cfg

    def get_logger(self):
        return self.logger

    def get_db(self) -> 'DBInterface':
        return self.db
