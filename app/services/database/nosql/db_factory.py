from app.services.database.interface import DBFactoryInterface
from app.services.database.interface import DBInterface
from app.services.database.nosql.db import DB


class DBFactory(DBFactoryInterface):
    _instance = None

    def __init__(self):
        pass

    @classmethod
    def instance(cls) -> DBInterface:
        if cls._instance is None:
            cls._instance = DB()
        return cls._instance
