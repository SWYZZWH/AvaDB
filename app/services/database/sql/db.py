from app.common.query_result.query_result import QueryResult
from app.services.database.interface import DBInterface
from app.common.context.context import Context
from app.common.error.status import Status, OK


class DB(DBInterface):

    def __init__(self):
        pass

    def start(self, ctx: Context) -> Status:
        # TODO: implement this
        return OK

    def process(self, query: str) -> (QueryResult | None, Status):
        # TODO: implement this
        return None, OK
