# from app.common.context.context import Context
from app.common.error.status import Status
from app.common.query.result import QueryResult


class DBInterface:

    def start(self, ctx) -> Status:
        pass

    # query is a json string
    def on_query(self, query: str) -> (QueryResult | None, Status):
        pass

    def on_insert(self, query: str) -> Status:
        pass


class DBFactoryInterface:

    @classmethod
    def instance(cls) -> DBInterface:
        pass
