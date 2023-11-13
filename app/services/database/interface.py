from app.common.context.context import Context
from app.common.error.status import Status
from app.common.query.result import QueryResult


class DBInterface:

    def start(self, ctx: Context) -> Status:
        pass

    # query is a json string
    def process(self, query: str) -> (QueryResult | None, Status):
        pass


class DBFactoryInterface:

    @classmethod
    def instance(cls) -> DBInterface:
        pass
