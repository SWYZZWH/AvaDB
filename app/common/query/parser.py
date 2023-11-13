import json


# TODO implement it
class QueryParser:
    def __init__(self, query: str):
        self.query = query
        pass

    def get_query(self) -> str:
        return self.query

    def parse(self) -> ExecutionPlan:
        return None


