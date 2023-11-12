# all queries should return uniformed QueryResult or None
# each result of query should be stored in a file to support streaming (thus the memory usage is reduced)
class QueryResult:

    def __init__(self, result_file_path):
        self.result_file_path = result_file_path

    def get_result_file_path(self) -> str:
        return self.result_file_path
