import json
from unittest import TestCase
import random
import config
import constant
import logger as mylogger

from app.common.context.context import Context
from app.common.query.query_engine import QueryEngine
from app.common.table.metadata import Metadata
from app.common.table.table_manager import get_table_manager
from app.services.database.sql.db_factory import DBFactory as SQLDBFactory


class Test(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.logger, cls.cfg = mylogger.get_logger(constant.DB_TYPE_SQL), config.config_map[constant.DB_TYPE_SQL]
        ctx = Context(cls.logger, cls.cfg, SQLDBFactory.instance())
        cls.tm = get_table_manager(ctx)
        ctx.set_table_manager(cls.tm)
        status = cls.tm.start()
        cls.qe = QueryEngine(ctx)
        assert status.ok()

    def test_query_engine_group_by(self):
        table_name_1 = "test_query_engine_group_by"
        self.tm.drop_table(table_name_1)
        table1, status = self.tm.create_table(table_name_1, Metadata(table_name_1, constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "str"}]))
        assert status.ok()
        table1.insert({"col1": 1, "col2": "a"})
        table1.insert({"col1": 3, "col2": "a"})
        table1.insert({"col1": 1, "col2": "b"})
        table1.insert({"col1": 2, "col2": "b"})
        table1.insert({"col1": 1, "col2": "c"})
        table1.insert({"col1": 1, "col2": "c"})

        query = {
            "src_table": table_name_1,
            "desired_columns": ["::col2", "::col1__MAX", "::col1__MIN", "::col1__COUNT", "::col1__SUM", "::col1__AVG"],
            "group_by": ["::col2"],
        }

        t, status = self.qe.run(json.dumps(query))
        assert status.ok()
        chunk, status = t.chunk_manager.get_fist_chunk()
        assert status.ok()
        assert chunk == [{'col2': 'a', 'col1__MAX': 3.0, 'col1__MIN': 1.0, 'col1__COUNT': 2.0, 'col1__SUM': 4.0, 'col1__AVG': 2.0},
                         {'col2': 'b', 'col1__MAX': 2.0, 'col1__MIN': 1.0, 'col1__COUNT': 2.0, 'col1__SUM': 3.0, 'col1__AVG': 1.5},
                         {'col2': 'c', 'col1__MAX': 1.0, 'col1__MIN': 1.0, 'col1__COUNT': 2.0, 'col1__SUM': 2.0, 'col1__AVG': 1.0}]

    def test_query_engine_sort(self):
        table_name = "test_query_engine_sort"
        self.tm.drop_table(table_name)
        table, status = self.tm.create_table(table_name, Metadata(table_name, constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "str"}]))
        assert status.ok()

        data_range = 100
        random_data = [{"col1": random.randint(0, data_range), "col2": "a"} for _ in range(self.cfg.max_chunk_size * 8)]
        # Step 2: Insert Data into Table
        status = table.insert_bulk(random_data)
        assert status.ok()

        query = {
            "src_table": table_name,
            "order_by": [{"column": "::col1", "is_asc": True}],
        }

        sorted_table, status = self.qe.run(json.dumps(query))
        assert status.ok()

        # Step 4: Verify the Sort
        # Load each chunk and check if the data is sorted
        for chunk_idx in range(8):  # Assuming 8 chunks as in your example
            chunk, status = sorted_table.chunk_manager.load_chunk(chunk_idx)
            assert status.ok()
            sorted_chunk = sorted(chunk, key=lambda x: x['col1'])
            assert chunk == sorted_chunk, f"Chunk {chunk_idx} is not sorted correctly."

    def test_query_engine_join(self):
        table_name1, table_name2 = "test_query_engine_join_1", "test_query_engine_join_2"
        self.tm.drop_table(table_name1)
        self.tm.drop_table(table_name2)
        table1, status = self.tm.create_table(table_name1, Metadata(table_name1, constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "str"}]))
        assert status.ok()
        table2, status = self.tm.create_table(table_name2, Metadata(table_name2, constant.DB_TYPE_SQL, [{"col3": "int"}, {"col2": "str"}]))
        assert status.ok()

        table1.insert({"col1": 1, "col2": "b"})
        table1.insert({"col1": 2, "col2": "a"})
        table2.insert({"col3": 3, "col2": "a"})
        table2.insert({"col3": 4, "col2": "b"})

        query = {
            "src_table": {
                "t1": table_name1,
                "t2": table_name2,
                "join_type": "outer",
                "join_condition": {
                    "op": "==",
                    "v1": "0::col2",
                    "v2": "0::col2"
                }
            },
            "desired_columns": [
                "::0::col1",
                "::0::col2"
            ]
        }
        self.qe.run(json.dumps(query))
