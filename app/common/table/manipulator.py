import math

import config
import constant

import logger as logger

from app.common.context.context import Context
from app.common.table.chunk_manager import ChunkManager
from app.common.table.field import extract_column_name
from app.common.table.metadata import Metadata
from app.common.table.selector import Selector
from app.common.table.table import Table
from app.common.table.table_manager import get_table_manager, TableManager
from app.services.database.sql.db_factory import DBFactory as SQLDBFactory
import random



class SortOption:

    def __init__(self, column: str, is_asc: bool, ways: int = config.max_chunk_size):
        self.column = column
        self.is_asc = is_asc
        self.ways = ways  # how many ways are used in merge-sort


class GroupByOption:

    def __init__(self, columns: list[str], new_columns: list[str], having_selector: Selector):
        self.columns = columns
        self.new_columns = new_columns
        self.having_selector = having_selector


class TableManipulator:
    """
    TableManipulator is responsible for implementing all query logics
    """

    @staticmethod
    def filter(src_table: Table, selector: Selector) -> Table:
        """
        select entries which satisfies certain condition, only works on single table
        :param src_table: input table
        :return: Table: temporary table
        """
        table_manger = get_table_manager()
        new_table, status = table_manger.create_tmp_table(src_table.metadata)
        if not status.ok():
            raise RuntimeError("failed to create new tmp table")

        new_records = []
        for chunk in src_table.chunk_manager.get_iter():
            for entry in chunk:
                match, status = selector.is_match([entry])
                if not status.ok():
                    raise RuntimeError("selector failed to valuate expression")
                if match:
                    new_records.append(entry)
                    if len(new_records) >= config.max_chunk_size:
                        status = new_table.insert_bulk(new_records)
                        if not status.ok():
                            raise RuntimeError("failed to insert new entry")
                        new_records = []
        if len(new_records):
            status = new_table.insert_bulk(new_records)
            if not status.ok():
                raise RuntimeError("failed to insert new entry")

        if not status.ok():
            raise RuntimeError("failed to insert records")

        return new_table

    # TODO support nosql, which has nested fields i.e. a.b.c
    @staticmethod
    def projection(src_table: Table, desired_column: list[str]) -> Table:
        """
        project src_table to desired columns, only works on single table
        if duplicated columns are desired, we have to give them different names
        :param src_table: input table
        :param desired_column: all columns to keep
        :return: temporary table
        """
        table_manger = get_table_manager()
        new_field_info = []
        if len(desired_column) != len(set(desired_column)):
            raise RuntimeError("unable to project duplicated columns {}, which is not existed".format(desired_column))

        for col_name in desired_column:
            extracted_name = extract_column_name(col_name)
            if extracted_name not in src_table.metadata.get_all_field_names():
                raise RuntimeError("failed to project column {}, which is not existed".format(extracted_name))
            new_field_info.append({extracted_name: src_table.metadata.get_field_type(extracted_name)})

        new_metadata = Metadata(src_table.name, src_table.metadata.db_type, new_field_info)
        new_table, status = table_manger.create_tmp_table(new_metadata)
        if not status.ok():
            raise RuntimeError("failed to create new tmp table")

        new_records = []
        for chunk in src_table.chunk_manager.get_iter():
            for entry in chunk:
                new_records.append(entry)
                if len(new_records) >= config.max_chunk_size:
                    status = new_table.insert_bulk(new_records)
                    if not status.ok():
                        raise RuntimeError("failed to insert new entry")
                    new_records = []

        if len(new_records):
            status = new_table.insert_bulk(new_records)
            if not status.ok():
                raise RuntimeError("failed to insert new entry")

        return new_table

    @staticmethod
    def split_on(src_table: Table, selector: Selector):
        pass

    @staticmethod
    def _sort_each_chunk(src_table: Table, column: str, is_asc: bool) -> Table:
        new_table, status = get_table_manager().create_tmp_table(src_table.metadata)
        if not status.ok():
            raise RuntimeError("failed to create new tmp table")

        # first pass
        for chunk in src_table.chunk_manager.get_iter():
            def sort_key(item):
                return item[column]

            new_chunk = sorted(chunk, key=sort_key, reverse=(not is_asc))
            status = new_table.insert_bulk(new_chunk)
            if not status.ok():
                raise RuntimeError("failed to insert records")

        return new_table

    @staticmethod
    def _merge_sort(src_table: Table, ways: int, column: str, is_asc: bool) -> Table:
        if ways <= 1:
            raise RuntimeError("invalid ways {} for merge sort".format(ways))

        total_chunks = src_table.chunk_manager.get_chunk_cnt()
        # if there are 0 - 100 chunks, then 1 run is needed
        # if there are 101 - 10000 chunks, then 2 run is needed
        # ...
        passes = math.ceil(math.log(total_chunks, ways))
        input_table = src_table
        step = 1
        generated_runs_cnt = total_chunks
        for _ in range(passes):
            cm = input_table.chunk_manager
            # total_chunks must be updated for each pass, as the size might shrink
            total_chunks = cm.get_chunk_cnt()
            generated_runs_cnt = math.ceil(generated_runs_cnt / ways)
            out_put_table, status = get_table_manager().create_tmp_table(src_table.metadata)
            if not status.ok():
                raise RuntimeError("failed to create new tmp table")
            idx = 0
            for i in range(generated_runs_cnt):
                runs = []
                # load at most ways chunks into memory
                for j in range(ways):
                    if idx >= total_chunks:
                        break
                    chunk, status = cm.load_chunk(idx)
                    if not status.ok():
                        raise RuntimeError("failed to load records")
                    runs.append([chunk, max(min(step, total_chunks - idx) - 1, 0), min(idx + step, total_chunks)])
                    idx += step
                idx_per_way, finish_cnt, new_records = [0 for _ in range(len(runs))], 0, []
                while finish_cnt != len(runs):
                    # if any way has run out of element, it should be ignored
                    elements = [(runs[j][0][idx_per_way[j]], j) for j in range(len(runs)) if idx_per_way[j] != len(runs[j][0])]

                    def sort_key(item):
                        return item[0][column]

                    next_idx = min(elements, key=sort_key)[1] if is_asc else max(elements, key=sort_key)[1]
                    # notice, here we don't have to flush chunk into memory whenever it's full
                    new_records.append(runs[next_idx][0][idx_per_way[next_idx]])
                    idx_per_way[next_idx] += 1
                    if idx_per_way[next_idx] == len(runs[next_idx][0]):
                        if runs[next_idx][1] != 0:
                            chunk, status = cm.load_chunk(runs[next_idx][2] - runs[next_idx][1])
                            if not status.ok():
                                raise RuntimeError("failed to load records")
                            runs[next_idx][0] = chunk
                            runs[next_idx][1] -= 1
                            idx_per_way[next_idx] = 0
                    if idx_per_way[next_idx] == len(runs[next_idx][0]) and runs[next_idx][1] == 0:
                        finish_cnt += 1
                status = out_put_table.insert_bulk(new_records)
                if not status.ok():
                    raise RuntimeError("failed to flush onto disk")
            step *= ways
            input_table = out_put_table

        return input_table

    @staticmethod
    def sort(src_table: Table, sort_options: list[SortOption]) -> Table:
        """
        sort src_table, use 100-way merge sort
        For multiple columns:
        1. sort by the first column, generate table t1
        2. spilt table into multiple sub_tables t11, ..., t1n, based on the value of the first column (similar as group_by)
        3. for each sub_table, sort by the second column, generate  sub_tables t21, ..., t2n
        4. merge t21, ..., t2n into t2
        5. split table into sub_tables t31, ..., t3n, based on the combination of the first two columns
        6. ...

        :param src_table:
        :param sort_options: list of SortOption, support sorting by multiple columns
        :return: result table
        """
        # TODO implement multi-field sorting
        column, is_asc, ways = sort_options[0].column, sort_options[0].is_asc, sort_options[0].ways
        table = TableManipulator._sort_each_chunk(src_table, column, is_asc)
        return TableManipulator._merge_sort(table, ways, column, is_asc)

    @staticmethod
    def group_by(src_table: Table, group_by_option: 'GroupByOption') -> Table:
        """
        group by table
        :param src_table: input table, only support single table
        :param group_by_option: options to execute groupBy operation
        :return: result table
        """
        # TODO implement it

    @staticmethod
    def concat(tables: list[Table]) -> Table:
        """
        concat multiple table into a single table (keep the sequence)
        :param tables: all tables to be merged (order matters)
        :return: merged table
        """
        if len(tables) == 0:
            raise RuntimeError("impossible to concat 0 tables")
        # check metadata, field_info should be exactly the same:
        if not all(table.metadata.get_all_fields() == tables[0].metadata.get_all_fields() for table in tables):
            raise RuntimeError("only be able to concat tables with the same field_infos")
        new_table: Table = get_table_manager().create_tmp_table(tables[0].metadata)
        for table in tables:
            for chunk in table.chunk_manager.get_iter():
                for entry in chunk:
                    status = new_table.insert(entry)
                    if not status.ok():
                        raise RuntimeError("failed to insert new entry to table {}".format(table.name))

        return new_table

    @staticmethod
    def join(tables: list[Table], join_option: 'JoinOption') -> Table:
        # TODO implement it
        pass


if __name__ == "__main__":
    logger, cfg = logger.get_logger(constant.DB_TYPE_SQL), config.config_map[constant.DB_TYPE_SQL]
    ctx = Context(logger, cfg, SQLDBFactory.instance())
    tm = get_table_manager(ctx)
    ctx.set_table_manager(tm)
    status = tm.start()
    assert status.ok()

    status = tm.drop_table("test_manipulator")
    assert status.ok()
    table, status = tm.create_table("test_manipulator", Metadata("test_manipulator", constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "str"}]))
    assert status.ok()

    status = table.insert_bulk([{"col1": i, "col2": "a"} for i in range(1025)])
    assert status.ok()
    assert table.chunk_manager.get_chunk_cnt() == 2
    status = table.insert_bulk([{"col1": i, "col2": "b"} for i in range(1023)])
    assert status.ok()
    assert table.chunk_manager.get_chunk_cnt() == 2

    new_table_1 = TableManipulator.filter(table, Selector({
        "op": "||",
        "v1": {
            "op": "==",
            "v1": "0::col2",
            "v2": "a",
        }, "v2": {
            "op": "==",
            "v1": "0::col2",
            "v2": "b",
        }
    }))
    assert status.ok()
    assert new_table_1.chunk_manager.get_chunk_cnt() == 2
    assert new_table_1.chunk_manager.is_chunk_full(new_table_1.chunk_manager.get_last_chunk()[0])

    new_table_2 = TableManipulator.projection(table, desired_column=["0::col2"])
    assert status.ok()
    assert new_table_2.chunk_manager.get_chunk_cnt() == 2
    assert new_table_2.chunk_manager.is_chunk_full(new_table_2.chunk_manager.get_last_chunk()[0])
    assert ('col2' in new_table_2.chunk_manager.get_last_chunk()[0][0] and 'col1' not in new_table_2.chunk_manager.get_last_chunk()[0][0])

    # test sorting
    tm.drop_table("test_manipulator_sorting")
    table, status = tm.create_table("test_manipulator_sorting", Metadata("test_manipulator_sorting", constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "str"}]))
    assert status.ok()

    # test 2 ways merge sort, use 2 bulks
    status = table.insert_bulk([{"col1": i, "col2": "a"} for i in range(cfg.max_chunk_size - 1)])
    assert status.ok()
    status = table.insert_bulk([{"col1": i, "col2": "a"} for i in range(cfg.max_chunk_size + 1)])
    assert status.ok()
    TableManipulator.sort(table, [SortOption('col1', False, 2)])

    # test 2 ways merge sort, use 8 bulks:
    table.chunk_manager.destroy_all_chunks()
    status = table.insert_bulk([{"col1": i, "col2": "a"} for i in range(cfg.max_chunk_size * 8 - 1)])
    assert status.ok()
    table = TableManipulator.sort(table, [SortOption('col1', False, 2)])
    assert table.chunk_manager.get_chunk_cnt() == 8
    chunk, status = table.chunk_manager.load_chunk(7)
    assert chunk[-1]['col1'] == 0
    assert status.ok()
    chunk, status = table.chunk_manager.load_chunk(0)
    assert status.ok()
    assert chunk[0]['col1'] == cfg.max_chunk_size * 8 - 2

    # test random data
    data_range = 100
    random_data = [{"col1": random.randint(0, data_range), "col2": "a"} for _ in range(cfg.max_chunk_size * 8)]
    # Step 2: Insert Data into Table
    table.chunk_manager.destroy_all_chunks()
    status = table.insert_bulk(random_data)
    assert status.ok()
    # Step 3: Sort the Data
    table = TableManipulator.sort(table, [SortOption('col1', True, 2)])
    # Step 4: Verify the Sort
    # Load each chunk and check if the data is sorted
    for chunk_idx in range(8):  # Assuming 8 chunks as in your example
        chunk, status = table.chunk_manager.load_chunk(chunk_idx)
        assert status.ok()
        sorted_chunk = sorted(chunk, key=lambda x: x['col1'])
        assert chunk == sorted_chunk, f"Chunk {chunk_idx} is not sorted correctly."
