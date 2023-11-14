import math
from collections import Counter
from enum import Enum

import config
import constant

import logger as logger

from app.common.context.context import Context
from app.common.table.chunk_manager import ChunkManager
from app.common.table.field import extract_column_name, FieldInfo
from app.common.table.metadata import Metadata
from app.common.table.selector import Selector
from app.common.table.table import Table
from app.common.table.table_manager import get_table_manager, TableManager
from app.services.database.sql.db_factory import DBFactory as SQLDBFactory
import random


class SortOption:

    def __init__(self, column: str, is_asc: bool = True, ways: int = config.max_chunk_size):
        self.column = column
        self.is_asc = is_asc
        self.ways = ways  # how many ways are used in merge-sort


class ReduceOperation(Enum):
    MAX = 1
    MIN = 2
    SUM = 3
    AVG = 4
    COUNT = 5


class ReduceOption:

    def __init__(self, column: str, agg: ReduceOperation):
        self.column = column
        self.agg = agg


class Reducer:

    def __init__(self, option: ReduceOption):
        self.column = option.column
        self.agg = option.agg
        self.sum = 0
        self.cnt = 0
        self.max = - math.inf
        self.min = math.inf

    def append(self, record: dict[str: object]):
        self.sum += record[self.column]
        self.cnt += 1
        self.max = max(self.max, record[self.column])
        self.min = min(self.min, record[self.column])

    def get_max(self):
        return self.max

    def get_min(self):
        return self.min

    def get_sum(self):
        return self.sum

    def get_count(self):
        return self.cnt

    def get_avg(self):
        return self.sum / self.cnt

    def reduce(self):
        if self.agg == ReduceOperation.MAX:
            return self.get_max()
        elif self.agg == ReduceOperation.MIN:
            return self.get_min()
        elif self.agg == ReduceOperation.COUNT:
            return self.get_count()
        elif self.agg == ReduceOperation.SUM:
            return self.get_sum()
        elif self.agg == ReduceOperation.AVG:
            return self.get_avg()
        raise NotImplemented


class GroupByOption:

    def __init__(self, column: str, reduce_options: list[ReduceOption]):
        """
        :param column: group by column
        :param reduce_options: e.g. max(col1), avg(col2), notice: col1 and col2 should not be the same as self.column
        """
        self.column = column
        self.options = reduce_options


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
    def _sort_one_column(src_table, sort_options: SortOption):
        column, is_asc, ways = sort_options.column, sort_options.is_asc, sort_options.ways
        table = TableManipulator._sort_each_chunk(src_table, column, is_asc)
        return TableManipulator._merge_sort(table, ways, column, is_asc)

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
        return TableManipulator._sort_one_column(src_table, sort_options[0])

    @staticmethod
    def _sort_and_split_on_column(src_table: Table, sort_option: SortOption) -> list[Table]:
        """
        split one sorted table into many tables according to the sorted column
        all records in any one of the result table have the same value of the column
        :param src_table:
        :param column:
        :return:
        """
        tm = get_table_manager()
        new_table, status = tm.create_tmp_table(src_table.metadata)
        if not status.ok():
            raise ValueError("failed to create empty table")
        tables: list[Table] = [new_table]

        if src_table.chunk_manager.is_empty_table():
            return tables

        column = sort_option.column
        sorted_table = TableManipulator.sort(src_table, [sort_option])
        last_value = sorted_table.chunk_manager.get_fist_chunk()[0][0][column]
        new_records = []
        for chunk in sorted_table.chunk_manager.get_iter():
            for entry in chunk:
                if last_value != entry[column]:
                    # dump records
                    status = tables[-1].chunk_manager.dump_bulk(new_records)
                    if not status.ok():
                        raise ValueError("failed to dump bulk")
                    new_table, status = tm.create_tmp_table(src_table.metadata)
                    if not status.ok():
                        raise ValueError("failed to create empty table")
                    new_records = []
                    last_value = entry[column]
                    tables.append(new_table)
                new_records.append(entry)
                # dump new_records
                if len(new_records) == config.max_chunk_size:
                    status = tables[-1].chunk_manager.dump_bulk(new_records)
                    if not status.ok():
                        raise ValueError("failed to dump bulk")
                    new_records = []
        if len(new_records) != 0:
            status = tables[-1].chunk_manager.dump_bulk(new_records)
            if not status.ok():
                raise ValueError("failed to dump bulk")
        return tables

    @staticmethod
    def _concat(tables: list[Table]) -> Table:
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
        new_table, status = get_table_manager().create_tmp_table(tables[0].metadata)
        if not status.ok():
            raise RuntimeError("failed to create tmp table")
        new_records = []
        for table in tables:
            for chunk in table.chunk_manager.get_iter():
                for entry in chunk:
                    new_records.append(entry)
                    if new_records == config.max_chunk_size:
                        status = new_table.chunk_manager.dump_bulk(new_records)
                        if not status.ok():
                            raise RuntimeError("failed to dump records")
                        new_records = []

        if len(new_records) != 0:
            status = new_table.chunk_manager.dump_bulk(new_records)
            if not status.ok():
                raise RuntimeError("failed to dump records")

        return new_table

    @staticmethod
    def rename_fields(src_table: Table, field_rename_map: dict[str, str]) -> Table:
        for k in field_rename_map:
            if src_table.metadata.get_field_info(k) is None:
                raise ValueError("{} is not in table {}".format(k, src_table.name))
        tm = get_table_manager()
        new_fields = []
        for field_info in src_table.metadata.get_all_fields():
            if field_info.name in field_rename_map:
                new_fields.append({field_rename_map[field_info.name]: field_info.value_type})
            else:
                new_fields.append({field_info.name: field_info.value_type})
        new_metadata = Metadata(src_table.metadata.table_name, src_table.metadata.db_type, new_fields)
        new_table, status = tm.create_tmp_table(new_metadata)
        if not status.ok():
            raise ValueError("failed to create new tmp table")
        return new_table

    @staticmethod
    def _decorate_reduced_column_name(name: str, agg: ReduceOperation):
        return constant.REDUCED_COLUMN_NAME_SEP.join([name, agg.name])

    @staticmethod
    def _reduce_table(src_table: Table, group_by_column: str, reduce_options: list[ReduceOption]) -> Table:
        """
        reduce all entries in a table into a single entry
        must call _sort_and_split_on_column first, thus all entries in this table share the same value for the group by column
        :param src_table:
        :param reduce_options: support aggregation functions on multiple columns (but not the group by column!)
        :return: a table with only one entry
        """
        if src_table.chunk_manager.is_empty_table():
            raise RuntimeError("can't reduce empty table")

        for option in reduce_options:
            if option.column == group_by_column:
                raise RuntimeError("can't call reduce function on group by column: {}".format(group_by_column))

        tm = get_table_manager()
        # group_by_column should be untouched
        # max(col) will be renamed as MAX_col
        new_fields = [{group_by_column: src_table.metadata.get_field_type(group_by_column)}]
        reducers = []
        for field_info in src_table.metadata.get_all_fields():
            for option in reduce_options:
                if field_info.get_name() == option.column:
                    new_field_name = TableManipulator._decorate_reduced_column_name(field_info.get_name(), option.agg)
                    if new_field_name in [reducer.column for reducer in reducers]:
                        raise RuntimeError("duplicated aggregated field detected: {}".format(new_field_name))
                    reducers.append(Reducer(option))
                    # TODO handle potential type transferring e.g. int -> float
                    # TODO compatible with nosql
                    new_fields.append({new_field_name: 'float'})
        new_table, status = tm.create_tmp_table(Metadata(src_table.metadata.table_name, src_table.metadata.db_type, new_fields))
        if not status.ok():
            raise RuntimeError("can't create tmp table")

        for chunk in src_table.chunk_manager.get_iter():
            for entry in chunk:
                for reducer in reducers:
                    reducer.append(entry)

        chunk, status = src_table.chunk_manager.get_fist_chunk()
        if not status.ok():
            raise RuntimeError("can't load src table")
        new_record = {group_by_column: chunk[0][group_by_column]}
        for i, field_info in enumerate(new_table.metadata.get_all_fields()[1:]):
            new_record[field_info.name] = reducers[i].reduce()

        status = new_table.chunk_manager.dump_one(new_record)
        if not status.ok():
            raise RuntimeError("can't dump new record")
        return new_table

    @staticmethod
    def group_by(src_table: Table, group_by_option: 'GroupByOption') -> Table:
        """
        group by col can be implemented by:
            1. gathering all fields we are interested, supported fields are: col, max_col,min_col, sum_col, count_col, avg_col
            2. call _sort_and_split_on_column, get a bunch of tables
            3. reduce each table to a table with a single line
            4. merge all these reduced tables
        This only involves O(N) I/O operations, N is entry cnt, which is acceptable
        no need for implementing subquery, it can simply be done by sub query)
        will rename max(col), min(col), sum(col), count(col), avg(col) to max_col, min_col, sum_col, count_col, avg_col

        :param src_table: input table, only support single table
        :param group_by_option: options to execute groupBy operation
        :return: result table
        """
        # TODO support group by on multiple columns & group by joined table
        tables = TableManipulator._sort_and_split_on_column(src_table, SortOption(group_by_option.column))
        new_tables = []
        for table in tables:
            new_tables.append(TableManipulator._reduce_table(table, group_by_option.column, group_by_option.options))

        return TableManipulator._concat(new_tables)

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
    sorted_table = TableManipulator.sort(table, [SortOption('col1', True, 2)])
    # Step 4: Verify the Sort
    # Load each chunk and check if the data is sorted
    for chunk_idx in range(8):  # Assuming 8 chunks as in your example
        chunk, status = sorted_table.chunk_manager.load_chunk(chunk_idx)
        assert status.ok()
        sorted_chunk = sorted(chunk, key=lambda x: x['col1'])
        assert chunk == sorted_chunk, f"Chunk {chunk_idx} is not sorted correctly."

    # test sort and split into different tables
    tables = TableManipulator._sort_and_split_on_column(table, SortOption('col1', True, 2))
    freq_list = sorted(Counter(item['col1'] for item in random_data).items())
    assert len(freq_list) == len(tables)
    for i, pair in enumerate(freq_list):
        entries, status = tables[i].chunk_manager.load_chunk(0)
        assert status.ok()
        assert all(entry['col1'] == pair[0] for entry in entries)

    # test concat tables
    new_table_4 = TableManipulator._concat(tables)
    sorted_random_data = sorted(random_data, key=lambda item: item['col1'])
    i = 0
    for chunk in new_table_4.chunk_manager.get_iter():
        for j, entry in enumerate(chunk):
            assert entry['col1'] == sorted_random_data[i]['col1']
            i += 1

    # test rename
    table_name = "test_manipulator_rename"
    tm.drop_table(table_name)
    table, status = tm.create_table(table_name, Metadata(table_name, constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "str"}]))
    new_table_3 = TableManipulator.rename_fields(table, {"col1": "col2", "col2": "col1"})
    assert new_table_3.metadata.get_all_fields()[0].get_name() == "col2"
    assert new_table_3.metadata.get_all_fields()[1].get_name() == "col1"

    # test group by
    table_name = "test_manipulator_groupby"
    tm.drop_table(table_name)
    table, status = tm.create_table(table_name, Metadata(table_name, constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "float"}, {"col3": "str"}]))
    assert status.ok()
    status = table.insert({"col1": 1, "col2": 1, "col3": "a"})
    assert status.ok()
    status = table.insert({"col1": 1, "col2": 1, "col3": "a"})
    assert status.ok()
    status = table.insert({"col1": 1, "col2": 2, "col3": "a"})
    assert status.ok()
    status = table.insert({"col1": 1, "col2": 3, "col3": "a"})
    assert status.ok()
    status = table.insert({"col1": 1, "col2": 0.5, "col3": "b"})
    assert status.ok()
    status = table.insert({"col1": 1, "col2": 1.5, "col3": "b"})
    assert status.ok()
    status = table.insert({"col1": 1, "col2": 1.0, "col3": "b"})
    assert status.ok()
    new_table = TableManipulator.group_by(table, group_by_option=GroupByOption(
        column="col3",
        reduce_options=[
            ReduceOption('col1', ReduceOperation.MAX),
            ReduceOption('col1', ReduceOperation.MIN),
            ReduceOption('col1', ReduceOperation.SUM),
            ReduceOption('col1', ReduceOperation.COUNT),
            ReduceOption('col1', ReduceOperation.AVG),
            ReduceOption('col2', ReduceOperation.MAX),
            ReduceOption('col2', ReduceOperation.SUM),
            ReduceOption('col2', ReduceOperation.COUNT),
            ReduceOption('col2', ReduceOperation.AVG),
        ]))

    # check metadata:
    assert new_table.metadata.get_all_field_names() == ['col3', 'col1__MAX', 'col1__MIN', 'col1__SUM', 'col1__COUNT', 'col1__AVG', 'col2__MAX', 'col2__SUM', 'col2__COUNT', 'col2__AVG']
    assert new_table.metadata.get_all_fields()[0].get_value_type() == "str"
    assert all(field.get_value_type() == "float" for field in new_table.metadata.get_all_fields()[1:])

    # check content
    chunk, status = new_table.chunk_manager.get_fist_chunk()
    assert status.ok()
    assert len(chunk) == 2  # group by is in effect
    assert chunk[0]['col1__MAX'] == 1
    assert chunk[0]['col1__MIN'] == 1
    assert chunk[1]['col1__COUNT'] == 3
    assert chunk[1]['col1__AVG'] == 1
    assert chunk[0]['col2__MAX'] == 3
    assert 'col2__MIN' not in chunk[0]
    assert chunk[0]['col2__SUM'] == 7
    assert chunk[0]['col2__COUNT'] == 4
    assert chunk[0]['col2__AVG'] == 7 / 4
    assert chunk[1]['col2__MAX'] == 1.5
    assert 'col2__MIN' not in chunk[1]
    assert chunk[1]['col2__SUM'] == 3
    assert chunk[1]['col2__COUNT'] == 3
    assert chunk[1]['col2__AVG'] == 1

