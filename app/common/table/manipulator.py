from app.common.table.metadata import Metadata
from app.common.table.selector import Selector
from app.common.table.table import Table
from app.common.table.table_manager import get_table_manager


class SortOption:

    def __init__(self, column: str, is_asc: bool):
        self.column = column
        self.is_asc = is_asc


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
        new_table: Table = table_manger.create_tmp_table(src_table.metadata)

        for chunk in src_table.chunk_manager.get_iter():
            for entry in chunk:
                match, status = selector.is_match([entry])
                if not status.ok():
                    raise RuntimeError("selector failed to valuate expression")
                status = new_table.insert(entry)
                if not status.ok():
                    raise RuntimeError("failed to insert new entry")

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
            if col_name not in src_table.metadata.get_all_field_names():
                raise RuntimeError("failed to project column {}, which is not existed".format(col_name))
            new_field_info.append({col_name: src_table.metadata.get_field_type(col_name)})

        new_metadata = Metadata(src_table.name, src_table.metadata.db_type, new_field_info)
        new_table: Table = table_manger.create_tmp_table(new_metadata)

        for chunk in src_table.chunk_manager.get_iter():
            for entry in chunk:
                status = new_table.insert(entry)
                if not status.ok():
                    raise RuntimeError("failed to insert new entry")

        return new_table

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
        # TODO implement it

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
