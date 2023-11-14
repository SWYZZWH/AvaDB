import os

from app.common.context.context import Context
from app.common.error.status import Status, OK, INTERNAL
from app.common.table.chunk_manager import ChunkManager

from app.common.table.metadata import Metadata


# iterate through a by chunks
class TableIterator:
    pass


# The abstraction of actual tables storing on the disk
# It should support two different data formats: json or csv
# Supported operations: projection, filtering, sorting, deduplication, grouping by,joining two tables
# It also contains all information of the table including metadata, location of metadata and actual table on the disk
class Table:

    def __init__(self, table_name: str, metadata: Metadata, ctx: Context):
        self.name = table_name
        self.metadata = metadata
        self.chunk_cnt = 0
        self.logger = ctx.get_logger()
        self.cfg = ctx.get_cfg()
        self.chunk_manager: ChunkManager = ChunkManager(os.path.join(self.cfg.get_tables_dir(), table_name), metadata, ctx)
        self.chunk_manager.start()

    # drop this table, delete from disk
    def drop(self) -> Status:
        self.logger.warn("dropping a table {}".format(self.name))
        status = self.chunk_manager.destroy_all_chunks()
        if not status.ok():
            self.logger.warn("failed to drop table {}".format(self.name))
            return status
        self.logger.warn("table {} is dropped".format(self.name))
        return OK

    # table should know how many chunks are on the disks, where to find each chunk, and how to update/delete/insert data into these trunks
    # record should be a json object
    def insert(self, record: dict) -> Status:
        # self.logger.info("inserting a record {} to table {}".format(record, self.name))
        status = self.chunk_manager.append(record)
        if not status.ok():
            self.logger.warn("failed to insert record {} to table {}".format(record, self.name))
            return status
        # self.logger.info("record {} is inserted to table {}".format(record, self.name))
        return OK

    def insert_bulk(self, records: list[dict]) -> Status:
        # self.logger.info("inserting a record {} to table {}".format(record, self.name))
        status = self.chunk_manager.append_bulk(records)
        if not status.ok():
            self.logger.warn("failed to insert record #{} to table {}".format(len(records), self.name))
            return status
        # self.logger.info("record {} is inserted to table {}".format(record, self.name))
        return OK

    # TODO add lock for all these operations on table
    def update(self, selector, new_record: dict) -> Status:
        chunk_cnt = self.chunk_manager.get_chunk_cnt()
        is_changed = False
        for i in range(chunk_cnt):
            chunk, status = self.chunk_manager.load_chunk(i)
            is_chunk_changed = False
            if not status.ok():
                self.logger.error("failed to update")
                return INTERNAL
            for j in range(len(chunk)):
                is_match, status = selector.is_match([chunk[j]])
                if not status.ok():
                    self.logger.error("failed to use selector, may due to unable to parse expression {}".format(selector.expression))
                    return INTERNAL
                if is_match:
                    self.logger.info("update record {} with {} in chunk {}".format(chunk[j], new_record, i))
                    chunk[j].update(new_record)
                    is_chunk_changed = True
                    is_changed = True
            if is_chunk_changed:
                self.chunk_manager.update_chunk(i, chunk)
                self.logger.info("successfully update chunk {} for table {}".format(i, self.name))
        if not is_changed:
            self.logger.warn("no record is modified")
        return OK

    # TODO add lock for all these operations on table
    def delete(self, selector) -> Status:
        chunk_cnt = self.chunk_manager.get_chunk_cnt()
        is_changed = False
        for i in range(chunk_cnt):
            chunk, status = self.chunk_manager.load_chunk(i)
            is_chunk_changed = False
            if not status.ok():
                self.logger.error("failed to update")
                return INTERNAL
            new_chunk = []
            for j in range(len(chunk)):
                is_match, status = selector.is_match([chunk[j]])
                if not status.ok():
                    self.logger.error("failed to use selector, may due to unable to parse expression {}".format(selector.expression))
                    return INTERNAL
                if is_match:
                    self.logger.info("delete record {} in chunk {}".format(chunk[j], i))
                    is_chunk_changed = True
                    is_changed = True
                    continue
                new_chunk.append(chunk[j])
            if is_chunk_changed:
                self.chunk_manager.update_chunk(i, new_chunk)
                self.logger.info("successfully delete records in chunk {} for table {}".format(i, self.name))
        if not is_changed:
            self.logger.warn("no record is deleted")
        return OK

    # def filter(self, selector) -> Table:

# def sort(self) -> (Table, Status):
#     pass
#
# def project(self) -> (Table, Status):
#     pass
#
# def filter(self) -> (Table, Status):
#     pass
#
# def uniq(self) -> (Table, Status):
#     pass
#
# def group_by(self) -> (Table, Status):
#     pass
#
# def join(self, t1: Table, t2: Table):
#     pass


# a table is consisted of chunks
# ChunkManager manage all chunks of a table
# chunks are stored as files 0${ext}, 1${ext}, ..., ${self.chunk_size - 1}${ext} under ./database/${db_type}/${table_name}, ${ext} is either .json or .csv
# once the chunk is created, it won't be deleted (even if all records in it are removed).
# all operations changing data files on the disk should be carried out by ChunkManager
