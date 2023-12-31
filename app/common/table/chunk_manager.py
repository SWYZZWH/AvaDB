import csv
import json
import os

import constant

import config
from app.common.context.context import Context
from app.common.error.status import Status, OK, FILE_NOT_EXIST, INVALID_ARGUMENT, UNSUPPORTED, UNKNOWN, FILE_EXIST, INTERNAL
from app.common.table.metadata import Metadata
from app.common.table.csv_adapter import row_to_object, object_to_row


class ChunkIterator:

    def __init__(self, chunk_manager: 'ChunkManager'):
        self.chunk_manager = chunk_manager
        self.current = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.current < self.chunk_manager.get_chunk_cnt():
            chunk, status = self.chunk_manager.load_chunk(self.current)
            if not status.ok():
                raise ValueError("failed to load next chunk!")
            self.current += 1
            return chunk
        else:
            raise StopIteration


class ChunkManager:

    # metadata is only used for SQL
    def __init__(self, table_path: str, metadata: Metadata, ctx: Context):
        self.metadata = metadata
        self.table_path = table_path
        self.ctx = ctx
        self.logger = self.ctx.get_logger()
        self.total_chunks = 0
        self.ext = self.ctx.get_cfg().get_file_extension()
        self.cfg = ctx.get_cfg()
        self.db_type = self.cfg.get_db_type()
        self.max_chunk_size = self.cfg.get_max_chunk_size()

    def start(self) -> Status:
        if not os.path.exists(self.table_path):
            self.ctx.get_logger().warn("uninitialized table {} found", self.table_path)
            return OK
        self.total_chunks = self.get_chunk_cnt()

    def get_chunk_cnt(self):
        if not os.path.exists(self.table_path):
            return 0

        chunk_ids = [int(os.path.splitext(f)[0]) for f in os.listdir(self.table_path)]
        return 0 if len(chunk_ids) == 0 else max(chunk_ids) + 1

    def get_fist_chunk(self) -> (list[object], Status):
        if self.total_chunks == 0:
            self.logger.error("empty table {}, can't find the first chunk".format(self.table_path))
            return [], FILE_NOT_EXIST

        return self.load_chunk(0)

    def get_last_chunk(self) -> (list[object], Status):
        if self.total_chunks == 0:
            self.logger.error("empty table {}, can't find the first chunk".format(self.table_path))
            return [], FILE_NOT_EXIST

        return self.load_chunk(self.total_chunks - 1)

    def get_chunk_path(self, chunk_idx) -> str:
        return os.path.join(self.table_path, str(chunk_idx) + self.ext)

    def get_iter(self) -> ChunkIterator:
        return ChunkIterator(self)

    def get_remaining_slots(self, occupied_cnt: int) -> int:
        remaining = self.max_chunk_size - occupied_cnt
        if remaining < 0:
            self.logger.error("detected overflow chunk!, occupied_cnt: {}, capacity: {}".format(occupied_cnt, self.max_chunk_size))
        return max(0, remaining)

    def get_last_chunk_index(self) -> int:
        return self.total_chunks - 1  # return -1 when table is empty

    # return a list of objects which can be iterated through
    # for sql, we have to transfer each row (which is simply a list to an object) to use it with ease
    def load_chunk(self, chunk_idx: int) -> (list[dict[str, object]], Status):
        if chunk_idx >= self.total_chunks:
            self.logger.error("try to load chunk {}, which is greater than total chunks: {}".format(chunk_idx, self.total_chunks))
            return [], INVALID_ARGUMENT

        chunk_path = self.get_chunk_path(chunk_idx)
        if not os.path.exists(chunk_path):
            self.logger.error("try to load chunk {} from not existed path {}, system is not consistent".format(chunk_idx, chunk_path))
            return [], FILE_NOT_EXIST

        if self.db_type not in config.supported_database_types:
            self.logger.error("unsupported db type".format(self.db_type))
            return [], UNSUPPORTED

        if self.db_type == constant.DB_TYPE_NOSQL:
            # Load a JSON file
            with open(chunk_path, 'r') as file:
                data = json.load(file)
            self.logger.info("load trunk {} for nosql successfully".format(chunk_idx))
            return data, OK
        elif self.db_type == constant.DB_TYPE_SQL:
            # Load a CSV file and convert each row to a dictionary
            with open(chunk_path, 'r') as file:
                reader = csv.reader(file)
                data = [row_to_object(row, self.metadata) for row in reader]
            self.logger.info("load trunk {} for sql successfully".format(chunk_idx))
            return data, OK

        return [], UNKNOWN

    def create_new_chunk(self) -> Status:
        new_chunk_path = self.get_chunk_path(self.total_chunks)
        if os.path.exists(new_chunk_path):
            self.logger.error("try to create a new chunk {} and override a existed chunk, system is not consistent".format(new_chunk_path))
            return FILE_EXIST

        with open(new_chunk_path, "w") as f:
            if self.cfg.is_nosql():
                f.write("[]")
        self.total_chunks += 1
        return OK

    def is_empty_table(self) -> bool:
        return self.total_chunks == 0

    def is_chunk_full(self, chunk: list[object]) -> bool:
        return len(chunk) == self.max_chunk_size

    # always append new record to the last chunk
    # if the last chunk is full i.e. len(chunk) == config.chunk_size, then create a new chunk as the last chunk
    def dump_one(self, record: dict) -> Status:
        self.dump_bulk([record])
        return OK

    def dump_bulk(self, records: list[dict]) -> Status:
        if self.is_empty_table():
            status = self.create_new_chunk()
            if not status.ok():
                return INTERNAL

        chunk, status = self.get_last_chunk()
        if not status.ok():
            self.logger.error("failed to append new record as unable to load last chunk")
            return status

        # fill the last chunk
        remaining = self.get_remaining_slots(len(chunk))
        insert_cnt = min(remaining, len(records))
        chunk += records[:insert_cnt]
        status = self.update_chunk(self.get_last_chunk_index(), chunk)
        if not status.ok():
            self.logger.error("failed to append records to the last chunk")
            return status
        records = records[insert_cnt:]

        # create new chunks and fill
        while len(records):
            status = self.create_new_chunk()
            if not status.ok():
                return INTERNAL
            size = min(self.max_chunk_size, len(records))
            status = self.update_chunk(self.get_last_chunk_index(), records[:size])
            if not status.ok():
                return INTERNAL
            records = records[size:]
        return OK

    def destroy_all_chunks(self) -> Status:
        self.logger.warn("deleting all chunks under {}".format(self.table_path))
        for i in range(0, self.total_chunks):
            chunk_path = self.get_chunk_path(i)
            os.remove(chunk_path)
        self.total_chunks = 0
        self.logger.warn("all chunks under {} are deleted".format(self.table_path))
        return OK

    def update_chunk(self, chunk_idx: int, chunk: list[dict[str, object]]) -> Status:
        if chunk_idx < 0 or chunk_idx >= self.total_chunks:
            self.logger.error("failed to update chunk due to invalid chunk_idx: {}".format(chunk_idx))
            return INVALID_ARGUMENT

        if self.cfg.is_sql():
            csv_rows = [object_to_row(obj, self.metadata) for obj in chunk]
            with open(self.get_chunk_path(chunk_idx), 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(csv_rows)

        elif self.cfg.is_nosql():
            with open(self.get_chunk_path(chunk_idx), 'w', newline='') as f:
                json.dump(chunk, f)

        self.logger.info("successfully update chunk {}".format(chunk_idx))
        return OK
