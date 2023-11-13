import os
import shutil
from enum import Enum

import config
import constant

import logger as logger

from app.common.context.context import Context
from app.common.error.status import Status, DUPLICATED_TABLE_CREATION_REQUEST, INVALID_ARGUMENT, OK, START_FAILED, INTERNAL, FILE_NOT_EXIST, FILE_EXIST
from app.common.table.metadata import Metadata, load_from_json, save_as_json
from app.common.table.selector import Selector
from app.common.table.table import Table
from app.common.table.chunk_manager import ChunkManager
from app.services.database.sql.db_factory import DBFactory as SQLDBFactory


class TableManagerState(Enum):
    RUNNING = 0
    STOPPED = 1


# Manager of all tables, avoid data racing
class TableManager:

    def __init__(self, ctx: Context):
        self.ctx = ctx
        self.logger = ctx.get_logger()
        self.cfg = self.ctx.get_cfg()
        self.tables_dir = self.cfg.get_tables_dir()
        self.metadata_dir = self.cfg.get_metadata_dir()
        self.table_map = {}
        self.state = TableManagerState.STOPPED
        self.tmp_table_cnt = 0

    def load_table_names(self) -> (list[str], Status):
        os.makedirs(self.metadata_dir, exist_ok=True)

        table_names = []
        for f in os.listdir(self.metadata_dir):
            if os.path.isdir(os.path.join(self.metadata_dir, f)):
                self.logger.warn("directory {} detected under metadata path {}".format(f, self.metadata_dir))
                continue
            if os.path.splitext(f)[1] != config.metadata_ext:
                self.logger.warn("error format metadata file {} detected under metadata path {}".format(f, self.metadata_dir))
                continue
            table_names.append(os.path.splitext(f)[0])
        return table_names, OK

    def load_all_metadata(self) -> (dict[str, Metadata], Status):
        table_names, status = self.load_table_names()
        if not status.ok():
            return Status

        metadata_map = {}
        for name in table_names:
            full_path = os.path.join(self.metadata_dir, name) + config.metadata_ext
            if not os.path.exists(full_path):
                self.logger.error("failed to load metadata for {} due to file doesn't exist", name)
                return {}, FILE_NOT_EXIST
            metadata, status = load_from_json(full_path, self.ctx)
            if not status.ok():
                self.logger.error("failed to load metadata from {}".format(full_path))
                return {}, Status
            metadata_map[name] = metadata
        return metadata_map, OK

    def _register_table_in_memory(self, table_name: str, metadata: Metadata) -> (Table | None, Status):
        if table_name in self.table_map:
            self.logger.error("the table {} is already existed, failed to recreate".format(table_name))
            return None, DUPLICATED_TABLE_CREATION_REQUEST

        if table_name == "":
            self.logger.error("can't create a table with empty name")
            return None, INVALID_ARGUMENT

        if metadata.get_table_name() != table_name:
            self.logger.info("invalid metadata, table name doesn't match: {} vs {}".format(metadata.get_table_name(), table_name))
            return None, INVALID_ARGUMENT

        if not os.path.exists(self.tables_dir):
            self.logger.info("database dir doesn't exist, creating it...")
            os.makedirs(self.tables_dir)

        if not os.path.exists(self.metadata_dir):
            self.logger.info("metadata dir doesn't exist, creating it...")
            os.makedirs(self.metadata_dir)

        self.table_map[table_name] = Table(table_name, metadata, self.ctx)
        return self.table_map[table_name], OK

    def _unregister_table_in_memory(self, table_name: str) -> Status:
        if table_name not in self.table_map:
            self.logger.warn("try to unregister a table which is not existed".format(table_name))
        else:
            self.table_map.pop(table_name)
        return OK

    # will override
    def _create_table_on_disk(self, table_name: str, metadata: Metadata) -> Status:
        status = save_as_json(metadata, self.ctx)
        if not status:
            self.logger.error("failed to create new table {} on disk, due to unable to save it to json".format(table_name))
            return INVALID_ARGUMENT
        table_path = os.path.join(self.tables_dir, table_name)
        if os.path.exists(table_path):
            self.logger.error("failed to create new table {} on disk, due to pre-existed tables".format(table_name))
            return FILE_EXIST
        os.makedirs(table_path, exist_ok=True)
        return OK

    # fault tolerate
    def _drop_table_on_disk(self, table_name: str):
        metadata_path = os.path.join(self.metadata_dir, table_name) + config.metadata_ext
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
        table_path = os.path.join(self.tables_dir, table_name)
        if os.path.exists(table_path):
            shutil.rmtree(table_path)

    def drop_table(self, table_name: str) -> Status:
        if not self.is_started():
            return START_FAILED
        status = self._unregister_table_in_memory(table_name)
        if not status.ok():
            self.logger.error("failed to drop table {}".format(table_name))
            return status

        # couldn't be reverted, should execute at the end
        self._drop_table_on_disk(table_name)
        return OK

    def create_table(self, table_name: str, metadata: Metadata) -> (Table | None, Status):
        if not self.is_started():
            return START_FAILED

        if table_name in self.table_map:
            self.logger.error("the table {} is already existed, failed to recreate".format(table_name))
            return None, DUPLICATED_TABLE_CREATION_REQUEST

        # try create on disk
        status = self._create_table_on_disk(table_name, metadata)
        if not status.ok():
            self.logger.error("failed to create table {}".format(table_name))
            return None, status

        table, status = self._register_table_in_memory(table_name, metadata)
        if not status:
            self.logger.error("failed to create new table {}".format(table_name))
            # on failed, we must destroy the dirs on the disk!
            self._drop_table_on_disk(table_name)
            return INTERNAL

        return table, OK

    def check_consistency_between_metadata_and_table(self) -> bool:
        table_names, status = self.load_table_names()
        if not status.ok():
            self.logger.error("failed to pass check due to unable to load table names")
            return False
        for f in os.listdir(self.tables_dir):
            full_path = os.path.join(self.tables_dir, f)
            if not os.path.isdir(full_path):
                self.logger.warn("regular file {} detected under tables dir".format(f))
            if f not in table_names:
                self.logger.error("unexpected table {} detected under tables dir, should be removed".format(f))
                return False
        return True

    def create_tmp_table(self, metadata) -> (Table | None, Status):
        tmp_table_name = constant.TMP_TABLE_PREFIX + str(self.tmp_table_cnt)
        metadata.table_name = tmp_table_name
        self.logger.info("creating temporary table: {}".format(tmp_table_name))
        return self.create_table(tmp_table_name, metadata)

    def is_tmp_table(self, table_name: str):
        return table_name.startswith(constant.TMP_TABLE_PREFIX)

    def clear_tmp_tables(self) -> Status:
        names, _ = self.load_table_names()
        for name in names:
            if self.is_tmp_table(name):
                if not self.drop_table(name).ok():
                    return INTERNAL

    def start(self) -> Status:
        if self.is_started():
            self.logger.error("table manager is already started")
            return START_FAILED

        status = self.clear_tmp_tables()
        if not status.ok():
            self.logger.error("failed to clear all tmp tables")
            return START_FAILED

        # check all tables under tables subdir
        # any table under tables subdir must have a metadata file under metadata (no dangling table is allowed)
        if not self.check_consistency_between_metadata_and_table():
            self.logger.error("failed to start table manager due to inconsistency between metadata and tables")
            return START_FAILED

        metadata_dict, status = self.load_all_metadata()
        if not status.ok():
            self.logger.error("failed to start table manager due to unable to load all metadata")
            return START_FAILED

        for name, metadata in metadata_dict.items():
            if name in self.table_map:
                self.logger.error("duplicated table {} detected", name)
                return DUPLICATED_TABLE_CREATION_REQUEST
            table_path = os.path.join(self.tables_dir, name)
            if not os.path.exists(table_path):
                os.makedirs(table_path)
            self.table_map[name] = Table(table_name=name, metadata=metadata, ctx=self.ctx)

        self.state = TableManagerState.RUNNING
        return OK

    def is_started(self) -> bool:
        return self.state == TableManagerState.RUNNING


table_manager_singleton = None


def get_table_manager(ctx: Context = None) -> TableManager:
    global table_manager_singleton
    if table_manager_singleton is None and ctx is not None:
        table_manager_singleton = TableManager(ctx)
    return table_manager_singleton


if __name__ == "__main__":
    logger, cfg = logger.get_logger(constant.DB_TYPE_SQL), config.config_map[constant.DB_TYPE_SQL]
    ctx = Context(logger, cfg, SQLDBFactory.instance())
    tm = TableManager(ctx)
    ctx.set_table_manager(tm)
    status = tm.start()

    status = tm.drop_table("test_table_manager")
    assert status.ok()
    table, status = tm.create_table("test_table_manager", Metadata("test_table_manager", constant.DB_TYPE_SQL, [{"col1": "int"}, {"col2": "str"}]))
    assert status.ok()

    for i in range(1024):
        # should all be in first chunk
        table.insert({"col1": i, "col2": "a"})
    assert table.chunk_manager.get_chunk_cnt() == 1
    chunk, status = table.chunk_manager.load_chunk(0)
    assert status.ok()
    assert len(chunk) == cfg.max_chunk_size

    # should be in the second chunk
    table.insert({"col1": 2, "col2": "b"})
    assert table.chunk_manager.get_chunk_cnt() == 2
    chunk, status = table.chunk_manager.load_chunk(1)
    assert len(chunk) == 1
    assert chunk[0]['col2'] == "b"

    status = table.delete(Selector({
        "op": "==",
        "v1": "0::col1",
        "v2": 2
    }))
    assert status.ok()
    chunk, status = table.chunk_manager.load_chunk(1)
    assert len(chunk) == 0
