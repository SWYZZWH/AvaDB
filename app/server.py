import csv
import json
import os
import sys
import tempfile

import constant
from flask import Flask, g, request, send_file

from app.common.error.status import Status, START_FAILED, OK
from app.common.query.query_engine import QueryEngine
from app.common.table.table_manager import TableManager, get_table_manager
from app.services.database.nosql.db_factory import DBFactory as NosqlDBFactory
from app.services.database.sql.db_factory import DBFactory as SQLDBFactory
import config
import logger
from app.common.query.result import QueryResult
from app.common.context import context
from app.common.context.context import Context
from typing import cast

app = Flask(__name__)


# only support queries now
# updating & inserting & deleting are not supported
@app.route('/')
def get_file():
    query = request.json
    result, status = g.ctx.get_db().on_query(query)

    if not status.ok():
        return "failed to process query: {}, due to {} ".format(query, status.code()), 400

    if result is None:
        return "query result is empty for query {} ".format(query), 200

    result = cast(QueryResult, result)
    g.ctx.logger.info("result dat can be found under {}".format(result.get_result_file_path()))

    return send_file(result.get_result_file_path(), as_attachment=True, mimetype='text/plain')


def check_args():
    if len(sys.argv) != 2 or sys.argv[1] not in config.supported_database_types:
        print("Usage: python server.py <database_type>\nSupported types are: {}".format(config.supported_database_types))
        sys.exit(1)


def get_db(logger: logger.Logger, cfg: config.DBConfig):
    if cfg.is_sql():
        return SQLDBFactory.instance()
    elif cfg.is_nosql():
        return NosqlDBFactory.instance()
    else:
        logger.error("unrecognized database type {}, unable to start".format(ctx.get_cfg().get_db_type()))
        return None


def start_db(ctx: Context) -> Status:
    status = ctx.get_db().start(ctx)
    if not status.ok():
        ctx.logger().error("failed to start {}, due to {}".format(ctx.get_cfg().get_db_type(), status))
        return START_FAILED

    return OK


@app.before_request
def before_request():
    # global context which could be used in entire life cycle
    g.ctx = ctx


if __name__ == '__main__':
    check_args()
    db_type = sys.argv[1]
    logger, cfg = logger.get_logger(db_type), config.config_map[db_type]
    ctx = context.Context(logger, cfg, get_db(logger, cfg))
    tm = get_table_manager(ctx)
    ctx.set_table_manager(tm)
    tm.start()
    qe = QueryEngine(ctx)
    ctx.set_query_engine(qe)

    start_db(ctx)
    app.run(port=ctx.get_cfg().get_port())
