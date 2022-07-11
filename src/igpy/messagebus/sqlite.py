"""Message Bus repository implementation using SQLite"""

from contextlib import contextmanager
from datetime import datetime
import sqlite3
from typing import List
from uuid import uuid4
from igpy.messagebus.persistence import (
    CursorFactory,
    Mapper,
    Repository,
    StoredMessage,
    TranscodingMapper,
)
from igpy.serialization.transcode import JSONTranscoder


class SQLiteRepository(Repository):
    queue_table_name: str = "queue_item"
    lock_table_name: str = "queue_item_lock"

    def __init__(self, db_name: str = None, mapper: Mapper = None):
        mapper = mapper or TranscodingMapper(JSONTranscoder())
        super().__init__(mapper)
        self.db_name = db_name or ":memory:"
        self.queue_insert_statement = f"INSERT INTO {self.queue_table_name} (message_id, posted_at, topic, state) VALUES (?,?,?,?)"
        self.queue_select_statement = f"SELECT * FROM {self.queue_table_name} WHERE message_id IN (SELECT message_id FROM {self.lock_table_name} WHERE lock_id=?) ORDER BY posted_at ASC"
        self.connection = sqlite3.connect(
            self.db_name,
            check_same_thread=False,
            detect_types=sqlite3.PARSE_DECLTYPES
            | sqlite3.PARSE_COLNAMES,  # required for "native" date/time conversion
        )

    @contextmanager
    def transaction(self):
        """Unit of work/transaction"""
        cursor = self.connection.cursor()
        yield cursor
        self.connection.commit()

    def table_exists(self, table_name):
        """Returns True if table exists and Flase otherwise"""
        with self.transaction() as trans:
            trans.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                [table_name],
            )
            return bool(trans.fetchall())

    def row_count(self, table_name):
        """Returns number of rows in a table"""
        with self.transaction() as trans:
            trans.execute(f"SELECT COUNT(*) FROM {table_name}")
            return trans.fetchone()[0]

    def initialize(self):
        with self.transaction() as trans:
            create_queue_table_sql = (
                "CREATE TABLE IF NOT EXISTS "
                f"{self.queue_table_name} ("
                "message_id TEXT, "
                "posted_at timestamp, "
                "topic TEXT, "
                "state BLOB, "
                "PRIMARY KEY "
                "(message_id))"
            )
            create_lock_table_sql = (
                "CREATE TABLE IF NOT EXISTS "
                f"    {self.lock_table_name} ("
                "        message_id TEXT, "
                "        lock_id TExT, "
                "        locked_at timestamp, "
                "        PRIMARY KEY "
                "            (lock_id, message_id))"
            )

            trans.execute(create_queue_table_sql)
            trans.execute(create_lock_table_sql)

    def _insert(self, item: StoredMessage):
        with self.transaction() as curs:
            params = [item.message_id, item.posted_at, item.topic, item.state]
            curs.execute(self.queue_insert_statement, params)

    def _select(self, batch_limit: int = None) -> List[StoredMessage]:
        lock_id = uuid4().hex
        lock_sql = (
            f"INSERT INTO {self.lock_table_name} (lock_id, message_id, locked_at) \n"
            f"        SELECT ?, message_id, ? FROM {self.queue_table_name} \n"
            f"         WHERE message_id NOT IN (SELECT message_id FROM {self.lock_table_name}) \n"
            f"         ORDER BY posted_at ASC \n"
            f"         LIMIT ?"
        )
        batch_limit = batch_limit or 1
        with self.transaction() as curs:
            curs.execute(lock_sql, [lock_id, datetime.utcnow(), batch_limit])
        with self.transaction() as curs:
            params = [lock_id]
            curs.execute(self.queue_select_statement, params)
            return [CursorFactory.as_message(curs, row) for row in curs.fetchall()]

    def _delete(self, item: StoredMessage):
        with self.transaction() as curs:
            curs.execute(
                f"DELETE FROM {self.lock_table_name} WHERE message_id=?",
                [item.message_id],
            )
            curs.execute(
                f"DELETE FROM {self.queue_table_name} WHERE message_id=?",
                [item.message_id],
            )
