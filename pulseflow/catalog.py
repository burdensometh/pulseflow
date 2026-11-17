from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
import json
import sqlite3
import uuid

from .values import Missing, canonical, utc_now


def compare_decimal(left, right):
    """Numeric ordering over canonical decimal text for exact window ranks."""
    first, second = Decimal(left), Decimal(right)
    return (first > second) - (first < second)


DDL = """
CREATE TABLE IF NOT EXISTS system_identity (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 instance_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS channels (
 name TEXT PRIMARY KEY,
 partitions INTEGER NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS source_positions (
 channel TEXT NOT NULL REFERENCES channels(name),
 partition_id INTEGER NOT NULL,
 next_offset INTEGER NOT NULL,
 PRIMARY KEY(channel,partition_id)
);
CREATE TABLE IF NOT EXISTS journal (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 channel TEXT NOT NULL REFERENCES channels(name),
 partition_id INTEGER NOT NULL,
 source_offset INTEGER NOT NULL,
 event_id TEXT NOT NULL,
 event_key TEXT NOT NULL,
 event_time INTEGER NOT NULL,
 document TEXT NOT NULL,
 checksum TEXT NOT NULL,
 UNIQUE(channel,partition_id,source_offset),
 UNIQUE(channel,event_id)
);
CREATE INDEX IF NOT EXISTS journal_channel_sequence ON journal(channel,sequence);
CREATE INDEX IF NOT EXISTS journal_key_time ON journal(channel,event_key,event_time);
CREATE TABLE IF NOT EXISTS ingress_receipts (
 channel TEXT NOT NULL,
 batch_id TEXT NOT NULL,
 fingerprint TEXT NOT NULL,
 accepted INTEGER NOT NULL,
 first_sequence INTEGER,
 last_sequence INTEGER,
 created_at TEXT NOT NULL,
 PRIMARY KEY(channel,batch_id)
);
CREATE TABLE IF NOT EXISTS pipelines (
 name TEXT PRIMARY KEY,
 channel TEXT NOT NULL REFERENCES channels(name),
 plan TEXT NOT NULL,
 plan_hash TEXT NOT NULL,
 buckets INTEGER NOT NULL,
 hash_version INTEGER NOT NULL,
 watermark INTEGER,
 last_sequence INTEGER NOT NULL DEFAULT 0,
 generation INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS progress (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 partition_id INTEGER NOT NULL,
 next_offset INTEGER NOT NULL DEFAULT 0,
 max_time INTEGER,
 declared_time INTEGER,
 PRIMARY KEY(pipeline,partition_id)
);
CREATE TABLE IF NOT EXISTS window_events (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 bucket INTEGER NOT NULL,
 window_start INTEGER NOT NULL,
 group_key TEXT NOT NULL,
 event_id TEXT NOT NULL,
 event_time INTEGER NOT NULL,
 value TEXT,
 document TEXT NOT NULL,
 PRIMARY KEY(pipeline,bucket,window_start,group_key,event_id)
);
CREATE TABLE IF NOT EXISTS window_status (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 bucket INTEGER NOT NULL,
 window_start INTEGER NOT NULL,
 window_end INTEGER NOT NULL,
 group_key TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('open','closed')),
 result_revision INTEGER,
 PRIMARY KEY(pipeline,bucket,window_start,group_key)
);
CREATE TABLE IF NOT EXISTS keyed_state (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 bucket INTEGER NOT NULL,
 state_kind TEXT NOT NULL,
 group_key TEXT NOT NULL,
 document TEXT NOT NULL,
 PRIMARY KEY(pipeline,bucket,state_kind,group_key)
);
CREATE TABLE IF NOT EXISTS seen_keys (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 dedup_key TEXT NOT NULL,
 expires_at INTEGER NOT NULL,
 event_id TEXT NOT NULL,
 PRIMARY KEY(pipeline,dedup_key)
);
CREATE TABLE IF NOT EXISTS late_events (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 event_id TEXT NOT NULL,
 source_sequence INTEGER NOT NULL,
 event_time INTEGER NOT NULL,
 watermark INTEGER NOT NULL,
 document TEXT NOT NULL,
 PRIMARY KEY(pipeline,event_id)
);
CREATE TABLE IF NOT EXISTS processed_events (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 event_id TEXT NOT NULL,
 source_sequence INTEGER NOT NULL,
 disposition TEXT NOT NULL,
 transformed TEXT,
 dimension_version INTEGER,
 PRIMARY KEY(pipeline,event_id)
);
CREATE TABLE IF NOT EXISTS results (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 result_key TEXT NOT NULL,
 revision INTEGER NOT NULL,
 document TEXT NOT NULL,
 deleted INTEGER NOT NULL,
 PRIMARY KEY(pipeline,result_key)
);
CREATE TABLE IF NOT EXISTS changes (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 result_key TEXT NOT NULL,
 revision INTEGER NOT NULL,
 operation TEXT NOT NULL CHECK(operation IN ('upsert','delete')),
 document TEXT NOT NULL,
 UNIQUE(pipeline,result_key,revision)
);
CREATE INDEX IF NOT EXISTS changes_pipeline_sequence ON changes(pipeline,sequence);
CREATE TABLE IF NOT EXISTS worker_leases (
 pipeline TEXT PRIMARY KEY REFERENCES pipelines(name),
 owner TEXT,
 fence INTEGER NOT NULL,
 expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS dimension_versions (
 version_id INTEGER PRIMARY KEY AUTOINCREMENT,
 dimension_name TEXT NOT NULL,
 entity_key TEXT NOT NULL,
 valid_from INTEGER NOT NULL,
 revision INTEGER NOT NULL,
 document TEXT NOT NULL,
 deleted INTEGER NOT NULL,
 UNIQUE(dimension_name,entity_key,valid_from,revision)
);
CREATE INDEX IF NOT EXISTS dimension_asof ON dimension_versions(dimension_name,entity_key,valid_from,revision);
CREATE TABLE IF NOT EXISTS dimension_receipts (
 batch_id TEXT PRIMARY KEY,
 fingerprint TEXT NOT NULL,
 versions TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS engine_runs (
 run_id TEXT PRIMARY KEY,
 pipeline TEXT NOT NULL,
 from_sequence INTEGER NOT NULL,
 to_sequence INTEGER NOT NULL,
 processed INTEGER NOT NULL,
 details TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS saved_points (
 point_id TEXT PRIMARY KEY,
 pipeline TEXT NOT NULL,
 filename TEXT NOT NULL,
 sha256 TEXT NOT NULL,
 generation INTEGER NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file_receipts (
 source_id TEXT PRIMARY KEY,
 channel TEXT NOT NULL,
 partition_id INTEGER NOT NULL,
 byte_offset INTEGER NOT NULL,
 prefix_sha256 TEXT NOT NULL,
 next_offset INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS dataset_exports (
 export_id TEXT PRIMARY KEY,
 pipeline TEXT NOT NULL,
 filename TEXT NOT NULL,
 sha256 TEXT NOT NULL,
 row_count INTEGER NOT NULL,
 last_change INTEGER NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 subject TEXT NOT NULL,
 action TEXT NOT NULL,
 detail TEXT NOT NULL,
 created_at TEXT NOT NULL
);
"""


class Catalog:
    def __init__(self, home):
        self.home = Path(home).resolve()
        self.home.mkdir(parents=True, exist_ok=True)
        self.path = self.home / "catalog.sqlite"
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(DDL)
            connection.execute(
                "INSERT OR IGNORE INTO system_identity VALUES (1,?)",
                (uuid.uuid4().hex,),
            )

    def identity(self):
        with self.transaction() as connection:
            return connection.execute(
                "SELECT instance_id FROM system_identity WHERE singleton=1"
            ).fetchone()[0]

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=20, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.create_collation("decimal_exact", compare_decimal)
        return connection

    @contextmanager
    def transaction(self, write=False):
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def channel(self, name, connection=None):
        if connection is None:
            with self.transaction() as connection:
                return self.channel(name, connection)
        row = connection.execute(
            "SELECT * FROM channels WHERE name=?", (name,)
        ).fetchone()
        if row is None:
            raise Missing("channel does not exist")
        return dict(row)

    def pipeline(self, name, connection=None):
        if connection is None:
            with self.transaction() as connection:
                return self.pipeline(name, connection)
        row = connection.execute(
            "SELECT * FROM pipelines WHERE name=?", (name,)
        ).fetchone()
        if row is None:
            raise Missing("pipeline does not exist")
        result = dict(row)
        result["plan"] = json.loads(result["plan"])
        return result

    def event(self, connection, subject, action, detail):
        connection.execute(
            "INSERT INTO audit_events(subject,action,detail,created_at) VALUES (?,?,?,?)",
            (subject, action, canonical(detail), utc_now()),
        )

    def history(self, subject):
        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM audit_events WHERE subject=? ORDER BY sequence",
                (subject,),
            )
            return [{**dict(row), "detail": json.loads(row["detail"])} for row in rows]
