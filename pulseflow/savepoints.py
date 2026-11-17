import json
import uuid

from .spec import plan_spec
from .state import bucket_for
from .values import (
    Conflict,
    Invalid,
    Missing,
    canonical,
    fingerprint,
    identifier,
    integer,
    utc_now,
)


STATE_TABLES = {
    "progress": ["partition_id", "next_offset", "max_time", "declared_time"],
    "window_events": [
        "bucket",
        "window_start",
        "group_key",
        "event_id",
        "event_time",
        "value",
        "document",
    ],
    "window_status": [
        "bucket",
        "window_start",
        "window_end",
        "group_key",
        "status",
        "result_revision",
    ],
    "session_events": [
        "bucket",
        "session_start",
        "group_key",
        "event_id",
        "event_time",
        "value",
        "document",
    ],
    "session_status": [
        "bucket",
        "session_start",
        "session_end",
        "group_key",
        "status",
        "result_revision",
    ],
    "keyed_state": ["bucket", "state_kind", "group_key", "document"],
    "seen_keys": ["dedup_key", "expires_at", "event_id"],
    "late_events": [
        "event_id",
        "source_sequence",
        "event_time",
        "watermark",
        "document",
    ],
    "processed_events": [
        "event_id",
        "source_sequence",
        "disposition",
        "transformed",
        "dimension_version",
    ],
    "results": ["result_key", "revision", "document", "deleted"],
    "changes": ["result_key", "revision", "operation", "document"],
}


class Savepoints:
    def __init__(self, catalog, journal, artifacts):
        self.catalog = catalog
        self.journal = journal
        self.artifacts = artifacts

    def create(self, pipeline, compressed=False):
        point_id = uuid.uuid4().hex
        with self.catalog.transaction() as connection:
            info = self.catalog.pipeline(pipeline, connection)
            source = self.catalog.channel(info["channel"], connection)
            tables = {}
            for table, columns in STATE_TABLES.items():
                order = "sequence" if table == "changes" else ",".join(columns)
                sql = f'SELECT {",".join(columns)} FROM {table} WHERE pipeline=? ORDER BY {order}'
                tables[table] = [
                    list(row) for row in connection.execute(sql, (pipeline,))
                ]
            document = {
                "format_version": 1,
                "point_id": point_id,
                "pipeline": info,
                "channel_partitions": source["partitions"],
                "source_prefix_sha256": self.journal.digest_prefix(
                    connection, info["channel"], info["last_sequence"]
                ),
                "state": tables,
            }
        artifact = self.artifacts.put_document(
            point_id + (".json.gz" if compressed else ".json"), document, compressed
        )
        with self.catalog.transaction(True) as connection:
            connection.execute(
                "INSERT INTO saved_points VALUES (?,?,?,?,?,?)",
                (
                    point_id,
                    pipeline,
                    artifact["filename"],
                    artifact["sha256"],
                    info["generation"],
                    utc_now(),
                ),
            )
        return {
            **artifact,
            "point_id": point_id,
            "generation": info["generation"],
            "compressed": compressed,
        }

    def get(self, point_id):
        with self.catalog.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM saved_points WHERE point_id=?", (point_id,)
            ).fetchone()
            if row is None:
                raise Missing("savepoint does not exist")
            return dict(row)

    def load(self, point_id):
        info = self.get(point_id)
        return self.artifacts.read_document(
            info["filename"], info["sha256"], info["filename"].endswith(".gz")
        )

    def validate(self, document):
        required = {
            "format_version",
            "point_id",
            "pipeline",
            "channel_partitions",
            "source_prefix_sha256",
            "state",
        }
        if (
            not isinstance(document, dict)
            or set(document) != required
            or document["format_version"] != 1
        ):
            raise Invalid("unsupported savepoint format")
        info = document["pipeline"]
        if not isinstance(info, dict):
            raise Invalid("savepoint pipeline is invalid")
        plan = plan_spec(info["plan"])
        if fingerprint(plan) != info["plan_hash"]:
            raise Conflict("savepoint plan fingerprint mismatch")
        buckets = integer(info["buckets"], "savepoint state buckets", 1, 128)
        if info["hash_version"] != 1:
            raise Invalid("unsupported savepoint state hash")
        integer(info["last_sequence"], "savepoint cursor", 0)
        integer(info["generation"], "savepoint generation", 0)
        state = document["state"]
        if not isinstance(state, dict) or set(state) != set(STATE_TABLES):
            raise Invalid("savepoint state sections mismatch")
        total = 0
        for table, columns in STATE_TABLES.items():
            rows = state[table]
            if not isinstance(rows, list):
                raise Invalid("savepoint state must contain arrays")
            total += len(rows)
            for row in rows:
                if not isinstance(row, list) or len(row) != len(columns):
                    raise Invalid("savepoint state row layout mismatch")
                named = dict(zip(columns, row))
                if "bucket" in named:
                    expected = bucket_for(
                        named["group_key"], buckets, info["hash_version"]
                    )
                    if named["bucket"] != expected:
                        raise Conflict("savepoint key is in the wrong state bucket")
                for field in ["document", "transformed"]:
                    if named.get(field) is not None:
                        json.loads(named[field])
        if total > 1000000:
            raise Invalid("savepoint row count exceeds limit")
        partitions = integer(
            document["channel_partitions"], "channel partitions", 1, 128
        )
        actual = sorted(row[0] for row in state["progress"])
        if actual != list(range(partitions)):
            raise Invalid("savepoint partition progress is incomplete")
        return document

    def restore(self, document, destination, buckets=None):
        self.validate(document)
        destination = identifier(destination)
        info = document["pipeline"]
        if buckets is not None and buckets != info["buckets"]:
            raise Conflict("v1 savepoint cannot rescale keyed state")
        with self.catalog.transaction(True) as connection:
            channel = self.catalog.channel(info["channel"], connection)
            if channel["partitions"] != document["channel_partitions"]:
                raise Conflict("savepoint source partition layout mismatch")
            if (
                self.journal.digest_prefix(
                    connection, info["channel"], info["last_sequence"]
                )
                != document["source_prefix_sha256"]
            ):
                raise Conflict("savepoint source prefix is unavailable or changed")
            if connection.execute(
                "SELECT 1 FROM pipelines WHERE name=?", (destination,)
            ).fetchone():
                raise Conflict("savepoint destination must be a new pipeline")
            connection.execute(
                "INSERT INTO pipelines VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    destination,
                    info["channel"],
                    canonical(info["plan"]),
                    info["plan_hash"],
                    info["buckets"],
                    info["hash_version"],
                    info["watermark"],
                    info["last_sequence"],
                    info["generation"],
                    utc_now(),
                ),
            )
            for table, columns in STATE_TABLES.items():
                names = ["pipeline"] + columns
                placeholders = ",".join("?" for _ in names)
                sql = f'INSERT INTO {table}({",".join(names)}) VALUES ({placeholders})'
                connection.executemany(
                    sql, ([destination] + row for row in document["state"][table])
                )
            self.catalog.event(
                connection,
                destination,
                "savepoint_restored",
                {"point_id": document["point_id"], "source": info["name"]},
            )
        return self.catalog.pipeline(destination)

    def restore_point(self, point_id, destination, buckets=None):
        return self.restore(self.load(point_id), destination, buckets)

    def list(self, pipeline):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM saved_points WHERE pipeline=? ORDER BY created_at,point_id",
                    (pipeline,),
                )
            ]
