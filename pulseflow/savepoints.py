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


V1_FORMAT = 1
V2_FORMAT = 2
SUPPORTED_FORMATS = {V1_FORMAT, V2_FORMAT}

# Physical state tables and their stored columns.  v1 savepoints pin every
# keyed row to its original state bucket, which makes the format impossible to
# restore under a different bucket count.
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

# v2 stores bucketed rows without a fixed bucket: ownership is derived from the
# group key against whatever bucket count the destination pipeline uses, so a
# savepoint can be restored into a differently sharded pipeline.
V2_STATE_TABLES = {
    table: [column for column in columns if column != "bucket"]
    for table, columns in STATE_TABLES.items()
}

BUCKETED_TABLES = ("window_events", "window_status", "keyed_state")
BUCKET_GROUP_COLUMN = {
    "window_events": "group_key",
    "window_status": "group_key",
    "keyed_state": "group_key",
}

# Logical primary-key columns of each state table, independent of the physical
# bucket prefix.  Duplicates here would violate a table constraint while the
# destination pipeline is being built, so they are rejected up front.
LOGICAL_KEYS = {
    "progress": ["partition_id"],
    "window_events": ["window_start", "group_key", "event_id"],
    "window_status": ["window_start", "group_key"],
    "keyed_state": ["state_kind", "group_key"],
    "seen_keys": ["dedup_key"],
    "late_events": ["event_id"],
    "processed_events": ["event_id"],
    "results": ["result_key"],
    "changes": ["result_key", "revision"],
}


def logical_columns(version, table):
    return (
        V2_STATE_TABLES[table]
        if version == V2_FORMAT
        else STATE_TABLES[table]
    )


def layout_columns(version, table):
    """Columns of a state row as it actually appears inside a savepoint."""
    return logical_columns(version, table)


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
            for table, physical_columns in STATE_TABLES.items():
                columns = logical_columns(V2_FORMAT, table)
                order = "sequence" if table == "changes" else ",".join(columns)
                sql = (
                    f"SELECT {','.join(columns)} FROM {table} "
                    "WHERE pipeline=? ORDER BY " + order
                )
                tables[table] = [
                    list(row) for row in connection.execute(sql, (pipeline,))
                ]
            document = {
                "format_version": V2_FORMAT,
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
            "format_version": V2_FORMAT,
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
            or document.get("format_version") not in SUPPORTED_FORMATS
        ):
            raise Invalid("unsupported savepoint format")
        version = document["format_version"]
        info = document["pipeline"]
        if not isinstance(info, dict):
            raise Invalid("savepoint pipeline is invalid")
        plan = plan_spec(info["plan"])
        if fingerprint(plan) != info["plan_hash"]:
            raise Conflict("savepoint plan fingerprint mismatch")
        source_buckets = integer(info["buckets"], "savepoint state buckets", 1, 128)
        if info["hash_version"] != 1:
            raise Invalid("unsupported savepoint state hash")
        integer(info["last_sequence"], "savepoint cursor", 0)
        integer(info["generation"], "savepoint generation", 0)
        state = document["state"]
        expected_tables = set(STATE_TABLES)
        if not isinstance(state, dict) or set(state) != expected_tables:
            raise Invalid("savepoint state sections mismatch")
        total = 0
        for table in expected_tables:
            columns = layout_columns(version, table)
            rows = state[table]
            if not isinstance(rows, list):
                raise Invalid("savepoint state must contain arrays")
            total += len(rows)
            group_column = BUCKET_GROUP_COLUMN.get(table)
            key_indexes = [columns.index(name) for name in LOGICAL_KEYS[table]]
            seen_keys = set()
            for row in rows:
                if not isinstance(row, list) or len(row) != len(columns):
                    raise Invalid("savepoint state row layout mismatch")
                named = dict(zip(columns, row))
                logical_key = tuple(row[index] for index in key_indexes)
                if logical_key in seen_keys:
                    raise Invalid("savepoint state contains a duplicate logical key")
                seen_keys.add(logical_key)
                if version == V1_FORMAT and table in BUCKETED_TABLES:
                    expected = bucket_for(
                        named[group_column], source_buckets, info["hash_version"]
                    )
                    if named["bucket"] != expected:
                        raise Conflict("savepoint key is in the wrong state bucket")
                for field in ["document", "transformed"]:
                    if field in named and named[field] is not None:
                        json.loads(named[field])
        if version == V2_FORMAT:
            self._validate_v2_references(state)
        if total > 1000000:
            raise Invalid("savepoint row count exceeds limit")
        partitions = integer(
            document["channel_partitions"], "channel partitions", 1, 128
        )
        actual = sorted(row[0] for row in state["progress"])
        if actual != list(range(partitions)):
            raise Invalid("savepoint partition progress is incomplete")
        return document

    def _validate_v2_references(self, state):
        # v2 dropped the physical bucket column; make sure the logical window
        # state stays internally consistent without it.
        # v2 logical columns:
        #   window_events: [window_start, group_key, event_id, event_time,
        #                   value, document]
        #   window_status: [window_start, window_end, group_key, status,
        #                   result_revision]
        events = {(row[0], row[1]) for row in state["window_events"]}
        statuses = {(row[0], row[2]) for row in state["window_status"]}
        if statuses != events:
            raise Invalid("savepoint window state references are inconsistent")
        if any(row[3] not in ("open", "closed") for row in state["window_status"]):
            raise Invalid("savepoint window status is invalid")

    def restore(self, document, destination, buckets=None):
        self.validate(document)
        version = document["format_version"]
        destination = identifier(destination)
        info = document["pipeline"]
        target_buckets = info["buckets"] if buckets is None else integer(
            buckets, "state buckets", 1, 128
        )
        if version == V1_FORMAT and target_buckets != info["buckets"]:
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
                    target_buckets,
                    info["hash_version"],
                    info["watermark"],
                    info["last_sequence"],
                    info["generation"],
                    utc_now(),
                ),
            )
            for table, physical_columns in STATE_TABLES.items():
                stored_columns = layout_columns(version, table)
                group_column = BUCKET_GROUP_COLUMN.get(table)
                rows = []
                for row in document["state"][table]:
                    named = dict(zip(stored_columns, row))
                    if table in BUCKETED_TABLES:
                        bucket = bucket_for(
                            named[group_column],
                            target_buckets,
                            info["hash_version"],
                        )
                        if version == V1_FORMAT:
                            # Keep the full v1 row shape; only the bucket slot
                            # is rewritten to the destination layout.
                            rewritten = list(row)
                            rewritten[0] = bucket
                            rows.append(rewritten)
                        else:
                            rows.append([bucket, *row])
                    else:
                        rows.append(list(row))
                names = ["pipeline"] + physical_columns
                placeholders = ",".join("?" for _ in names)
                sql = f'INSERT INTO {table}({",".join(names)}) VALUES ({placeholders})'
                connection.executemany(
                    sql, ([destination] + row for row in rows)
                )
            self.catalog.event(
                connection,
                destination,
                "savepoint_restored",
                {
                    "point_id": document["point_id"],
                    "source": info["name"],
                    "format_version": version,
                    "source_buckets": info["buckets"],
                    "buckets": target_buckets,
                    "rescaled": target_buckets != info["buckets"],
                },
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
