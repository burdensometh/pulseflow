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


# State owned by the engine itself rather than by any operator.  Operators
# declare the state they own; these tables carry pipeline progress, event
# dispositions and published results.
ENGINE_TABLES = {
    "progress": ["partition_id", "next_offset", "max_time", "declared_time"],
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

# The fixed table layout written by savepoints that predate operator
# ownership.  Such documents are still accepted; their rows are assigned to
# the operator that now owns them.
LEGACY_STATE_TABLES = {
    "progress": ENGINE_TABLES["progress"],
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
    "late_events": ENGINE_TABLES["late_events"],
    "processed_events": ENGINE_TABLES["processed_events"],
    "results": ENGINE_TABLES["results"],
    "changes": ENGINE_TABLES["changes"],
}


class Savepoints:
    def __init__(self, catalog, journal, artifacts, operators):
        self.catalog = catalog
        self.journal = journal
        self.artifacts = artifacts
        self.operators = operators

    # -- layout -----------------------------------------------------------
    def _derived_tables(self, plan):
        """State tables for a plan, taken from the operators that apply."""

        tables = dict(ENGINE_TABLES)
        for operator in self.operators.applicable(plan):
            for table, columns in operator.state_tables.items():
                tables[table] = columns
        return tables

    def _read_tables(self, connection, pipeline, tables):
        section = {}
        for table, columns in tables.items():
            order = "sequence" if table == "changes" else ",".join(columns)
            sql = (
                f'SELECT {",".join(columns)} FROM {table} WHERE pipeline=? '
                f"ORDER BY {order}"
            )
            section[table] = [list(row) for row in connection.execute(sql, (pipeline,))]
        return section

    @staticmethod
    def _check_rows(table, columns, rows):
        if not isinstance(rows, list):
            raise Invalid("savepoint state must contain arrays")
        for row in rows:
            if not isinstance(row, list) or len(row) != len(columns):
                raise Invalid("savepoint state row layout mismatch")
            named = dict(zip(columns, row))
            for value in ("document", "transformed"):
                if named.get(value) is not None:
                    json.loads(named[value])

    def _claimants(self, plan, table, columns, row):
        owners = []
        for operator in self.operators.applicable(plan):
            if operator.state_tables.get(table) != columns:
                continue
            if operator.claims_row(table, columns, row):
                owners.append(operator)
        return owners

    def _assert_ownership(self, connection, info):
        """Reject live state that no active operator owns.

        Dropping such rows from a savepoint would make a restore appear to
        succeed while losing state, so snapshotting fails instead.
        """

        tables = {}
        for operator in self.operators:
            for table, columns in operator.state_tables.items():
                tables[table] = columns
        for table, columns in tables.items():
            sql = f'SELECT {",".join(columns)} FROM {table} WHERE pipeline=?'
            for row in connection.execute(sql, (info["name"],)):
                values = list(row)
                owners = self._claimants(info["plan"], table, columns, values)
                if not owners:
                    raise Invalid(
                        "pipeline state has no active owner in table " + table
                    )
                if len(owners) > 1:
                    raise Invalid("pipeline state row has ambiguous ownership")

    def _version(self, plan):
        """v1 documents assume operator state version 1.

        A plan whose operators declare a newer state version needs the v2
        layout, where each operator's version is written explicitly.
        """

        for operator in self.operators.applicable(plan):
            if operator.schema_version != 1:
                return 2
        return 1

    # -- creation ---------------------------------------------------------
    def create(self, pipeline, compressed=False):
        point_id = uuid.uuid4().hex
        with self.catalog.transaction() as connection:
            info = self.catalog.pipeline(pipeline, connection)
            source = self.catalog.channel(info["channel"], connection)
            plan = info["plan"]
            self._assert_ownership(connection, info)
            active = self.operators.applicable(plan)
            version = self._version(plan)
            if version == 1:
                # Table-oriented layout: every active operator contributes its
                # declared tables, so the state list is never hardcoded.
                tables = self._read_tables(
                    connection, pipeline, self._derived_tables(plan)
                )
            else:
                tables = self._read_tables(connection, pipeline, ENGINE_TABLES)
            document = {
                "format_version": version,
                "point_id": point_id,
                "pipeline": info,
                "channel_partitions": source["partitions"],
                "source_prefix_sha256": self.journal.digest_prefix(
                    connection, info["channel"], info["last_sequence"]
                ),
                "state": tables,
            }
            if version == 2:
                document["operators"] = {
                    operator.name: {
                        "schema_version": operator.schema_version,
                        "state": operator.snapshot_state(connection, info),
                    }
                    for operator in active
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

    # -- reading -----------------------------------------------------------
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

    # -- validation --------------------------------------------------------
    def _validate_pipeline(self, document):
        info = document.get("pipeline")
        if not isinstance(info, dict):
            raise Invalid("savepoint pipeline is invalid")
        plan = plan_spec(info["plan"], self.operators.plan_extensions())
        if fingerprint(plan) != info.get("plan_hash"):
            raise Conflict("savepoint plan fingerprint mismatch")
        buckets = integer(info["buckets"], "savepoint state buckets", 1, 128)
        if info["hash_version"] != 1:
            raise Invalid("unsupported savepoint state hash")
        integer(info["last_sequence"], "savepoint cursor", 0)
        integer(info["generation"], "savepoint generation", 0)
        return info, plan, buckets

    def _validate_v1(self, document, info, plan, buckets):
        state = document["state"]
        if not isinstance(state, dict):
            raise Invalid("savepoint state sections mismatch")
        derived = self._derived_tables(plan)
        if set(state) == set(derived):
            expected = derived
        elif set(state) == set(LEGACY_STATE_TABLES):
            expected = LEGACY_STATE_TABLES
        else:
            known = set(ENGINE_TABLES) | set(derived) | set(LEGACY_STATE_TABLES)
            unknown = [table for table in state if table not in known]
            if unknown:
                raise Invalid(
                    "savepoint state has no active operator for: "
                    + ", ".join(sorted(unknown))
                )
            missing = [table for table in derived if table not in state]
            raise Invalid(
                "savepoint state is missing operator tables: "
                + ", ".join(sorted(missing))
            )
        for operator in self.operators.applicable(plan):
            if operator.schema_version != 1:
                raise Conflict(
                    "savepoint state version for operator "
                    + operator.name
                    + " is incompatible"
                )
            # Every active operator's state must be represented; otherwise a
            # restore would silently start that operator with empty state.
            missing = set(operator.state_tables) - set(expected)
            if missing:
                raise Invalid(
                    "savepoint is missing operator state for "
                    + operator.name
                    + ": "
                    + ", ".join(sorted(missing))
                )
        return self._validate_tables(state, expected, plan, buckets)

    def _validate_tables(self, state, expected, plan, buckets):
        engine = {}
        sections = {}
        total = 0
        active = self.operators.applicable(plan)
        for table, rows in state.items():
            columns = expected.get(table)
            if columns is None:
                raise Invalid("savepoint contains an unknown state section")
            self._check_rows(table, columns, rows)
            total += len(rows)
            if table in ENGINE_TABLES:
                engine[table] = rows
                continue
            for row in rows:
                named = dict(zip(columns, row))
                if "bucket" in named:
                    expected_bucket = bucket_for(named["group_key"], buckets, 1)
                    if named["bucket"] != expected_bucket:
                        raise Conflict(
                            "savepoint key is in the wrong state bucket"
                        )
                owners = self._claimants(plan, table, columns, row)
                if not owners:
                    raise Invalid(
                        "savepoint state has no active owner in table " + table
                    )
                if len(owners) > 1:
                    raise Invalid(
                        "savepoint state row has ambiguous operator ownership"
                    )
                sections.setdefault(owners[0].name, {}).setdefault(table, []).append(
                    row
                )
        if total > 1000000:
            raise Invalid("savepoint row count exceeds limit")
        for table in ENGINE_TABLES:
            if table not in engine:
                raise Invalid("savepoint is missing engine state: " + table)
        context = {"buckets": buckets, "hash_version": 1}
        complete = {}
        for operator in active:
            section = {
                table: sections.get(operator.name, {}).get(table, [])
                for table in operator.state_tables
            }
            operator.validate_state(context, section)
            complete[operator.name] = section
        return engine, complete

    def _validate_v2(self, document, info, plan, buckets):
        state = document["state"]
        if not isinstance(state, dict) or set(state) != set(ENGINE_TABLES):
            raise Invalid("savepoint state sections mismatch")
        for table, columns in ENGINE_TABLES.items():
            self._check_rows(table, columns, state[table])
        saved = document.get("operators")
        if not isinstance(saved, dict):
            raise Invalid("savepoint operator sections must be a mapping")
        active = {operator.name: operator for operator in self.operators.applicable(plan)}
        unknown = set(saved) - set(active)
        if unknown:
            raise Invalid(
                "savepoint references unknown or inactive operators: "
                + ", ".join(sorted(unknown))
            )
        missing = set(active) - set(saved)
        if missing:
            raise Invalid(
                "savepoint is missing operator state for: "
                + ", ".join(sorted(missing))
            )
        context = {"buckets": buckets, "hash_version": 1}
        sections = {}
        total = sum(len(rows) for rows in state.values())
        for name, operator in active.items():
            payload = saved[name]
            if not isinstance(payload, dict) or set(payload) != {
                "schema_version",
                "state",
            }:
                raise Invalid("savepoint operator section layout mismatch")
            if payload["schema_version"] != operator.schema_version:
                raise Conflict(
                    "savepoint state version for operator "
                    + name
                    + " is incompatible"
                )
            operator.validate_state(context, payload["state"])
            sections[name] = payload["state"]
            total += sum(len(rows) for rows in payload["state"].values())
        if total > 1000000:
            raise Invalid("savepoint row count exceeds limit")
        return state, sections

    def validate(self, document):
        if not isinstance(document, dict):
            raise Invalid("unsupported savepoint format")
        version = document.get("format_version")
        common = {
            "format_version",
            "point_id",
            "pipeline",
            "channel_partitions",
            "source_prefix_sha256",
            "state",
        }
        if version == 1:
            if set(document) != common:
                raise Invalid("unsupported savepoint format")
            info, plan, buckets = self._validate_pipeline(document)
            self._validate_v1(document, info, plan, buckets)
        elif version == 2:
            if set(document) != common | {"operators"}:
                raise Invalid("unsupported savepoint format")
            info, plan, buckets = self._validate_pipeline(document)
            self._validate_v2(document, info, plan, buckets)
        else:
            raise Invalid("unsupported savepoint format")
        partitions = integer(
            document["channel_partitions"], "channel partitions", 1, 128
        )
        actual = sorted(row[0] for row in document["state"]["progress"])
        if actual != list(range(partitions)):
            raise Invalid("savepoint partition progress is incomplete")
        return document

    # -- restore -----------------------------------------------------------
    def restore(self, document, destination, buckets=None):
        self.validate(document)
        destination = identifier(destination)
        info = document["pipeline"]
        plan = plan_spec(info["plan"], self.operators.plan_extensions())
        if buckets is not None and buckets != info["buckets"]:
            raise Conflict("v1 savepoint cannot rescale keyed state")
        if document["format_version"] == 1:
            engine, sections = self._validate_v1(
                document, info, plan, info["buckets"]
            )
        else:
            engine, sections = self._validate_v2(
                document, info, plan, info["buckets"]
            )
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
            for table, columns in ENGINE_TABLES.items():
                names = ["pipeline"] + columns
                placeholders = ",".join("?" for _ in names)
                sql = f'INSERT INTO {table}({",".join(names)}) VALUES ({placeholders})'
                connection.executemany(
                    sql, ([destination] + row for row in engine[table])
                )
            restored = {
                "name": destination,
                "buckets": info["buckets"],
                "hash_version": info["hash_version"],
            }
            for operator in self.operators.applicable(plan):
                operator.restore_state(connection, restored, sections[operator.name])
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
