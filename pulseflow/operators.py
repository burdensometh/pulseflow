import json
import re

from .expressions import field
from .state import bucket_for
from .values import Invalid, canonical, identifier, integer
from .windows import starts_for


EVENT_DISPOSITIONS = {"accepted", "duplicate", "filtered"}

KEYED_COLUMNS = ["bucket", "state_kind", "group_key", "document"]

_SQL_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")


def _sql_name(value):
    if not isinstance(value, str) or not _SQL_NAME.fullmatch(value):
        raise Invalid("operator state table and column names must be SQL identifiers")
    return value


def _decode(document):
    try:
        return json.loads(document)
    except (TypeError, ValueError) as error:
        raise Invalid("operator state document is not valid JSON") from error


class Operator:
    """A self-contained stateful stage.

    An operator owns every state row it writes, declares the tables and
    columns carrying that state, and reacts to the engine lifecycle: one event
    at a time, watermark movement, and savepoint save/restore.  The engine and
    savepoint layer only know this protocol; they never branch on a concrete
    operator type.

    ``process`` returns ``None`` to pass the event down the chain, or one of
    :data:`EVENT_DISPOSITIONS` to stop the chain and record that disposition.
    ``watermark`` may return an integer to report flushed output.
    """

    name = None
    schema_version = 1
    event_order = 100
    watermark_order = 100
    state_kind = None
    state_tables = {}
    ddl = ()
    plan_key = None
    plan_validator = None

    def applies(self, plan):
        return True

    def process(self, connection, pipeline, row):
        return None

    def watermark(self, connection, pipeline):
        return None

    def check(self, connection, pipeline, problems):
        return None

    # -- ownership symmetry -------------------------------------------------
    def claims_row(self, table, columns, row):
        """Whether this operator owns one row of a shared table.

        Operators that share a table (for example the keyed ``latest`` and
        ``sequence`` operators sharing ``keyed_state``) discriminate by
        ``state_kind``; every other operator owns its whole declared table.
        """

        if table not in self.state_tables:
            return False
        if self.state_kind is not None and "state_kind" in columns:
            return row[columns.index("state_kind")] == self.state_kind
        return True

    def validate_state(self, pipeline, state):
        if not isinstance(state, dict) or set(state) != set(self.state_tables):
            raise Invalid("operator state sections mismatch")
        buckets = pipeline["buckets"]
        hash_version = pipeline["hash_version"]
        for table, columns in self.state_tables.items():
            rows = state[table]
            if not isinstance(rows, list):
                raise Invalid("operator state must contain arrays")
            for row in rows:
                if not isinstance(row, list) or len(row) != len(columns):
                    raise Invalid("operator state row layout mismatch")
                if not self.claims_row(table, columns, row):
                    raise Invalid("operator state contains another operator's rows")
                named = dict(zip(columns, row))
                if "bucket" in named:
                    expected = bucket_for(
                        named["group_key"], buckets, hash_version
                    )
                    if named["bucket"] != expected:
                        raise Conflict("operator state key is in the wrong bucket")
                for value in ("document", "transformed"):
                    if named.get(value) is not None:
                        _decode(named[value])

    def snapshot_state(self, connection, pipeline):
        section = {}
        for table, columns in self.state_tables.items():
            values = [pipeline["name"]]
            sql = f'SELECT {",".join(columns)} FROM {table} WHERE pipeline=?'
            if self.state_kind is not None and "state_kind" in columns:
                sql += " AND state_kind=?"
                values.append(self.state_kind)
            sql += " ORDER BY " + ",".join(columns)
            section[table] = [list(row) for row in connection.execute(sql, values)]
        return section

    def restore_state(self, connection, pipeline, state):
        for table, columns in self.state_tables.items():
            names = ["pipeline"] + list(columns)
            placeholders = ",".join("?" for _ in names)
            sql = f'INSERT INTO {table}({",".join(names)}) VALUES ({placeholders})'
            connection.executemany(
                sql, ([pipeline["name"]] + list(row) for row in state[table])
            )


class KeyedOperator(Operator):
    """An operator whose state lives in ``keyed_state`` under its own kind."""

    event_order = 50
    state_tables = {"keyed_state": KEYED_COLUMNS}
    ddl = (
        """
        CREATE TABLE IF NOT EXISTS keyed_state (
         pipeline TEXT NOT NULL REFERENCES pipelines(name),
         bucket INTEGER NOT NULL,
         state_kind TEXT NOT NULL,
         group_key TEXT NOT NULL,
         document TEXT NOT NULL,
         PRIMARY KEY(pipeline,bucket,state_kind,group_key)
        )
        """,
    )

    def __init__(self, state):
        self.state = state
        # ``name`` is a stable identity; ``state_kind`` is the namespace inside
        # keyed_state.  Defaulting the kind to the name keeps the write path and
        # the savepoint ownership filter in agreement.
        if self.state_kind is None:
            self.state_kind = self.name

    def get(self, connection, pipeline, key):
        return self.state.get(connection, pipeline, self.state_kind, key)

    def put(self, connection, pipeline, key, value):
        self.state.put(connection, pipeline, self.state_kind, key, value)

    def delete(self, connection, pipeline, key):
        self.state.delete(connection, pipeline, self.state_kind, key)

    def scan(self, connection, pipeline):
        return self.state.scan(connection, pipeline, self.state_kind)


class WindowOperator(Operator):
    name = "window"
    event_order = 20
    watermark_order = 10
    ddl = (
        """
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
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS window_status (
         pipeline TEXT NOT NULL REFERENCES pipelines(name),
         bucket INTEGER NOT NULL,
         window_start INTEGER NOT NULL,
         window_end INTEGER NOT NULL,
         group_key TEXT NOT NULL,
         status TEXT NOT NULL CHECK(status IN ('open','closed')),
         result_revision INTEGER,
         PRIMARY KEY(pipeline,bucket,window_start,group_key)
        )
        """,
    )
    state_tables = {
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
    }

    def __init__(self, windows):
        self.windows = windows

    def process(self, connection, pipeline, row):
        self.windows.add(connection, pipeline, row)
        return None

    def watermark(self, connection, pipeline):
        return self.windows.close(connection, pipeline)

    def check(self, connection, pipeline, problems):
        info = pipeline
        actual = {}
        for row in connection.execute(
            "SELECT * FROM window_events WHERE pipeline=?", (info["name"],)
        ):
            actual.setdefault(row["event_id"], []).append(row["window_start"])
        for row in connection.execute(
            "SELECT * FROM processed_events WHERE pipeline=? AND disposition='accepted'",
            (info["name"],),
        ):
            transformed = json.loads(row["transformed"])
            expected = sorted(starts_for(transformed["time"], info["plan"]["window"]))
            if sorted(actual.get(row["event_id"], [])) != expected:
                problems.append("window membership mismatch for " + row["event_id"])
        for row in connection.execute(
            "SELECT * FROM window_status WHERE pipeline=?", (info["name"],)
        ):
            if row["status"] == "closed" and (
                info["watermark"] is None or row["window_end"] > info["watermark"]
            ):
                problems.append("window closed ahead of watermark")


class DedupOperator(Operator):
    name = "dedup"
    event_order = 10
    watermark_order = 20
    ddl = (
        """
        CREATE TABLE IF NOT EXISTS seen_keys (
         pipeline TEXT NOT NULL REFERENCES pipelines(name),
         dedup_key TEXT NOT NULL,
         expires_at INTEGER NOT NULL,
         event_id TEXT NOT NULL,
         PRIMARY KEY(pipeline,dedup_key)
        )
        """,
    )
    state_tables = {"seen_keys": ["dedup_key", "expires_at", "event_id"]}

    def applies(self, plan):
        return "dedup" in plan

    def process(self, connection, pipeline, row):
        spec = pipeline["plan"]["dedup"]
        value = field(row, spec["field"])
        if value is None:
            return None
        key = canonical(value)
        old = connection.execute(
            "SELECT * FROM seen_keys WHERE pipeline=? AND dedup_key=?",
            (pipeline["name"], key),
        ).fetchone()
        if old and row["time"] < old["expires_at"]:
            return "duplicate"
        connection.execute(
            "INSERT INTO seen_keys VALUES (?,?,?,?) "
            "ON CONFLICT(pipeline,dedup_key) DO UPDATE SET "
            "expires_at=excluded.expires_at,event_id=excluded.event_id",
            (pipeline["name"], key, row["time"] + spec["ttl_ms"], row["id"]),
        )
        return None

    def watermark(self, connection, pipeline):
        if pipeline["watermark"] is None:
            return None
        connection.execute(
            "DELETE FROM seen_keys WHERE pipeline=? AND expires_at<=?",
            (pipeline["name"], pipeline["watermark"]),
        )
        return None


class LatestOperator(KeyedOperator):
    name = "latest"
    state_kind = "latest"
    event_order = 30

    def process(self, connection, pipeline, row):
        previous = self.get(connection, pipeline, row["key"])
        if previous is None or (row["time"], row["id"]) > (
            previous["time"],
            previous["id"],
        ):
            self.put(connection, pipeline, row["key"], row)
        return None


class SequenceOperator(KeyedOperator):
    name = "sequence"
    state_kind = "sequence"
    event_order = 40
    watermark_order = 30

    def __init__(self, state, results):
        super().__init__(state)
        self.results = results

    def applies(self, plan):
        return "sequence" in plan

    def process(self, connection, pipeline, row):
        spec = pipeline["plan"]["sequence"]
        previous = self.get(connection, pipeline, row["key"])
        if row["kind"] == spec["first_kind"]:
            if previous is None or (row["time"], row["id"]) > (
                previous["time"],
                previous["id"],
            ):
                self.put(connection, pipeline, row["key"], row)
            return None
        if row["kind"] != spec["then_kind"] or previous is None:
            return None
        if not previous["time"] <= row["time"] <= previous["time"] + spec["within_ms"]:
            return None
        key = canonical(["sequence", previous["id"], row["id"]])
        document = {
            "kind": "sequence",
            "key": row["key"],
            "first": previous,
            "then": row,
            "elapsed_ms": row["time"] - previous["time"],
        }
        self.results.publish(connection, pipeline["name"], key, document)
        self.delete(connection, pipeline, row["key"])
        return None

    def watermark(self, connection, pipeline):
        watermark = pipeline["watermark"]
        if watermark is None:
            return None
        spec = pipeline["plan"]["sequence"]
        for row in self.scan(connection, pipeline):
            if row["document"]["time"] + spec["within_ms"] < watermark:
                self.delete(connection, pipeline, row["group_key"])
        return None


class OperatorRegistry:
    """Ordered, validated collection of independently registered operators.

    Registration is the only place that knows concrete operator classes.  The
    engine, watermark scheduler, consistency checks and savepoint layer consume
    the resulting chains and state declarations generically.
    """

    def __init__(self, operators=()):
        self._operators = []
        for operator in operators:
            self.register(operator)

    def register(self, operator):
        if not isinstance(operator, Operator):
            raise Invalid("operator must implement the operator lifecycle")
        identifier(operator.name, "operator name")
        integer(operator.schema_version, "operator state version", 1)
        integer(operator.event_order, "operator event order", 0)
        integer(operator.watermark_order, "operator watermark order", 0)
        if not isinstance(operator.state_tables, dict):
            raise Invalid("operator state tables must be a mapping")
        if operator.state_kind is not None:
            identifier(operator.state_kind, "operator state kind")
        if not isinstance(operator.ddl, (list, tuple)):
            raise Invalid("operator DDL must be a sequence of statements")
        for statement in operator.ddl:
            if not isinstance(statement, str) or not statement.strip():
                raise Invalid("operator DDL statements must be nonempty SQL")
        if operator.plan_key is not None:
            identifier(operator.plan_key, "operator plan key")
            if not callable(operator.plan_validator):
                raise Invalid("operator plan key requires a spec validator")
        for table, columns in operator.state_tables.items():
            _sql_name(table)
            if (
                not isinstance(columns, list)
                or not columns
                or not all(isinstance(column, str) and column for column in columns)
                or len(set(columns)) != len(columns)
            ):
                raise Invalid("operator state table layout is invalid")
            for column in columns:
                _sql_name(column)
            for other in self._operators:
                if table not in other.state_tables:
                    continue
                if other.state_tables[table] != columns:
                    raise Invalid("operators declare incompatible state columns")
                if (
                    "state_kind" not in columns
                    or operator.state_kind is None
                    or other.state_kind is None
                    or operator.state_kind == other.state_kind
                ):
                    raise Invalid(
                        "operators sharing a state table need distinct state kinds"
                    )
        for other in self._operators:
            if other.name == operator.name:
                raise Invalid("duplicate operator name")
        self._operators.append(operator)
        return operator

    def __iter__(self):
        return iter(self._operators)

    def names(self):
        return {operator.name for operator in self._operators}

    def get(self, name):
        for operator in self._operators:
            if operator.name == name:
                return operator
        return None

    def applicable(self, plan):
        return tuple(
            sorted(
                (operator for operator in self._operators if operator.applies(plan)),
                key=lambda operator: operator.name,
            )
        )

    def event_chain(self, plan):
        return tuple(
            sorted(self.applicable(plan), key=lambda op: (op.event_order, op.name))
        )

    def watermark_chain(self, plan):
        return tuple(
            sorted(self.applicable(plan), key=lambda op: (op.watermark_order, op.name))
        )

    def schema(self):
        statements = []
        for operator in self._operators:
            statements.extend(operator.ddl)
        return tuple(statements)

    def plan_extensions(self):
        """Plan keys and validators contributed by registered operators."""

        extensions = {}
        for operator in self._operators:
            if operator.plan_key is None:
                continue
            name = identifier(operator.plan_key, "operator plan key")
            if name in extensions:
                raise Invalid("duplicate operator plan key: " + name)
            if not callable(operator.plan_validator):
                raise Invalid("operator plan key requires a spec validator")
            extensions[name] = operator.plan_validator
        return extensions

    def owned_tables(self, plan):
        tables = {}
        for operator in self.applicable(plan):
            for table, columns in operator.state_tables.items():
                tables[table] = columns
        return tables
