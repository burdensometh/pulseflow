from pathlib import Path
import json
import sqlite3

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


DDL = """
CREATE TABLE IF NOT EXISTS external_sources (
 name TEXT PRIMARY KEY,
 channel TEXT NOT NULL,
 partition_id INTEGER NOT NULL,
 definition TEXT NOT NULL,
 definition_hash TEXT NOT NULL,
 source_cursor INTEGER NOT NULL,
 prefix_sha256 TEXT NOT NULL,
 imported_rows INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS external_poll_runs (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 source TEXT NOT NULL,
 from_cursor INTEGER NOT NULL,
 to_cursor INTEGER NOT NULL,
 rows_imported INTEGER NOT NULL,
 captured_high INTEGER NOT NULL,
 created_at TEXT NOT NULL
);
"""


def column(value):
    identifier(value, "SQL column")
    if "." in value or "-" in value:
        raise Invalid("SQL column must be a simple identifier")
    return '"' + value + '"'


def definition(value):
    required = {"table", "sequence", "key", "time", "value"}
    optional = {"attrs", "kind"}
    if (
        not isinstance(value, dict)
        or required - set(value)
        or set(value) - required - optional
    ):
        raise Invalid("SQL source definition is incomplete")
    for key in required | ({"kind"} if "kind" in value else set()):
        column(value[key])
    attrs = value.get("attrs", {})
    if not isinstance(attrs, dict) or not all(isinstance(key, str) for key in attrs):
        raise Invalid("SQL attrs must map names to columns")
    for name in attrs.values():
        column(name)
    return {**value, "attrs": attrs}


class SQLiteIngress:
    def __init__(self, catalog, journal):
        self.catalog = catalog
        self.journal = journal
        connection = catalog.connect()
        try:
            connection.executescript(DDL)
        finally:
            connection.close()

    def register(self, name, channel, partition, spec):
        name = identifier(name)
        spec = definition(spec)
        digest = fingerprint(spec)
        with self.catalog.transaction(True) as connection:
            info = self.catalog.channel(channel, connection)
            integer(partition, "source partition", 0, info["partitions"] - 1)
            old = connection.execute(
                "SELECT * FROM external_sources WHERE name=?", (name,)
            ).fetchone()
            if old:
                if (old["channel"], old["partition_id"], old["definition_hash"]) != (
                    channel,
                    partition,
                    digest,
                ):
                    raise Conflict(
                        "SQL source identity already has a different binding"
                    )
            else:
                connection.execute(
                    "INSERT INTO external_sources VALUES (?,?,?,?,?,?,?,0)",
                    (
                        name,
                        channel,
                        partition,
                        canonical(spec),
                        digest,
                        -1,
                        fingerprint([]),
                    ),
                )
        return self.status(name)

    def status(self, name, connection=None):
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.status(name, connection)
        row = connection.execute(
            "SELECT * FROM external_sources WHERE name=?", (name,)
        ).fetchone()
        if row is None:
            raise Missing("SQL source is not registered")
        return {**dict(row), "definition": json.loads(row["definition"])}

    def rows(self, source, spec, after, through, limit=None):
        fields = list(
            dict.fromkeys(
                [spec[key] for key in ["sequence", "key", "time", "value"]]
                + ([spec["kind"]] if "kind" in spec else [])
                + list(spec["attrs"].values())
            )
        )
        sql = (
            "SELECT "
            + ",".join(column(name) for name in fields)
            + " FROM "
            + column(spec["table"])
            + " WHERE "
            + column(spec["sequence"])
            + ">?"
            + " AND "
            + column(spec["sequence"])
            + "<=?"
            + " ORDER BY "
            + column(spec["sequence"])
        )
        values = [after, through]
        if limit is not None:
            sql += " LIMIT ?"
            values.append(limit)
        return [dict(row) for row in source.execute(sql, values)]

    def normalize(self, name, spec, rows):
        records = []
        previous = -1
        for row in rows:
            sequence = integer(row[spec["sequence"]], "source sequence", 0)
            if sequence <= previous:
                raise Conflict("SQL source sequence is not unique and increasing")
            previous = sequence
            event = {
                "id": name + ":" + str(sequence),
                "key": row[spec["key"]],
                "time": row[spec["time"]],
                "value": row[spec["value"]],
                "kind": row[spec["kind"]] if "kind" in spec else "reading",
                "attrs": {
                    key: row[value] for key, value in spec.get("attrs", {}).items()
                },
            }
            records.append(event)
        return records

    def poll(self, name, database, limit=1000):
        integer(limit, "SQL polling limit", 1, 10000)
        path = Path(database).resolve()
        if not path.is_file() or path == self.catalog.path:
            raise Invalid("SQL source must be a separate existing SQLite file")
        source = sqlite3.connect(
            path.as_uri() + "?mode=ro", uri=True, isolation_level=None
        )
        source.row_factory = sqlite3.Row
        try:
            source.execute("BEGIN")
            with self.catalog.transaction(True) as connection:
                state = self.status(name, connection)
                spec = state["definition"]
                high = source.execute(
                    "SELECT max("
                    + column(spec["sequence"])
                    + ") FROM "
                    + column(spec["table"])
                ).fetchone()[0]
                high = -1 if high is None else integer(high, "source high sequence", 0)
                if high < state["source_cursor"]:
                    raise Conflict(
                        "SQL source was truncated behind its committed cursor"
                    )
                prefix = self.rows(source, spec, -1, state["source_cursor"])
                self.normalize(name, spec, prefix)
                if fingerprint(prefix) != state["prefix_sha256"]:
                    raise Conflict("already imported SQL source prefix was modified")
                rows = self.rows(source, spec, state["source_cursor"], high, limit)
                events = self.normalize(name, spec, rows)
                offset = connection.execute(
                    "SELECT next_offset FROM source_positions WHERE channel=? AND partition_id=?",
                    (state["channel"], state["partition_id"]),
                ).fetchone()[0]
                records = [
                    {
                        "partition": state["partition_id"],
                        "offset": offset + i,
                        "event": value,
                    }
                    for i, value in enumerate(events)
                ]
                cursor = rows[-1][spec["sequence"]] if rows else state["source_cursor"]
                batch = (
                    "sql."
                    + fingerprint([name, state["source_cursor"], cursor, records])[:64]
                )
                receipt = self.journal.append_in(
                    connection, state["channel"], batch, records
                )
                connection.execute(
                    "UPDATE external_sources SET source_cursor=?,prefix_sha256=?,"
                    "imported_rows=imported_rows+? WHERE name=?",
                    (cursor, fingerprint(prefix + rows), len(rows), name),
                )
                connection.execute(
                    "INSERT INTO external_poll_runs(source,from_cursor,to_cursor,rows_imported,captured_high,created_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (name, state["source_cursor"], cursor, len(rows), high, utc_now()),
                )
                return {
                    "source": name,
                    "rows": len(rows),
                    "cursor": cursor,
                    "captured_high": high,
                    "ingress": receipt,
                }
        finally:
            source.close()

    def runs(self, name):
        with self.catalog.transaction() as connection:
            self.status(name, connection)
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM external_poll_runs WHERE source=? ORDER BY sequence",
                    (name,),
                )
            ]
