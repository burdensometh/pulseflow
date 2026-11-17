from contextlib import contextmanager
from pathlib import Path
import json
import sqlite3

from .values import Conflict, canonical, integer


DDL = """
CREATE TABLE IF NOT EXISTS subscriptions (
 name TEXT PRIMARY KEY,
 origin TEXT NOT NULL,
 pipeline TEXT NOT NULL,
 plan_hash TEXT NOT NULL,
 cursor INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS materialized (
 subscription TEXT NOT NULL,
 result_key TEXT NOT NULL,
 revision INTEGER NOT NULL,
 document TEXT NOT NULL,
 deleted INTEGER NOT NULL,
 PRIMARY KEY(subscription,result_key)
);
CREATE TABLE IF NOT EXISTS applied_changes (
 subscription TEXT NOT NULL,
 source_sequence INTEGER NOT NULL,
 fingerprint TEXT NOT NULL,
 PRIMARY KEY(subscription,source_sequence)
);
"""


class Consumer:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as connection:
            connection.executescript(DDL)

    @contextmanager
    def transaction(self):
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=20)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def bind(self, connection, name, source, pipeline):
        identity = source.catalog.identity()
        info = source.catalog.pipeline(pipeline)
        old = connection.execute(
            "SELECT * FROM subscriptions WHERE name=?", (name,)
        ).fetchone()
        if old:
            if (old["origin"], old["pipeline"], old["plan_hash"]) != (
                identity,
                pipeline,
                info["plan_hash"],
            ):
                raise Conflict("subscription belongs to a different source")
            return dict(old)
        connection.execute(
            "INSERT INTO subscriptions VALUES (?,?,?,?,0)",
            (name, identity, pipeline, info["plan_hash"]),
        )
        return {
            "name": name,
            "origin": identity,
            "pipeline": pipeline,
            "plan_hash": info["plan_hash"],
            "cursor": 0,
        }

    def apply(self, source, pipeline, name="default", limit=1000):
        integer(limit, "consumer page size", 1, 10000)
        from .values import fingerprint

        with self.transaction() as destination:
            subscription = self.bind(destination, name, source, pipeline)
            with source.catalog.transaction() as connection:
                head = source.results.head(pipeline, connection)
                if subscription["cursor"] > head:
                    raise Conflict("source changelog moved behind consumer cursor")
                changes = source.results.changes(
                    pipeline, subscription["cursor"], limit, head, connection
                )
            for change in changes:
                old = destination.execute(
                    "SELECT * FROM materialized WHERE subscription=? AND result_key=?",
                    (name, change["result_key"]),
                ).fetchone()
                if old and change["revision"] <= old["revision"]:
                    raise Conflict("source returned a nonincreasing result revision")
                destination.execute(
                    "INSERT INTO materialized VALUES (?,?,?,?,?) ON CONFLICT(subscription,result_key) DO UPDATE SET revision=excluded.revision,document=excluded.document,deleted=excluded.deleted",
                    (
                        name,
                        change["result_key"],
                        change["revision"],
                        canonical(change["document"]),
                        int(change["operation"] == "delete"),
                    ),
                )
                destination.execute(
                    "INSERT INTO applied_changes VALUES (?,?,?)",
                    (name, change["sequence"], fingerprint(change)),
                )
            cursor = changes[-1]["sequence"] if changes else subscription["cursor"]
            destination.execute(
                "UPDATE subscriptions SET cursor=? WHERE name=?", (cursor, name)
            )
        return {
            "subscription": name,
            "applied": len(changes),
            "cursor": cursor,
            "head": head,
        }

    def catch_up(self, source, pipeline, name="default", limit=1000):
        total = 0
        while True:
            result = self.apply(source, pipeline, name, limit)
            total += result["applied"]
            if result["applied"] < limit:
                return {**result, "applied": total}

    def bootstrap(self, source, pipeline, name="default"):
        with self.transaction() as destination:
            old = self.bind(destination, name, source, pipeline)
            with source.catalog.transaction() as connection:
                head = source.results.head(pipeline, connection)
                rows = source.results.rows(
                    pipeline, include_deleted=True, connection=connection
                )
            if old["cursor"] > head:
                raise Conflict("cannot bootstrap from older source state")
            destination.execute(
                "DELETE FROM materialized WHERE subscription=?", (name,)
            )
            destination.execute(
                "DELETE FROM applied_changes WHERE subscription=?", (name,)
            )
            destination.executemany(
                "INSERT INTO materialized VALUES (?,?,?,?,?)",
                (
                    (
                        name,
                        row["result_key"],
                        row["revision"],
                        canonical(row["document"]),
                        row["deleted"],
                    )
                    for row in rows
                ),
            )
            destination.execute(
                "UPDATE subscriptions SET cursor=? WHERE name=?", (head, name)
            )
        return {"subscription": name, "cursor": head, "rows": len(rows)}

    def rows(self, name="default", include_deleted=False):
        with self.transaction() as connection:
            sql = "SELECT * FROM materialized WHERE subscription=?"
            if not include_deleted:
                sql += " AND deleted=0"
            sql += " ORDER BY result_key"
            return [
                {**dict(row), "document": json.loads(row["document"])}
                for row in connection.execute(sql, (name,))
            ]

    def status(self, name="default"):
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM subscriptions WHERE name=?", (name,)
            ).fetchone()
            return dict(row) if row else None
