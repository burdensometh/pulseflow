import json

from .values import Conflict, Invalid, canonical, integer


class Results:
    def __init__(self, catalog):
        self.catalog = catalog

    def publish(self, connection, pipeline, key, document, delete=False):
        if not isinstance(key, str) or not key:
            raise Invalid("result key must be nonempty")
        encoded = canonical(document)
        old = connection.execute(
            "SELECT * FROM results WHERE pipeline=? AND result_key=?", (pipeline, key)
        ).fetchone()
        if old and old["document"] == encoded and bool(old["deleted"]) == delete:
            return old["revision"]
        revision = old["revision"] + 1 if old else 1
        connection.execute(
            "INSERT INTO results VALUES (?,?,?,?,?) ON CONFLICT(pipeline,result_key) DO UPDATE SET revision=excluded.revision,document=excluded.document,deleted=excluded.deleted",
            (pipeline, key, revision, encoded, int(delete)),
        )
        connection.execute(
            "INSERT INTO changes(pipeline,result_key,revision,operation,document) VALUES (?,?,?,?,?)",
            (pipeline, key, revision, "delete" if delete else "upsert", encoded),
        )
        return revision

    def rows(self, pipeline, include_deleted=False, connection=None):
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.rows(pipeline, include_deleted, connection)
        self.catalog.pipeline(pipeline, connection)
        sql = "SELECT * FROM results WHERE pipeline=?"
        if not include_deleted:
            sql += " AND deleted=0"
        sql += " ORDER BY result_key"
        return [
            {**dict(row), "document": json.loads(row["document"])}
            for row in connection.execute(sql, (pipeline,))
        ]

    def changes(self, pipeline, after=0, limit=1000, through=None, connection=None):
        integer(after, "change cursor", 0)
        integer(limit, "change limit", 1, 10000)
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.changes(pipeline, after, limit, through, connection)
        self.catalog.pipeline(pipeline, connection)
        sql = "SELECT * FROM changes WHERE pipeline=? AND sequence>?"
        values = [pipeline, after]
        if through is not None:
            integer(through, "change upper bound", after)
            sql += " AND sequence<=?"
            values.append(through)
        sql += " ORDER BY sequence LIMIT ?"
        values.append(limit)
        return [
            {**dict(row), "document": json.loads(row["document"])}
            for row in connection.execute(sql, values)
        ]

    def head(self, pipeline, connection=None):
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.head(pipeline, connection)
        self.catalog.pipeline(pipeline, connection)
        return connection.execute(
            "SELECT coalesce(max(sequence),0) FROM changes WHERE pipeline=?",
            (pipeline,),
        ).fetchone()[0]

    def get(self, pipeline, key):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            row = connection.execute(
                "SELECT * FROM results WHERE pipeline=? AND result_key=?",
                (pipeline, key),
            ).fetchone()
            return (
                {**dict(row), "document": json.loads(row["document"])} if row else None
            )

    def replace_visible(self, pipeline, expected_head, documents):
        if not isinstance(documents, dict):
            raise Invalid("replacement must map result keys to documents")
        with self.catalog.transaction(True) as connection:
            if self.head(pipeline, connection) != expected_head:
                raise Conflict("result head changed before replacement")
            previous = {
                row["result_key"]: row
                for row in self.rows(pipeline, connection=connection)
            }
            for key in sorted(set(previous) - set(documents)):
                self.publish(
                    connection, pipeline, key, previous[key]["document"], delete=True
                )
            for key, value in sorted(documents.items()):
                self.publish(connection, pipeline, key, value)
            self.catalog.event(
                connection,
                pipeline,
                "results_replaced",
                {"previous_head": expected_head, "rows": len(documents)},
            )
        return {"head": self.head(pipeline), "rows": len(documents)}
