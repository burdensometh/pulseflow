from decimal import Decimal, localcontext
import json

from .values import (
    Conflict,
    Invalid,
    canonical,
    decimal,
    decimal_text,
    fingerprint,
    identifier,
    utc_now,
)


DDL = """
CREATE TABLE IF NOT EXISTS rollup_jobs (
 name TEXT PRIMARY KEY,
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 definition TEXT NOT NULL,
 definition_hash TEXT NOT NULL,
 cursor INTEGER NOT NULL,
 generation INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS rollup_inputs (
 job TEXT NOT NULL REFERENCES rollup_jobs(name),
 result_key TEXT NOT NULL,
 revision INTEGER NOT NULL,
 document TEXT NOT NULL,
 PRIMARY KEY(job,result_key)
);
CREATE TABLE IF NOT EXISTS rollup_rows (
 job TEXT NOT NULL REFERENCES rollup_jobs(name),
 group_key TEXT NOT NULL,
 document TEXT NOT NULL,
 PRIMARY KEY(job,group_key)
);
CREATE TABLE IF NOT EXISTS rollup_runs (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 job TEXT NOT NULL,
 from_cursor INTEGER NOT NULL,
 to_cursor INTEGER NOT NULL,
 changed_inputs INTEGER NOT NULL,
 output_rows INTEGER NOT NULL,
 created_at TEXT NOT NULL
);
"""


def rollup_definition(value):
    if not isinstance(value, dict) or set(value) != {"groups", "metrics"}:
        raise Invalid("rollup requires key-to-group mapping and metrics")
    if not isinstance(value["groups"], dict) or not all(
        isinstance(key, str) and isinstance(group, str)
        for key, group in value["groups"].items()
    ):
        raise Invalid("rollup groups must map strings to strings")
    metrics = value["metrics"]
    if not isinstance(metrics, dict) or not metrics:
        raise Invalid("rollup metrics must be a nonempty object")
    for name, spec in metrics.items():
        identifier(name)
        if not isinstance(spec, dict) or set(spec) != {"op", "metric"}:
            raise Invalid("rollup metric requires op and metric")
        if spec["op"] not in {"sum", "min", "max", "count"}:
            raise Invalid("unsupported rollup metric operation")
        if not isinstance(spec["metric"], str):
            raise Invalid("rollup source metric must be a name")
    return value


class Rollups:
    def __init__(self, catalog, results):
        self.catalog = catalog
        self.results = results
        with catalog.connect() as connection:
            connection.executescript(DDL)

    def register(self, name, pipeline, definition):
        name = identifier(name)
        definition = rollup_definition(definition)
        checksum = fingerprint(definition)
        with self.catalog.transaction(True) as connection:
            self.catalog.pipeline(pipeline, connection)
            old = connection.execute(
                "SELECT * FROM rollup_jobs WHERE name=?", (name,)
            ).fetchone()
            if old:
                if (old["pipeline"], old["definition_hash"]) != (pipeline, checksum):
                    raise Conflict("rollup identity is immutable")
            else:
                connection.execute(
                    "INSERT INTO rollup_jobs VALUES (?,?,?,?,0,0)",
                    (name, pipeline, canonical(definition), checksum),
                )
        return self.status(name)

    def status(self, name, connection=None):
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.status(name, connection)
        row = connection.execute(
            "SELECT * FROM rollup_jobs WHERE name=?", (name,)
        ).fetchone()
        if row is None:
            from .values import Missing

            raise Missing("rollup does not exist")
        return {**dict(row), "definition": json.loads(row["definition"])}

    def aggregate(self, rows, definition):
        groups = {}
        for source in rows:
            if source.get("kind") != "window":
                continue
            key = source["key"]
            group = definition["groups"].get(key, key)
            identity = canonical([group, source["start"], source["end"]])
            groups.setdefault(identity, []).append(source)
        output = []
        for identity, members in sorted(groups.items()):
            group, start, end = json.loads(identity)
            metrics = {}
            with localcontext() as context:
                context.prec = 76
                for name, spec in definition["metrics"].items():
                    values = [row["metrics"].get(spec["metric"]) for row in members]
                    values = [decimal(value) for value in values if value is not None]
                    if spec["op"] == "count":
                        value = len(values)
                    elif not values:
                        value = None
                    elif spec["op"] == "sum":
                        value = decimal_text(sum(values, Decimal(0)))
                    elif spec["op"] == "min":
                        value = decimal_text(min(values))
                    else:
                        value = decimal_text(max(values))
                    metrics[name] = value
            output.append(
                {
                    "group_key": identity,
                    "document": {
                        "key": group,
                        "start": start,
                        "end": end,
                        "metrics": metrics,
                    },
                }
            )
        return output

    def refresh(self, name, limit=1000):
        with self.catalog.transaction(True) as connection:
            job = self.status(name, connection)
            head = self.results.head(job["pipeline"], connection)
            if head < job["cursor"]:
                raise Conflict("rollup source changelog moved backwards")
            changes = self.results.changes(
                job["pipeline"], job["cursor"], limit, head, connection
            )
            for change in changes:
                if change["operation"] == "delete":
                    connection.execute(
                        "DELETE FROM rollup_inputs WHERE job=? AND result_key=?",
                        (name, change["result_key"]),
                    )
                else:
                    connection.execute(
                        "INSERT INTO rollup_inputs VALUES (?,?,?,?) "
                        "ON CONFLICT(job,result_key) DO UPDATE SET "
                        "revision=excluded.revision,document=excluded.document",
                        (
                            name,
                            change["result_key"],
                            change["revision"],
                            canonical(change["document"]),
                        ),
                    )
            inputs = [
                json.loads(row[0])
                for row in connection.execute(
                    "SELECT document FROM rollup_inputs WHERE job=? ORDER BY result_key",
                    (name,),
                )
            ]
            rows = self.aggregate(inputs, job["definition"])
            connection.execute("DELETE FROM rollup_rows WHERE job=?", (name,))
            connection.executemany(
                "INSERT INTO rollup_rows VALUES (?,?,?)",
                ((name, row["group_key"], canonical(row["document"])) for row in rows),
            )
            cursor = changes[-1]["sequence"] if changes else job["cursor"]
            connection.execute(
                "UPDATE rollup_jobs SET cursor=?,generation=generation+1 WHERE name=?",
                (cursor, name),
            )
            connection.execute(
                "INSERT INTO rollup_runs(job,from_cursor,to_cursor,changed_inputs,output_rows,created_at) "
                "VALUES (?,?,?,?,?,?)",
                (name, job["cursor"], cursor, len(changes), len(rows), utc_now()),
            )
        return {
            "job": name,
            "changed": len(changes),
            "cursor": cursor,
            "head": head,
            "rows": len(rows),
        }

    def catch_up(self, name, limit=1000):
        changed = 0
        while True:
            result = self.refresh(name, limit)
            changed += result["changed"]
            if result["changed"] < limit:
                return {**result, "changed": changed}

    def rows(self, name):
        with self.catalog.transaction() as connection:
            self.status(name, connection)
            return [
                json.loads(row[0])
                for row in connection.execute(
                    "SELECT document FROM rollup_rows WHERE job=? ORDER BY group_key",
                    (name,),
                )
            ]

    def runs(self, name):
        with self.catalog.transaction() as connection:
            self.status(name, connection)
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM rollup_runs WHERE job=? ORDER BY sequence", (name,)
                )
            ]
