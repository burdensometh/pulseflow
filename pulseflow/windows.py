import json

from .state import bucket_for
from .statistics import summarize
from .values import Invalid, canonical


def starts_for(event_time, spec):
    slide, size, offset = spec["slide_ms"], spec["size_ms"], spec["offset_ms"]
    last = ((event_time - offset) // slide) * slide + offset
    count = (size + slide - 1) // slide
    return [
        start
        for start in (last - i * slide for i in range(count))
        if start <= event_time < start + size
    ]


class Windows:
    def __init__(self, catalog, results):
        self.catalog = catalog
        self.results = results

    def add(self, connection, pipeline, row):
        spec = pipeline["plan"]["window"]
        bucket = bucket_for(row["key"], pipeline["buckets"], pipeline["hash_version"])
        for start in starts_for(row["time"], spec):
            old = connection.execute(
                "SELECT status FROM window_status WHERE pipeline=? AND bucket=? AND window_start=? AND group_key=?",
                (pipeline["name"], bucket, start, row["key"]),
            ).fetchone()
            if old and old["status"] != "open":
                raise Invalid("event cannot update a closed window")
            count = connection.execute(
                "SELECT count(*) FROM window_events WHERE pipeline=? AND bucket=? AND window_start=? AND group_key=?",
                (pipeline["name"], bucket, start, row["key"]),
            ).fetchone()[0]
            if count >= pipeline["plan"]["max_window_events"]:
                raise Invalid("window event capacity exceeded")
            connection.execute(
                "INSERT OR IGNORE INTO window_status VALUES (?,?,?,?,?,?,?)",
                (
                    pipeline["name"],
                    bucket,
                    start,
                    start + spec["size_ms"],
                    row["key"],
                    "open",
                    None,
                ),
            )
            connection.execute(
                "INSERT INTO window_events VALUES (?,?,?,?,?,?,?,?)",
                (
                    pipeline["name"],
                    bucket,
                    start,
                    row["key"],
                    row["id"],
                    row["time"],
                    row["value"],
                    canonical(row),
                ),
            )

    def rows_in(self, connection, pipeline, bucket, start, key):
        rows = connection.execute(
            "SELECT document FROM window_events WHERE pipeline=? AND bucket=? AND window_start=? AND group_key=? ORDER BY event_time,event_id",
            (pipeline, bucket, start, key),
        )
        return [json.loads(row[0]) for row in rows]

    def close(self, connection, pipeline):
        if pipeline["watermark"] is None:
            return 0
        rows = connection.execute(
            "SELECT * FROM window_status WHERE pipeline=? AND status='open' AND window_end<=? ORDER BY window_end,bucket,group_key",
            (pipeline["name"], pipeline["watermark"]),
        ).fetchall()
        for state in rows:
            events = self.rows_in(
                connection,
                pipeline["name"],
                state["bucket"],
                state["window_start"],
                state["group_key"],
            )
            document = {
                "kind": "window",
                "key": state["group_key"],
                "start": state["window_start"],
                "end": state["window_end"],
                "metrics": summarize(events, pipeline["plan"]["metrics"]),
            }
            key = canonical(
                [
                    "window",
                    state["group_key"],
                    state["window_start"],
                    state["window_end"],
                ]
            )
            revision = self.results.publish(connection, pipeline["name"], key, document)
            connection.execute(
                "UPDATE window_status SET status='closed',result_revision=? WHERE pipeline=? AND bucket=? AND window_start=? AND group_key=?",
                (
                    revision,
                    pipeline["name"],
                    state["bucket"],
                    state["window_start"],
                    state["group_key"],
                ),
            )
        return len(rows)

    def inspect(self, pipeline, start=None, key=None):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            sql = "SELECT * FROM window_status WHERE pipeline=?"
            values = [pipeline]
            if start is not None:
                sql += " AND window_start=?"
                values.append(start)
            if key is not None:
                sql += " AND group_key=?"
                values.append(key)
            sql += " ORDER BY window_start,group_key"
            return [dict(row) for row in connection.execute(sql, values)]

    def preview(self, pipeline, start, key):
        with self.catalog.transaction() as connection:
            info = self.catalog.pipeline(pipeline, connection)
            bucket = bucket_for(key, info["buckets"], info["hash_version"])
            rows = self.rows_in(connection, pipeline, bucket, start, key)
            return {"rows": rows, "metrics": summarize(rows, info["plan"]["metrics"])}
