import json

from .state import bucket_for
from .statistics import summarize
from .values import Invalid, canonical


class Sessions:
    def __init__(self, catalog, results):
        self.catalog = catalog
        self.results = results

    def add(self, connection, pipeline, row):
        gap = pipeline["plan"]["window"]["gap_ms"]
        bucket = bucket_for(row["key"], pipeline["buckets"], pipeline["hash_version"])
        start, end = row["time"], row["time"] + gap
        blocked = connection.execute(
            "SELECT 1 FROM session_status WHERE pipeline=? AND bucket=? AND group_key=?"
            " AND status!='open' AND session_start<? AND session_end>? LIMIT 1",
            (pipeline["name"], bucket, row["key"], end, start),
        ).fetchone()
        if blocked:
            raise Invalid("event cannot update a closed session")
        merges = connection.execute(
            "SELECT * FROM session_status WHERE pipeline=? AND bucket=? AND group_key=?"
            " AND status='open' AND session_start<? AND session_end>? ORDER BY session_start",
            (pipeline["name"], bucket, row["key"], end, start),
        ).fetchall()
        if merges:
            start = min(start, merges[0]["session_start"])
            end = max([end] + [state["session_end"] for state in merges])
            count = connection.execute(
                "SELECT count(*) FROM session_events WHERE pipeline=? AND bucket=?"
                " AND group_key=? AND session_start IN (%s)"
                % ",".join("?" for _ in merges),
                (
                    [pipeline["name"], bucket, row["key"]]
                    + [state["session_start"] for state in merges]
                ),
            ).fetchone()[0]
        else:
            count = 0
        if count >= pipeline["plan"]["max_window_events"]:
            raise Invalid("session event capacity exceeded")
        for state in merges:
            connection.execute(
                "UPDATE session_events SET session_start=? WHERE pipeline=? AND bucket=?"
                " AND group_key=? AND session_start=?",
                (start, pipeline["name"], bucket, row["key"], state["session_start"]),
            )
            connection.execute(
                "DELETE FROM session_status WHERE pipeline=? AND bucket=? AND group_key=?"
                " AND session_start=?",
                (pipeline["name"], bucket, row["key"], state["session_start"]),
            )
        connection.execute(
            "INSERT INTO session_status VALUES (?,?,?,?,?,?,?)",
            (pipeline["name"], bucket, start, end, row["key"], "open", None),
        )
        connection.execute(
            "INSERT INTO session_events VALUES (?,?,?,?,?,?,?,?)",
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
            "SELECT document FROM session_events WHERE pipeline=? AND bucket=? AND session_start=? AND group_key=? ORDER BY event_time,event_id",
            (pipeline, bucket, start, key),
        )
        return [json.loads(row[0]) for row in rows]

    def close(self, connection, pipeline):
        if pipeline["watermark"] is None:
            return 0
        rows = connection.execute(
            "SELECT * FROM session_status WHERE pipeline=? AND status='open' AND session_end<=? ORDER BY session_end,bucket,group_key",
            (pipeline["name"], pipeline["watermark"]),
        ).fetchall()
        for state in rows:
            events = self.rows_in(
                connection,
                pipeline["name"],
                state["bucket"],
                state["session_start"],
                state["group_key"],
            )
            document = {
                "kind": "session",
                "key": state["group_key"],
                "start": state["session_start"],
                "end": state["session_end"],
                "metrics": summarize(events, pipeline["plan"]["metrics"]),
            }
            key = canonical(
                [
                    "session",
                    state["group_key"],
                    state["session_start"],
                    state["session_end"],
                ]
            )
            revision = self.results.publish(connection, pipeline["name"], key, document)
            connection.execute(
                "UPDATE session_status SET status='closed',result_revision=? WHERE pipeline=? AND bucket=? AND session_start=? AND group_key=?",
                (
                    revision,
                    pipeline["name"],
                    state["bucket"],
                    state["session_start"],
                    state["group_key"],
                ),
            )
        return len(rows)

    def inspect(self, pipeline, start=None, key=None):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            sql = "SELECT * FROM session_status WHERE pipeline=?"
            values = [pipeline]
            if start is not None:
                sql += " AND session_start=?"
                values.append(start)
            if key is not None:
                sql += " AND group_key=?"
                values.append(key)
            sql += " ORDER BY session_start,group_key"
            return [dict(row) for row in connection.execute(sql, values)]

    def preview(self, pipeline, start, key):
        with self.catalog.transaction() as connection:
            info = self.catalog.pipeline(pipeline, connection)
            bucket = bucket_for(key, info["buckets"], info["hash_version"])
            rows = self.rows_in(connection, pipeline, bucket, start, key)
            return {"rows": rows, "metrics": summarize(rows, info["plan"]["metrics"])}
