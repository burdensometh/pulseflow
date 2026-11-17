import json
from decimal import localcontext

from .state import bucket_for
from .statistics import RunningTotals, rank_for, render_metrics, summarize
from .values import Invalid, canonical, decimal_text


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
    """Window assignment and exact closing aggregation.

    Every accepted event is persisted in the catalog, so window state already
    lives on disk. ``max_window_events`` is the in-memory record budget, not a
    capacity limit: closing streams a window back in chunks of at most that
    many rows and pushes exact distinct/quantile work into disk-backed SQL, so
    windows larger than the budget spill to disk instead of being rejected.
    """

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

    def value_chunks(self, connection, pipeline, bucket, start, key, budget):
        """Yield one window's stored values in chunks of at most `budget` rows.

        Rows stream in the same order rows_in used, so the exact running sum
        matches a full in-memory computation bit for bit, while at most
        `budget` rows are held in a Python collection at any time.
        """
        cursor = connection.execute(
            "SELECT value FROM window_events WHERE pipeline=? AND bucket=? AND window_start=? AND group_key=? ORDER BY event_time,event_id",
            (pipeline, bucket, start, key),
        )
        while True:
            chunk = cursor.fetchmany(budget)
            if not chunk:
                return
            yield chunk

    def metrics_for(self, connection, pipeline, bucket, start, key):
        """Exact window metrics reading at most max_window_events rows at once."""
        plan = pipeline["plan"]
        metrics = plan["metrics"]
        totals = RunningTotals()
        with localcontext() as context:
            context.prec = 76
            for chunk in self.value_chunks(
                connection,
                pipeline["name"],
                bucket,
                start,
                key,
                plan["max_window_events"],
            ):
                for row in chunk:
                    totals.add(row[0])
        distinct = None
        if "distinct" in metrics:
            distinct = connection.execute(
                "SELECT COUNT(DISTINCT value) FROM window_events "
                "WHERE pipeline=? AND bucket=? AND window_start=? AND group_key=? AND value IS NOT NULL",
                (pipeline["name"], bucket, start, key),
            ).fetchone()[0]

        def percentile_at(numerator):
            if totals.count_value == 0:
                return None
            row = connection.execute(
                "SELECT value FROM window_events "
                "WHERE pipeline=? AND bucket=? AND window_start=? AND group_key=? AND value IS NOT NULL "
                "ORDER BY value COLLATE decimal_exact LIMIT 1 OFFSET ?",
                (
                    pipeline["name"],
                    bucket,
                    start,
                    key,
                    rank_for(totals.count_value, numerator) - 1,
                ),
            ).fetchone()
            return decimal_text(row[0])

        return render_metrics(
            metrics,
            totals.count,
            totals.count_value,
            totals.total,
            totals.minimum,
            totals.maximum,
            distinct,
            percentile_at,
        )

    def close(self, connection, pipeline):
        if pipeline["watermark"] is None:
            return 0
        rows = connection.execute(
            "SELECT * FROM window_status WHERE pipeline=? AND status='open' AND window_end<=? ORDER BY window_end,bucket,group_key",
            (pipeline["name"], pipeline["watermark"]),
        ).fetchall()
        for state in rows:
            document = {
                "kind": "window",
                "key": state["group_key"],
                "start": state["window_start"],
                "end": state["window_end"],
                "metrics": self.metrics_for(
                    connection,
                    pipeline,
                    state["bucket"],
                    state["window_start"],
                    state["group_key"],
                ),
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
