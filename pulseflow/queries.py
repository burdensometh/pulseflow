from collections import Counter
import json

from .statistics import distribution, summarize, topk
from .values import Missing, canonical, fingerprint


class Queries:
    def __init__(self, app):
        self.app = app

    def event(self, pipeline, event_id):
        with self.app.catalog.transaction() as connection:
            info = self.app.catalog.pipeline(pipeline, connection)
            source = connection.execute(
                "SELECT * FROM journal WHERE channel=? AND event_id=?",
                (info["channel"], event_id),
            ).fetchone()
            if source is None:
                raise Missing("source event does not exist")
            processed = connection.execute(
                "SELECT * FROM processed_events WHERE pipeline=? AND event_id=?",
                (pipeline, event_id),
            ).fetchone()
            memberships = [
                dict(row)
                for row in connection.execute(
                    "SELECT bucket,window_start,group_key FROM window_events "
                    "WHERE pipeline=? AND event_id=? ORDER BY window_start",
                    (pipeline, event_id),
                )
            ]
            late = connection.execute(
                "SELECT watermark FROM late_events WHERE pipeline=? AND event_id=?",
                (pipeline, event_id),
            ).fetchone()
            return {
                "source": {**dict(source), "document": json.loads(source["document"])},
                "processed": dict(processed) if processed else None,
                "windows": memberships,
                "rejection_watermark": late[0] if late else None,
            }

    def accepted(self, pipeline, key=None):
        with self.app.catalog.transaction() as connection:
            self.app.catalog.pipeline(pipeline, connection)
            rows = connection.execute(
                "SELECT transformed FROM processed_events "
                "WHERE pipeline=? AND disposition='accepted' ORDER BY source_sequence",
                (pipeline,),
            )
            values = [json.loads(row[0]) for row in rows]
            return (
                values if key is None else [row for row in values if row["key"] == key]
            )

    def statistics(self, pipeline, key=None):
        rows = self.accepted(pipeline, key)
        return {
            "metrics": summarize(
                rows,
                [
                    "count",
                    "count_value",
                    "sum",
                    "mean",
                    "min",
                    "max",
                    "p50",
                    "p95",
                    "distinct",
                ],
            ),
            "kinds": distribution(rows, "kind"),
            "keys": distribution(rows, "key"),
        }

    def top(self, pipeline, k=10):
        return topk(self.accepted(pipeline), k)

    def series(self, pipeline, key, start, end, step_ms, mode="step", max_gap=None):
        from .series import integral, resample

        rows = self.accepted(pipeline, key)
        return {
            "grid": resample(rows, start, end, step_ms, mode, max_gap),
            "integral": integral(rows, start, end, mode, max_gap),
        }

    def window_lineage(self, pipeline, start, key):
        preview = self.app.windows.preview(pipeline, start, key)
        return {
            "window_start": start,
            "key": key,
            "events": [self.event(pipeline, row["id"]) for row in preview["rows"]],
            "metrics": preview["metrics"],
        }

    def dimension_impact(self, pipeline, dimension, key, valid_from, valid_to=None):
        with self.app.catalog.transaction() as connection:
            info = self.app.catalog.pipeline(pipeline, connection)
            if info["plan"].get("dimension", {}).get("name") != dimension:
                return {"events": [], "windows": []}
            sql = (
                "SELECT p.event_id,j.event_time,p.dimension_version "
                "FROM processed_events p JOIN journal j "
                "ON p.source_sequence=j.sequence "
                "WHERE p.pipeline=? AND j.event_key=? AND j.event_time>=?"
            )
            values = [pipeline, key, valid_from]
            if valid_to is not None:
                sql += " AND j.event_time<?"
                values.append(valid_to)
            sql += " ORDER BY j.event_time,p.event_id"
            events = [dict(row) for row in connection.execute(sql, values)]
            ids = {row["event_id"] for row in events}
            windows = set()
            for row in connection.execute(
                "SELECT window_start,group_key,event_id FROM window_events WHERE pipeline=?",
                (pipeline,),
            ):
                if row["event_id"] in ids:
                    windows.add((row["window_start"], row["group_key"]))
            return {"events": events, "windows": [list(row) for row in sorted(windows)]}

    def lag(self, pipeline):
        with self.app.catalog.transaction() as connection:
            info = self.app.catalog.pipeline(pipeline, connection)
            rows = connection.execute(
                "SELECT p.partition_id,p.next_offset AS processed,s.next_offset AS ingested,"
                "p.max_time,p.declared_time FROM progress p "
                "JOIN source_positions s ON p.partition_id=s.partition_id "
                "WHERE p.pipeline=? AND s.channel=? ORDER BY p.partition_id",
                (pipeline, info["channel"]),
            )
            activity = {
                row["partition_id"]: row
                for row in self.app.watermarks.activity(connection, info)[
                    "partitions"
                ]
            }
            return [
                {
                    **dict(row),
                    "pending": row["ingested"] - row["processed"],
                    "idle": activity.get(row["partition_id"], {}).get("idle", False),
                }
                for row in rows
            ]

    def compare_consumer(self, pipeline, consumer, subscription="default"):
        expected = Counter(
            canonical(row["document"]) for row in self.app.results.rows(pipeline)
        )
        actual = Counter(
            canonical(row["document"]) for row in consumer.rows(subscription)
        )
        return {
            "equal": expected == actual,
            "missing": list((expected - actual).elements()),
            "extra": list((actual - expected).elements()),
            "expected_sha256": fingerprint(sorted(expected.items())),
            "actual_sha256": fingerprint(sorted(actual.items())),
        }
