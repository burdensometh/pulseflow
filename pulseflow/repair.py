import json

from .state import bucket_for
from .statistics import summarize
from .values import Invalid, canonical
from .windows import starts_for


CHANGE_KEYS = (
    "events_added",
    "events_removed",
    "windows_opened",
    "windows_removed",
    "results_updated",
    "results_retracted",
)


class Repair:
    """Incrementally correct window membership for running pipelines.

    The repair never touches the source journal, source positions or the
    processing cursor.  It reconciles the window state of already accepted
    events against the current (corrected) membership formula and publishes
    every changed result through the normal versioned change stream, so
    downstream consumers converge on the correct windows by catching up.
    Re-running the repair is a no-op because it only applies the difference
    between the stored state and the state implied by the current formula.
    """

    def __init__(self, app):
        self.app = app

    def reconcile_pipeline(self, connection, name):
        info = self.app.catalog.pipeline(name, connection)
        plan = info["plan"]
        spec = plan["window"]
        maximum = plan["max_window_events"]

        accepted = connection.execute(
            "SELECT event_id,transformed FROM processed_events "
            "WHERE pipeline=? AND disposition='accepted' ORDER BY source_sequence",
            (name,),
        ).fetchall()

        # Expected membership: (bucket, window_start, group_key) -> event rows.
        expected = {}
        for row in accepted:
            event = json.loads(row["transformed"])
            bucket = bucket_for(event["key"], info["buckets"], info["hash_version"])
            for start in starts_for(event["time"], spec):
                expected.setdefault((bucket, start, event["key"]), []).append(event)

        actual_rows = connection.execute(
            "SELECT bucket,window_start,group_key,event_id FROM window_events "
            "WHERE pipeline=?",
            (name,),
        ).fetchall()
        actual = {}
        for row in actual_rows:
            actual.setdefault(
                (row["bucket"], row["window_start"], row["group_key"]), set()
            ).add(row["event_id"])

        report = {key: 0 for key in CHANGE_KEYS}
        report["pipeline"] = name

        # 1. Reconcile window_events against the expected membership.  Every
        #    stored window identity is visited so memberships in windows that
        #    must disappear entirely are removed as well.
        for identity in sorted(set(actual) | set(expected)):
            bucket, start, key = identity
            events = expected.get(identity, [])
            if len(events) > maximum:
                raise Invalid("window event capacity exceeded during repair")
            have = actual.get(identity, set())
            want = {event["id"] for event in events}
            for event_id in have - want:
                connection.execute(
                    "DELETE FROM window_events WHERE pipeline=? AND bucket=? "
                    "AND window_start=? AND group_key=? AND event_id=?",
                    (name, bucket, start, key, event_id),
                )
                report["events_removed"] += 1
            for event in events:
                if event["id"] in have:
                    continue
                connection.execute(
                    "INSERT INTO window_events VALUES (?,?,?,?,?,?,?,?)",
                    (
                        name,
                        bucket,
                        start,
                        key,
                        event["id"],
                        event["time"],
                        event["value"],
                        canonical(event),
                    ),
                )
                report["events_added"] += 1

        # 2. Reconcile window_status, creating or removing window rows.
        status_rows = {
            (r["bucket"], r["window_start"], r["group_key"]): r
            for r in connection.execute(
                "SELECT * FROM window_status WHERE pipeline=?", (name,)
            )
        }
        for identity in expected:
            if identity in status_rows:
                continue
            bucket, start, key = identity
            end = start + spec["size_ms"]
            closed = info["watermark"] is not None and end <= info["watermark"]
            connection.execute(
                "INSERT INTO window_status VALUES (?,?,?,?,?,?,?)",
                (name, bucket, start, end, key, "closed" if closed else "open", None),
            )
            report["windows_opened"] += 1
        for identity, row in list(status_rows.items()):
            if identity in expected:
                continue
            bucket, start, key = identity
            if row["result_revision"] is not None:
                result_key = canonical(["window", key, start, start + spec["size_ms"]])
                old = connection.execute(
                    "SELECT document FROM results WHERE pipeline=? AND result_key=?",
                    (name, result_key),
                ).fetchone()
                if old is not None:
                    self.app.results.publish(
                        connection,
                        name,
                        result_key,
                        json.loads(old["document"]),
                        delete=True,
                    )
                    report["results_retracted"] += 1
            connection.execute(
                "DELETE FROM window_status WHERE pipeline=? AND bucket=? "
                "AND window_start=? AND group_key=?",
                (name, bucket, start, key),
            )
            report["windows_removed"] += 1

        # 3. Recompute every closed window and publish changed metrics.
        for state in connection.execute(
            "SELECT * FROM window_status WHERE pipeline=? AND status='closed' "
            "ORDER BY window_end,bucket,group_key",
            (name,),
        ):
            events = self.app.windows.rows_in(
                connection, name, state["bucket"], state["window_start"],
                state["group_key"],
            )
            document = {
                "kind": "window",
                "key": state["group_key"],
                "start": state["window_start"],
                "end": state["window_end"],
                "metrics": summarize(events, plan["metrics"]),
            }
            result_key = canonical(
                [
                    "window",
                    state["group_key"],
                    state["window_start"],
                    state["window_end"],
                ]
            )
            old = connection.execute(
                "SELECT revision,document FROM results WHERE pipeline=? AND result_key=?",
                (name, result_key),
            ).fetchone()
            encoded = canonical(document)
            if old is None or old["document"] != encoded:
                revision = self.app.results.publish(
                    connection, name, result_key, document
                )
                report["results_updated"] += 1
            else:
                revision = old["revision"]
            if revision != state["result_revision"]:
                connection.execute(
                    "UPDATE window_status SET result_revision=? WHERE pipeline=? "
                    "AND bucket=? AND window_start=? AND group_key=?",
                    (
                        revision,
                        name,
                        state["bucket"],
                        state["window_start"],
                        state["group_key"],
                    ),
                )
        return report

    def reconcile(self, pipeline=None):
        with self.app.catalog.transaction(True) as connection:
            if pipeline is None:
                names = [
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM pipelines ORDER BY name"
                    )
                ]
            else:
                self.app.catalog.pipeline(pipeline, connection)
                names = [pipeline]
            reports = []
            for name in names:
                report = self.reconcile_pipeline(connection, name)
                if any(report[key] for key in CHANGE_KEYS):
                    connection.execute(
                        "UPDATE pipelines SET generation=generation+1 WHERE name=?",
                        (name,),
                    )
                    self.app.catalog.event(
                        connection, name, "window_membership_repaired", report
                    )
                reports.append(report)
        return {"pipelines": reports}
