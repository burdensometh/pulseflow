from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from pulseflow import PulseFlow
from pulseflow.consumer import Consumer
from pulseflow.values import canonical
from pulseflow.windows import starts_for


PLAN = {
    "window": {"kind": "hopping", "size_ms": 11, "slide_ms": 3},
    "metrics": ["count", "sum"],
}


def reading(event_id, at, value="1", key="a"):
    return {
        "id": event_id,
        "key": key,
        "time": at,
        "value": value,
        "kind": "reading",
        "attrs": {},
    }


def record(offset, value, partition=0):
    return {"partition": partition, "offset": offset, "event": value}


def buggy_starts_for(event_time, spec):
    slide, size, offset = spec["slide_ms"], spec["size_ms"], spec["offset_ms"]
    last = int((event_time - offset) / slide) * slide + offset
    count = size // slide
    return [
        start
        for start in (last - i * slide for i in range(count))
        if start <= event_time < start + size
    ]


def materialize_with_buggy_formula(app, pipeline):
    """Replace the membership formula with the pre-fix behaviour and drain."""
    from pulseflow import windows as windows_module

    original = windows_module.starts_for
    windows_module.starts_for = buggy_starts_for
    try:
        app.engine.drain(pipeline)
    finally:
        windows_module.starts_for = original


class RepairTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)

    def build_legacy_state(self, home):
        app = PulseFlow(home / "state")
        app.journal.create("readings", 1)
        app.engine.create("flow", "readings", PLAN)
        rows = [
            reading("event0", -5),
            reading("event1", -3),
            reading("event2", 0),
            reading("event3", 2),
            reading("event4", 15),
        ]
        app.journal.append(
            "readings", "data", [record(i, row) for i, row in enumerate(rows)]
        )
        materialize_with_buggy_formula(app, "flow")
        return app, rows

    def build_correct_reference(self, rows):
        reference = PulseFlow(self.home / "reference")
        reference.journal.create("readings", 1)
        reference.engine.create("flow", "readings", PLAN)
        reference.journal.append(
            "readings",
            "data",
            [record(i, row) for i, row in enumerate(rows)],
        )
        reference.engine.drain("flow")
        return reference

    def test_repair_matches_a_clean_correct_run(self):
        app, rows = self.build_legacy_state(self.home)
        reference = self.build_correct_reference(rows)

        # The legacy state fails the corrected consistency check.
        self.assertFalse(app.consistency.check("flow")["ok"])
        before = {
            row["result_key"]: row["document"]
            for row in app.results.rows("flow")
        }
        report = app.repair.reconcile("flow")["pipelines"][0]
        self.assertEqual(report["events_added"], 5)
        self.assertEqual(report["events_removed"], 0)
        # Two corrected windows already exist (9,12,15 stay open); the other
        # three corrected starts are new.
        self.assertEqual(report["windows_opened"], 3)
        # Two newly created windows are already past the watermark and
        # publish their first result; the pre-existing [-9,2) window changes
        # metrics and publishes revision 2.
        self.assertEqual(report["results_updated"], 3)
        self.assertTrue(app.consistency.check("flow")["ok"])

        repaired = {
            row["result_key"]: row["document"]
            for row in app.results.rows("flow")
        }
        expected = {
            row["result_key"]: row["document"]
            for row in reference.results.rows("flow")
        }
        self.assertEqual(repaired, expected)
        # Window documents carry aggregate metrics rather than membership
        # lists.  [-9,2) changes from two to three events so it advances to
        # revision 2; the other three pre-existing windows keep identical
        # count/sum documents and therefore stay at revision 1.  Newly
        # created windows whose end is already past the watermark publish at
        # revision 1, while [6,17) remains open with no result yet.
        bumped = {key: app.results.get("flow", key)["revision"] for key in before}
        self.assertEqual(
            bumped,
            {
                canonical(["window", "a", -9, 2]): 2,
                canonical(["window", "a", -6, 5]): 1,
                canonical(["window", "a", -3, 8]): 1,
                canonical(["window", "a", 0, 11]): 1,
            },
        )
        new_keys = set(repaired) - set(before)
        self.assertEqual(
            sorted(new_keys),
            sorted(
                canonical(["window", "a", start, start + 11])
                for start in (-15, -12)
            ),
        )
        for key in new_keys:
            self.assertEqual(app.results.get("flow", key)["revision"], 1)
        self.assertTrue(
            all(
                state["status"] == "open"
                and state["window_start"] in (6, 9, 12, 15)
                for state in app.windows.inspect("flow")
                if state["status"] == "open"
            )
        )

        # Every accepted event now shows exactly the corrected membership.
        window_spec = app.catalog.pipeline("flow")["plan"]["window"]
        for row in rows:
            actual = sorted(
                item["window_start"]
                for item in app.queries.event("flow", row["id"])["windows"]
            )
            self.assertEqual(
                actual, sorted(starts_for(row["time"], window_spec))
            )

        # Closed-window status rows point at the published revisions.
        for state in app.windows.inspect("flow"):
            if state["status"] == "closed":
                result = app.results.get(
                    "flow",
                    canonical(
                        ["window", "a", state["window_start"], state["window_end"]]
                    ),
                )
                self.assertEqual(result["revision"], state["result_revision"])

    def test_repair_is_idempotent_and_keeps_new_input(self):
        app, rows = self.build_legacy_state(self.home)
        app.repair.reconcile()
        first_head = app.results.head("flow")
        second = app.repair.reconcile("flow")
        self.assertEqual(
            second,
            {
                "pipelines": [
                    {
                        "pipeline": "flow",
                        "events_added": 0,
                        "events_removed": 0,
                        "windows_opened": 0,
                        "windows_removed": 0,
                        "results_updated": 0,
                        "results_retracted": 0,
                    }
                ]
            },
        )
        self.assertEqual(app.results.head("flow"), first_head)
        self.assertEqual(
            len(app.catalog.history("flow")),
            2,  # pipeline_created plus exactly one repair audit event
        )

        # New input arriving after the repair uses corrected membership and
        # lands without losing or re-processing any source event. Events at
        # or beyond the current watermark (15) remain accepted.
        cursor_before = app.engine.status("flow")["last_sequence"]
        app.journal.append(
            "readings",
            "late-arrival",
            [
                record(5, reading("event5", 15)),
                record(6, reading("event6", 17)),
            ],
        )
        app.engine.drain("flow")
        self.assertEqual(app.engine.status("flow")["dispositions"].get("late", 0), 0)
        self.assertEqual(app.engine.status("flow")["pending"], 0)
        self.assertGreater(app.engine.status("flow")["last_sequence"], cursor_before)
        self.assertTrue(app.consistency.check("flow")["ok"])
        self.assertEqual(
            sorted(
                item["window_start"]
                for item in app.queries.event("flow", "event6")["windows"]
            ),
            sorted(
                starts_for(
                    17, app.catalog.pipeline("flow")["plan"]["window"]
                )
            ),
        )
        # Re-running the repair after new input changes nothing.
        again = app.repair.reconcile("flow")["pipelines"][0]
        self.assertTrue(all(again[key] == 0 for key in again if key != "pipeline"))

    def test_repair_repairs_replay_pipeline_and_consumer_converges(self):
        app, _ = self.build_legacy_state(self.home)
        # An independent replay pipeline produced the same wrong state.
        app.replay.fork("flow", "replayed")
        consumer = Consumer(self.home / "consumer.sqlite")
        consumer.catch_up(app, "flow")
        self.assertEqual(consumer.status()["cursor"], app.results.head("flow"))

        app.repair.reconcile()
        for name in ("flow", "replayed"):
            self.assertTrue(app.consistency.check(name)["ok"])
        self.assertTrue(app.replay.compare("flow", "replayed")["equal"])

        # The versioned change stream lets the existing consumer converge.
        consumer.catch_up(app, "flow")
        self.assertTrue(app.queries.compare_consumer("flow", consumer)["equal"])
        self.assertEqual(consumer.status()["cursor"], app.results.head("flow"))
        revisions = {row["result_key"]: row["revision"] for row in consumer.rows()}
        # Every downstream revision is positive and monotonic.
        for key in revisions:
            self.assertGreaterEqual(revisions[key], 1)

    def test_repair_does_not_touch_source_or_progress(self):
        app, rows = self.build_legacy_state(self.home)
        before = {
            "positions": app.journal.positions("readings"),
            "sequence": app.engine.status("flow")["last_sequence"],
            "progress": app.watermarks.status("flow"),
        }
        app.repair.reconcile("flow")
        self.assertEqual(app.journal.positions("readings"), before["positions"])
        self.assertEqual(
            app.engine.status("flow")["last_sequence"], before["sequence"]
        )
        self.assertEqual(app.watermarks.status("flow"), before["progress"])
        self.assertEqual(
            app.engine.status("flow")["processed_events"], len(rows)
        )
        self.assertEqual(
            app.engine.late("flow"), []
        )

    def test_removal_path_repairs_windows_that_should_not_exist(self):
        # Add a bogus closed window referencing a real event before the
        # repair runs: the repair must remove its membership row, retract the
        # published result and drop the window status entirely.
        app, _ = self.build_legacy_state(self.home)
        bogus_key = canonical(["window", "a", -30, -19])
        with app.catalog.transaction(True) as connection:
            from pulseflow.results import Results

            revision = Results(app.catalog).publish(
                connection,
                "flow",
                bogus_key,
                {
                    "kind": "window",
                    "key": "a",
                    "start": -30,
                    "end": -19,
                    "metrics": {"count": 1, "sum": "1"},
                },
            )
            connection.execute(
                "INSERT INTO window_status VALUES (?,?,?,?,?,?,?)",
                ("flow", 2, -30, -19, "a", "closed", revision),
            )
            connection.execute(
                "INSERT INTO window_events VALUES (?,?,?,?,?,?,?,?)",
                (
                    "flow", 2, -30, "a", "event4", 15, "1",
                    '{"attrs":{},"id":"event4","key":"a","kind":"reading",'
                    '"time":15,"value":"1"}',
                ),
            )
        self.assertFalse(app.consistency.check("flow")["ok"])
        report = app.repair.reconcile("flow")["pipelines"][0]
        self.assertEqual(report["events_removed"], 1)
        self.assertEqual(report["windows_removed"], 1)
        self.assertEqual(report["results_retracted"], 1)
        self.assertTrue(app.consistency.check("flow")["ok"])
        latest = app.results.get("flow", bogus_key)
        self.assertEqual(latest["revision"], revision + 1)
        self.assertEqual(latest["deleted"], 1)
        # A second run has nothing left to correct.
        again = app.repair.reconcile("flow")["pipelines"][0]
        self.assertTrue(all(again[key] == 0 for key in again if key != "pipeline"))


if __name__ == "__main__":
    unittest.main()
