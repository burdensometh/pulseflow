from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import copy
import json
import unittest

from pulseflow import PulseFlow
from pulseflow.consumer import Consumer
from pulseflow.spec import plan_spec
from pulseflow.state import bucket_for
from pulseflow.values import Conflict, Invalid, Missing, canonical
from pulseflow.windows import starts_for


PLAN = {
    "window": {"kind": "tumbling", "size_ms": 10},
    "metrics": ["count", "sum", "mean", "min", "max", "p50", "p95", "distinct"],
}


def reading(event_id, at, value="1", key="a", kind="reading", attrs=None):
    return {
        "id": event_id,
        "key": key,
        "time": at,
        "value": value,
        "kind": kind,
        "attrs": attrs or {},
    }


def record(offset, value, partition=0):
    return {"partition": partition, "offset": offset, "event": value}


class FlowTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "flow")
        self.app.journal.create("readings", 1)
        self.app.engine.create("flow", "readings", PLAN)

    def append(self, rows, batch="batch"):
        return self.app.journal.append(
            "readings", batch, [record(i, row) for i, row in enumerate(rows)]
        )

    def test_ingress_identity_and_atomic_offsets(self):
        rows = [reading("one", 0), reading("two", 2)]
        first = self.append(rows)
        self.assertEqual(first["accepted"], 2)
        self.assertTrue(self.append(rows)["replayed"])
        self.assertEqual(self.app.journal.positions("readings"), {"0": 2})
        self.assertEqual(len(self.app.journal.read("readings", partition=0)), 2)
        self.assertEqual(len(self.app.journal.key_history("readings", "a")), 2)
        self.assertEqual(self.app.journal.receipt("readings", "batch")["accepted"], 2)
        self.assertIsNone(self.app.journal.receipt("readings", "missing"))
        with self.assertRaises(Conflict):
            self.append([reading("other", 0)])
        with self.assertRaises(Conflict):
            self.app.journal.append("readings", "gap", [record(3, reading("three", 3))])
        with self.assertRaises(Conflict):
            self.app.journal.append(
                "readings", "collision", [record(2, reading("one", 4))]
            )
        with self.assertRaises(Conflict):
            self.app.journal.append(
                "readings", "mismatch", [record(0, reading("one", 99))]
            )
        replay = self.app.journal.append(
            "readings", "new_receipt", [record(0, rows[0])]
        )
        self.assertEqual(replay["accepted"], 0)
        with self.assertRaises(Invalid):
            self.app.journal.append(
                "readings", "invalid", [record(2, reading("valid", 4)), {}]
            )
        self.assertEqual(self.app.journal.positions("readings"), {"0": 2})
        self.assertFalse(self.app.journal.receipt("readings", "invalid"))
        self.assertEqual(self.app.journal.create("readings", 1)["partitions"], 1)
        with self.assertRaises(Conflict):
            self.app.journal.create("readings", 2)

    def test_closed_windows_output_queries_and_replay(self):
        self.append(
            [reading("a", 0, "2"), reading("b", 5, "10"), reading("c", 10, "4")]
        )
        result = self.app.engine.drain("flow", limit=1)
        self.assertEqual(result["processed"], 3)
        rows = self.app.results.rows("flow")
        self.assertEqual(rows[0]["document"]["metrics"]["sum"], "12")
        self.assertEqual(rows[0]["document"]["metrics"]["p95"], "10")
        self.assertEqual(
            self.app.results.get("flow", rows[0]["result_key"])["revision"], 1
        )
        self.assertIsNone(self.app.results.get("flow", "not-a-key"))
        self.assertEqual(len(self.app.results.changes("flow")), 1)
        self.assertEqual(self.app.engine.status("flow")["pending"], 0)
        self.assertTrue(self.app.engine.runs("flow"))
        self.assertEqual(
            self.app.queries.event("flow", "b")["windows"][0]["window_start"], 0
        )
        self.assertEqual(
            len(self.app.queries.window_lineage("flow", 0, "a")["events"]), 2
        )
        self.assertEqual(self.app.queries.statistics("flow")["metrics"]["sum"], "16")
        self.assertEqual(self.app.queries.top("flow", 1)[0]["id"], "b")
        self.assertEqual(self.app.queries.lag("flow")[0]["pending"], 0)
        self.assertTrue(self.app.consistency.check("flow")["ok"])
        forked = self.app.replay.fork("flow", "fork")
        self.assertEqual(forked["replayed"], 3)
        self.assertTrue(self.app.replay.compare("flow", "fork")["equal"])
        self.assertEqual(self.app.replay.disposition_diff("flow", "fork"), [])
        self.assertEqual(self.app.replay.source_multiset("flow")["rows"], 3)
        different = copy.deepcopy(PLAN)
        different["mapping"] = {"value": {"literal": "2"}}
        self.app.replay.fork("flow", "other", different)
        self.assertFalse(self.app.replay.compare("flow", "other")["equal"])
        self.assertTrue(self.app.replay.disposition_diff("flow", "other"))
        self.assertTrue(self.app.catalog.history("flow"))
        self.assertEqual(
            self.app.engine.create("flow", "readings", PLAN)["name"], "flow"
        )
        with self.assertRaises(Conflict):
            self.app.engine.create("flow", "readings", PLAN, buckets=5)

    def test_hopping_membership_and_exclusive_right_boundary(self):
        plan = {
            "window": {"kind": "hopping", "size_ms": 10, "slide_ms": 3},
            "delay_ms": 100,
        }
        self.app.engine.create("hops", "readings", plan)
        self.append(
            [reading("negative", -1), reading("boundary", 0), reading("uneven", 2)]
        )
        self.app.engine.drain("hops")
        self.assertEqual(
            sorted(starts_for(-1, self.app.catalog.pipeline("hops")["plan"]["window"])),
            [-9, -6, -3],
        )
        self.assertEqual(len(self.app.queries.event("hops", "uneven")["windows"]), 3)
        self.assertEqual(len(self.app.queries.event("hops", "boundary")["windows"]), 4)
        self.assertTrue(self.app.consistency.check("hops")["ok"])
        self.app.watermarks.declare("hops", 0, 200)
        self.app.engine.drain("hops")
        self.assertEqual(
            len(self.app.windows.inspect("hops")), len(self.app.results.rows("hops"))
        )
        for row in self.app.windows.inspect("hops"):
            self.assertEqual(row["status"], "closed")

    def test_watermarks_wait_for_all_partitions_and_keep_late_evidence(self):
        self.app.journal.create("two", 2)
        self.app.engine.create("twoflow", "two", PLAN)
        self.app.journal.append("two", "first", [record(0, reading("fast", 100))])
        self.app.engine.drain("twoflow")
        self.assertIsNone(self.app.watermarks.status("twoflow")["watermark"])
        self.app.journal.append("two", "second", [record(0, reading("slow", 5), 1)])
        self.app.engine.drain("twoflow")
        self.assertEqual(self.app.watermarks.status("twoflow")["watermark"], 5)
        self.app.watermarks.declare("twoflow", 1, 50)
        self.app.engine.drain("twoflow")
        self.assertEqual(self.app.watermarks.status("twoflow")["watermark"], 50)
        self.app.journal.append("two", "late", [record(1, reading("late", 49), 1)])
        self.app.engine.drain("twoflow")
        self.assertEqual(self.app.engine.late("twoflow")[0]["event_id"], "late")
        self.assertEqual(
            self.app.queries.event("twoflow", "late")["rejection_watermark"], 50
        )
        with self.assertRaises(Conflict):
            self.app.watermarks.declare("twoflow", 1, 40)
        self.app.journal.append("two", "unread", [record(2, reading("unread", 60), 1)])
        with self.assertRaises(Conflict):
            self.app.watermarks.declare("twoflow", 1, 70)

    def test_idle_partition_leaves_and_rejoins_watermark_coordination(self):
        self.app.journal.create("devices", 2)
        plan = copy.deepcopy(PLAN)
        plan["idle"] = {"timeout_ms": 1000}
        self.app.engine.create("idleflow", "devices", plan)
        self.app.journal.append(
            "devices",
            "start",
            [record(0, reading("a0", 0)), record(0, reading("b0", 0), 1)],
        )
        self.app.engine.drain("idleflow", now_ms=0)
        self.assertEqual(self.app.watermarks.status("idleflow")["watermark"], 0)

        # Device 0 keeps reporting far ahead while device 1 stays silent.
        self.app.journal.append("devices", "fast", [record(1, reading("a1", 100))])
        self.app.engine.drain("idleflow", now_ms=500)
        status = self.app.watermarks.status("idleflow")
        self.assertEqual(status["watermark"], 0)
        self.assertEqual(status["idle_partitions"], [])

        # Sustained silence past the timeout retires device 1, so the window
        # belonging to device 0 closes.  Device 0 has to keep reporting; if it
        # went silent too there would be nothing to anchor the watermark.
        self.app.journal.append("devices", "fast2", [record(2, reading("a2", 200))])
        self.app.engine.drain("idleflow", now_ms=1600)
        status = self.app.watermarks.status("idleflow")
        self.assertEqual(status["idle_partitions"], [1])
        self.assertEqual(status["watermark"], 200)
        self.assertTrue(self.app.results.rows("idleflow"))

        # Device 1 resumes: an old record is late, a fresh one is computed,
        # and the partition rejoins coordination immediately.
        self.app.journal.append(
            "devices",
            "resume",
            [record(1, reading("b-old", 50), 1), record(2, reading("b-new", 400), 1)],
        )
        self.app.engine.drain("idleflow", now_ms=1700)
        status = self.app.watermarks.status("idleflow")
        self.assertEqual(status["idle_partitions"], [])
        self.assertEqual(
            [row["event_id"] for row in self.app.engine.late("idleflow")], ["b-old"]
        )
        self.assertEqual(
            self.app.queries.event("idleflow", "b-new")["processed"]["disposition"],
            "accepted",
        )
        # The watermark is the minimum over *active* partitions, so it still
        # waits for device 0's own event time (200), not the resumed 400.
        self.assertEqual(status["watermark"], 200)
        self.app.journal.append("devices", "ahead", [record(3, reading("a3", 500))])
        self.app.engine.drain("idleflow", now_ms=1800)
        self.assertGreaterEqual(
            self.app.watermarks.status("idleflow")["watermark"], 400
        )
        self.assertTrue(self.app.consistency.check("idleflow")["ok"])

    def test_idle_requires_no_backlog_and_never_advances_when_all_idle(self):
        self.app.journal.create("devices", 2)
        plan = copy.deepcopy(PLAN)
        plan["idle"] = {"timeout_ms": 100}
        self.app.engine.create("idleflow", "devices", plan)
        self.app.journal.append(
            "devices",
            "start",
            [record(0, reading("a0", 0)), record(0, reading("b0", 0), 1)],
        )
        self.app.engine.drain("idleflow", now_ms=0)

        # Both partitions go silent: nothing may be invented.  The watermark
        # holds at the last real value and no window closes on its own.
        self.app.engine.drain("idleflow", now_ms=1000)
        self.app.engine.drain("idleflow", now_ms=2000)
        status = self.app.watermarks.status("idleflow")
        self.assertEqual(status["idle_partitions"], [0, 1])
        self.assertEqual(status["watermark"], 0)
        self.assertEqual(self.app.results.rows("idleflow"), [])
        self.assertEqual(self.app.windows.inspect("idleflow")[0]["status"], "open")

        # Device 1 has an unread record: that is backlog, not silence.  Even
        # with the processing clock far ahead it must stay in coordination.
        self.app.journal.append(
            "devices", "backlog", [record(1, reading("b1", 500), 1)]
        )
        status = self.app.watermarks.status("idleflow")
        self.assertNotIn(1, status["idle_partitions"])
        self.app.engine.drain("idleflow", now_ms=3000)
        self.assertEqual(
            self.app.queries.event("idleflow", "b1")["processed"]["disposition"],
            "accepted",
        )
        self.assertTrue(self.app.consistency.check("idleflow")["ok"])

    def test_idle_clock_rollback_and_restart_never_close_early(self):
        self.app.journal.create("devices", 2)
        plan = copy.deepcopy(PLAN)
        plan["idle"] = {"timeout_ms": 1000}
        self.app.engine.create("idleflow", "devices", plan)
        self.app.journal.append(
            "devices",
            "start",
            [record(0, reading("a0", 0)), record(0, reading("b0", 0), 1)],
        )
        self.app.engine.drain("idleflow", now_ms=0)
        self.app.journal.append("devices", "fast", [record(1, reading("a1", 500))])
        self.app.engine.drain("idleflow", now_ms=500)
        self.assertEqual(self.app.watermarks.status("idleflow")["watermark"], 0)

        # A fresh process reads the same catalog with the wall clock rolled
        # back; the persisted monotonic clock must not be rewound, so device 1
        # neither idles early nor lets the watermark jump backwards.
        restarted = PulseFlow(self.home / "flow")
        restarted.engine.drain("idleflow", now_ms=0)
        status = restarted.watermarks.status("idleflow")
        self.assertEqual(status["watermark"], 0)
        self.assertEqual(status["idle_partitions"], [])

        # Once the real clock catches up, idling proceeds normally.  Device 0
        # keeps reporting so there is still an anchor for the watermark.
        restarted.journal.append("devices", "fast2", [record(2, reading("a2", 500))])
        restarted.engine.drain("idleflow", now_ms=1500)
        status = restarted.watermarks.status("idleflow")
        self.assertEqual(status["idle_partitions"], [1])
        self.assertEqual(status["watermark"], 500)
        self.assertTrue(restarted.consistency.check("idleflow")["ok"])

    def test_idle_forward_clock_spike_cannot_close_windows_on_its_own(self):
        self.app.journal.create("devices", 2)
        plan = copy.deepcopy(PLAN)
        plan["idle"] = {"timeout_ms": 1000}
        self.app.engine.create("idleflow", "devices", plan)
        self.app.journal.append(
            "devices",
            "start",
            [record(0, reading("a0", 0)), record(0, reading("b0", 0), 1)],
        )
        self.app.engine.drain("idleflow", now_ms=0)

        # A forward clock spike can retire the silent device, but the fast
        # device's own event time still bounds the watermark: it may not jump
        # to the clock, only to evidence the remaining partition provides.
        self.app.journal.append("devices", "spike", [record(1, reading("a1", 100))])
        self.app.engine.drain("idleflow", now_ms=10**9)
        status = self.app.watermarks.status("idleflow")
        self.assertEqual(status["idle_partitions"], [1])
        self.assertEqual(status["watermark"], 100)

        # With every partition silent nothing anchors the watermark, so a
        # later clock advance leaves it exactly where the evidence put it.
        self.app.engine.drain("idleflow", now_ms=2 * 10**9)
        status = self.app.watermarks.status("idleflow")
        self.assertEqual(status["idle_partitions"], [0, 1])
        self.assertEqual(status["watermark"], 100)

    def test_idle_restart_advances_only_on_real_evidence(self):
        self.app.journal.create("devices", 2)
        plan = copy.deepcopy(PLAN)
        plan["idle"] = {"timeout_ms": 1000}
        self.app.engine.create("idleflow", "devices", plan)
        self.app.journal.append(
            "devices",
            "start",
            [record(0, reading("a0", 0)), record(0, reading("b0", 0), 1)],
        )
        self.app.engine.drain("idleflow", now_ms=0)
        self.app.journal.append("devices", "fast", [record(1, reading("a1", 100))])
        self.app.engine.drain("idleflow", now_ms=500)

        # A restart long after downtime finds both devices silent.  Silence is
        # sustained past the timeout, so both may retire — but with nothing
        # active there is no anchor and the watermark must freeze at the last
        # evidence (0 here), never jump to the processing clock.  No window may
        # close ahead of the data.
        restarted = PulseFlow(self.home / "flow")
        restarted.engine.drain("idleflow", now_ms=100000)
        status = restarted.watermarks.status("idleflow")
        self.assertEqual(status["idle_partitions"], [0, 1])
        self.assertEqual(status["watermark"], 0)
        self.assertTrue(restarted.consistency.check("idleflow")["ok"])
        self.assertTrue(
            all(
                row["window_end"] <= 0
                for row in restarted.windows.inspect("idleflow")
                if row["status"] == "closed"
            )
        )

        # Device 0 resumes reporting: it rejoins and the watermark follows its
        # real event time again.
        restarted.journal.append("devices", "resume", [record(2, reading("a2", 400))])
        restarted.engine.drain("idleflow", now_ms=101000)
        status = restarted.watermarks.status("idleflow")
        self.assertEqual(status["idle_partitions"], [1])
        self.assertEqual(status["watermark"], 400)

    def test_idle_declare_reactivates_a_retired_partition(self):
        self.app.journal.create("devices", 2)
        plan = copy.deepcopy(PLAN)
        plan["idle"] = {"timeout_ms": 100}
        self.app.engine.create("idleflow", "devices", plan)
        self.app.journal.append(
            "devices",
            "start",
            [record(0, reading("a0", 0)), record(0, reading("b0", 0), 1)],
        )
        self.app.engine.drain("idleflow", now_ms=0)
        self.app.journal.append("devices", "fast", [record(1, reading("a1", 100))])
        self.app.engine.drain("idleflow", now_ms=500)
        self.assertEqual(self.app.watermarks.status("idleflow")["idle_partitions"], [1])

        # An explicit declaration is activity: the partition rejoins and the
        # watermark waits for it again instead of skipping it as idle.  The
        # watermark itself is monotonic, so it stays at 100 rather than
        # retreating to the declared value.
        self.app.watermarks.declare("idleflow", 1, 50)
        status = self.app.watermarks.status("idleflow")
        self.assertEqual(status["idle_partitions"], [])
        self.assertEqual(status["watermark"], 100)
        self.assertTrue(self.app.consistency.check("idleflow")["ok"])

    def test_idle_state_survives_savepoint_and_restore(self):
        self.app.journal.create("devices", 2)
        plan = copy.deepcopy(PLAN)
        plan["idle"] = {"timeout_ms": 1000}
        self.app.engine.create("idleflow", "devices", plan)
        self.app.journal.append(
            "devices",
            "start",
            [record(0, reading("a0", 0)), record(0, reading("b0", 0), 1)],
        )
        self.app.engine.drain("idleflow", now_ms=0)
        self.app.journal.append("devices", "fast", [record(1, reading("a1", 300))])
        self.app.engine.drain("idleflow", now_ms=1500)
        self.assertEqual(self.app.watermarks.status("idleflow")["idle_partitions"], [1])

        point = self.app.savepoints.create("idleflow")
        self.app.savepoints.restore_point(point["point_id"], "restored")
        status = self.app.watermarks.status("restored")
        self.assertEqual(status["idle_partitions"], [1])
        self.assertEqual(status["watermark"], 300)
        self.assertTrue(self.app.consistency.check("restored")["ok"])
        self.assertTrue(self.app.replay.compare("idleflow", "restored")["equal"])

    def test_idle_plan_spelling_validation(self):
        base = {"window": {"kind": "tumbling", "size_ms": 10}}
        for extra, timeout in [
            ({"idle": 0}, 0),
            ({"idle": {"timeout_ms": 5}}, 5),
            ({"idle_timeout_ms": 5}, 5),
            ({"idle_partitions": {"timeout_ms": 7}}, 7),
        ]:
            self.assertEqual(
                plan_spec({**base, **extra})["idle"], {"timeout_ms": timeout}
            )
        for extra in [
            {"idle": {}},
            {"idle": True},
            {"idle": {"timeout_ms": 5, "idle_timeout_ms": 6}},
            {"idle": False, "idle_timeout_ms": 5},
            {"idle": {"enabled": "yes", "timeout_ms": 5}},
        ]:
            with self.subTest(extra=extra), self.assertRaises(Invalid):
                plan_spec({**base, **extra})
        self.assertNotIn("idle", plan_spec(base))

    def test_mapping_dedup_latest_and_sequence_lifecycle(self):
        plan = copy.deepcopy(PLAN)
        plan.update(
            delay_ms=100,
            dedup={"field": "attrs.token", "ttl_ms": 10},
            sequence={"first_kind": "A", "then_kind": "B", "within_ms": 10},
            mapping={
                "keep": {"op": "ne", "args": [{"field": "kind"}, {"literal": "skip"}]}
            },
        )
        self.app.engine.create("operators", "readings", plan)
        self.append(
            [
                reading("start", 0, kind="A", attrs={"token": "x"}),
                reading("duplicate", 1, attrs={"token": "x"}),
                reading("skip", 2, kind="skip"),
                reading("ignored", 3, kind="C"),
                reading("match", 5, kind="B"),
                reading("orphan", 6, kind="B", key="b"),
                reading("fresh", 12, kind="A", attrs={"token": "x"}),
                reading("older", 11, kind="A"),
                reading("beyond", 30, kind="B"),
            ]
        )
        self.app.engine.drain("operators")
        status = self.app.engine.status("operators")
        self.assertEqual(status["dispositions"]["duplicate"], 1)
        self.assertEqual(status["dispositions"]["filtered"], 1)
        sequences = [
            r
            for r in self.app.results.rows("operators")
            if r["document"]["kind"] == "sequence"
        ]
        self.assertEqual(len(sequences), 1)
        self.assertEqual(
            self.app.state.inspect("operators", "latest")[0]["document"]["id"], "beyond"
        )
        self.app.watermarks.declare("operators", 0, 300)
        self.app.engine.drain("operators")
        self.assertEqual(self.app.state.inspect("operators", "sequence"), [])

    def test_dimensions_asof_null_policy_and_retroactive_gap(self):
        versions = [
            {
                "dimension": "stations",
                "key": "a",
                "valid_from": 0,
                "revision": 1,
                "attrs": {"factor": "2"},
            },
            {
                "dimension": "stations",
                "key": "a",
                "valid_from": 10,
                "revision": 1,
                "attrs": {"factor": "3"},
            },
        ]
        first = self.app.dimensions.publish("dims", versions)
        self.assertTrue(self.app.dimensions.publish("dims", versions)["replayed"])
        self.assertEqual(
            self.app.dimensions.lookup("stations", "a", 10)["document"]["factor"], "3"
        )
        self.assertEqual(
            self.app.dimensions.intervals("stations", "a")[0]["valid_to"], 10
        )
        self.assertEqual(len(self.app.dimensions.history("stations", "a")), 2)
        plan = copy.deepcopy(PLAN)
        plan.update(
            dimension={"name": "stations"},
            mapping={
                "value": {
                    "op": "mul",
                    "args": [{"field": "value"}, {"field": "attrs.dimension.factor"}],
                }
            },
        )
        self.app.engine.create("enriched", "readings", plan)
        self.append([reading("one", 0), reading("two", 10)])
        self.app.engine.drain("enriched")
        self.assertEqual(
            self.app.results.rows("enriched")[0]["document"]["metrics"]["sum"], "2"
        )
        self.app.dimensions.publish(
            "correction", [{**versions[0], "revision": 2, "attrs": {"factor": "9"}}]
        )
        self.assertEqual(
            self.app.dimensions.lookup("stations", "a", 0)["document"]["factor"], "9"
        )
        impact = self.app.queries.dimension_impact("enriched", "stations", "a", 0, 10)
        self.assertEqual([r["event_id"] for r in impact["events"]], ["one"])
        self.assertEqual(
            self.app.results.rows("enriched")[0]["document"]["metrics"]["sum"], "2"
        )
        self.assertEqual(
            self.app.queries.dimension_impact("flow", "stations", "a", 0)["events"], []
        )
        with self.assertRaises(Conflict):
            self.app.dimensions.publish("dims", [])
        with self.assertRaises(Conflict):
            self.app.dimensions.publish(
                "same_version", [{**versions[0], "attrs": {"factor": "99"}}]
            )
        self.app.dimensions.publish(
            "delete", [{**versions[1], "revision": 2, "deleted": True}]
        )
        self.assertIsNone(self.app.dimensions.lookup("stations", "a", 10))
        with self.app.catalog.transaction() as connection:
            value = reading("absent", 0, key="missing")
            self.assertEqual(
                self.app.dimensions.enrich(
                    connection, value, {"name": "stations", "missing": "null"}
                )[0]["attrs"]["dimension"],
                None,
            )
            self.assertEqual(
                self.app.dimensions.enrich(
                    connection, value, {"name": "stations", "missing": "drop"}
                ),
                (None, None),
            )
            with self.assertRaises(Invalid):
                self.app.dimensions.enrich(
                    connection, value, {"name": "stations", "missing": "error"}
                )

    def test_worker_fences_and_failed_chunk_rolls_back_all_effects(self):
        lease = self.app.leases.acquire("flow", "worker", 0, 10)
        self.assertEqual(self.app.leases.acquire("flow", "worker", 1, 10), lease)
        with self.assertRaises(Conflict):
            self.app.leases.acquire("flow", "other", 1, 10)
        renewed = self.app.leases.renew(lease, 2, 10)
        self.assertEqual(renewed["expires_at"], 12)
        replacement = self.app.leases.acquire("flow", "other", 12, 10)
        with self.assertRaises(Conflict):
            self.app.engine.step("flow", lease, 12)
        self.app.leases.release(replacement, 13)
        self.append([reading("one", 0), reading("two", 10)])
        with patch.object(
            self.app.results, "publish", side_effect=OSError("author storage failure")
        ):
            with self.assertRaises(OSError):
                self.app.engine.drain("flow")
        self.assertEqual(self.app.engine.status("flow")["processed_events"], 0)
        self.assertEqual(self.app.results.rows("flow"), [])
        self.app.engine.drain("flow")
        self.assertEqual(self.app.engine.status("flow")["processed_events"], 2)

    def test_consumer_upsert_delete_bootstrap_and_atomic_failure(self):
        self.append([reading("one", 0), reading("two", 10)])
        self.app.engine.drain("flow")
        consumer = Consumer(self.home / "consumer.sqlite")
        consumer.catch_up(self.app, "flow", limit=1)
        self.assertTrue(self.app.queries.compare_consumer("flow", consumer)["equal"])
        old_head = self.app.results.head("flow")
        self.app.results.replace_visible("flow", old_head, {})
        self.assertFalse(self.app.queries.compare_consumer("flow", consumer)["equal"])
        consumer.catch_up(self.app, "flow")
        self.assertEqual(consumer.rows(), [])
        self.assertTrue(consumer.rows(include_deleted=True)[0]["deleted"])
        self.assertTrue(consumer.status()["cursor"])
        consumer.bootstrap(self.app, "flow")
        self.assertEqual(consumer.rows(), [])
        with self.assertRaises(Conflict):
            self.app.results.replace_visible("flow", old_head, {})
        other = PulseFlow(self.home / "other")
        other.journal.create("readings", 1)
        other.engine.create("flow", "readings", PLAN)
        with self.assertRaises(Conflict):
            consumer.apply(other, "flow")
        with self.assertRaises(OSError):
            with consumer.transaction() as connection:
                connection.execute("DELETE FROM materialized")
                raise OSError("rollback")
        self.assertEqual(len(consumer.rows(include_deleted=True)), 1)
