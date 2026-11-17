from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import copy
import json
import unittest

from pulseflow import PulseFlow
from pulseflow.consumer import Consumer
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
        # size 10 / slide 3 means a row belongs to at most ceil(10/3) = 4
        # windows; t=2 only reaches the three windows still covering it
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

    def test_hopping_nondivisible_size_and_negative_grid_membership(self):
        # 11 is not a multiple of 3: every row belongs to ceil(11/3) = 4 windows
        plan = {"window": {"kind": "hopping", "size_ms": 11, "slide_ms": 3}}
        self.app.engine.create("grid", "readings", plan)
        rows = [
            reading("event0", -5),
            reading("event1", -3),
            reading("event2", 0),
            reading("event3", 2),
            reading("event4", 15),
        ]
        self.app.journal.append(
            "readings", "batch", [record(i, row) for i, row in enumerate(rows)]
        )
        self.app.engine.drain("grid")
        expected = {
            "event0": [-15, -12, -9, -6],
            "event1": [-12, -9, -6, -3],
            "event2": [-9, -6, -3, 0],
            "event3": [-6, -3, 0],
            "event4": [6, 9, 12, 15],
        }
        for event_id, starts in expected.items():
            actual = sorted(
                row["window_start"]
                for row in self.app.queries.event("grid", event_id)["windows"]
            )
            self.assertEqual(actual, starts)
        # right boundary stays exclusive: event at t=2 is not in [-9,2);
        # t=0 is inside that window, t=2 instead belongs to [-6,5)
        minus_nine = self.app.windows.preview("grid", -9, "a")["rows"]
        self.assertEqual([row["id"] for row in minus_nine], ["event0", "event1", "event2"])
        minus_six = self.app.windows.preview("grid", -6, "a")["rows"]
        self.assertEqual(
            [row["id"] for row in minus_six],
            ["event0", "event1", "event2", "event3"],
        )
        self.assertTrue(self.app.consistency.check("grid")["ok"])

    def test_starts_for_grid_offset_and_negative_offsets(self):
        shifted = {"kind": "hopping", "size_ms": 10, "slide_ms": 10, "offset_ms": 7}
        self.assertEqual(starts_for(0, shifted), [-3])
        self.assertEqual(starts_for(-3, shifted), [-3])
        self.assertEqual(starts_for(7, shifted), [7])
        self.assertEqual(starts_for(6, shifted), [-3])
        negative_offset = {
            "kind": "hopping",
            "size_ms": 11,
            "slide_ms": 3,
            "offset_ms": -4,
        }
        for t in range(-30, 31):
            for start in starts_for(t, negative_offset):
                self.assertIn((start + 4) % 3, (0,))
                self.assertLessEqual(start, t)
                self.assertLess(t, start + 11)
        tumbling = {"kind": "tumbling", "size_ms": 10, "slide_ms": 10, "offset_ms": 0}
        self.assertEqual(starts_for(-1, tumbling), [-10])
        self.assertEqual(starts_for(-10, tumbling), [-10])
        self.assertEqual(starts_for(-11, tumbling), [-20])

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
