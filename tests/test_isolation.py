from pathlib import Path
from tempfile import TemporaryDirectory
import copy
import unittest

from pulseflow import PulseFlow
from pulseflow.values import Conflict, Invalid, Missing
from test_flow import PLAN, reading, record


ISO_PLAN = copy.deepcopy(PLAN)
ISO_PLAN["error_mode"] = "isolate"
ISO_PLAN["mapping"] = {
    "value": {
        "op": "div",
        "args": [{"field": "value"}, {"field": "attrs.divisor"}],
    }
}


def iso_reading(event_id, at, value="10", divisor="2", key="a"):
    return reading(event_id, at, value=value, key=key, attrs={"divisor": divisor})


class IsolationTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "flow")
        self.app.journal.create("readings", 2)
        self.app.engine.create("flow", "readings", ISO_PLAN)

    def test_fault_isolated_and_other_partition_proceeds(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, iso_reading("p0-good", 0), 0),
                record(1, iso_reading("p0-bad", 1, divisor="0"), 0),
                record(2, iso_reading("p0-after", 2), 0),
                record(0, iso_reading("p1-a", 0, key="b"), 1),
                record(1, iso_reading("p1-b", 1, key="b"), 1),
            ],
        )
        result = self.app.engine.drain("flow")
        self.assertEqual(result["faulted"], 1)
        self.assertEqual(result["held"], 1)
        status = self.app.engine.status("flow")
        self.assertEqual(status["active_faults"], 1)
        faults = self.app.engine.faults("flow")
        self.assertEqual(len(faults), 1)
        self.assertEqual(faults[0]["event_id"], "p0-bad")
        self.assertIn("division by zero", faults[0]["reason"])
        accepted = {row["id"] for row in self.app.queries.accepted("flow")}
        self.assertIn("p1-a", accepted)
        self.assertIn("p1-b", accepted)
        self.assertIn("p0-good", accepted)
        self.assertNotIn("p0-bad", accepted)
        self.assertNotIn("p0-after", accepted)
        # p0-after must be held, not processed
        held = self.app.engine.held_events("flow")
        self.assertEqual([r["event_id"] for r in held], ["p0-after"])
        self.assertTrue(self.app.consistency.check("flow")["ok"])

    def test_watermark_cannot_pass_unresolved_fault(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, iso_reading("bad", 5, divisor="0"), 0),
                record(0, iso_reading("slow", 100, key="b"), 1),
            ],
        )
        self.app.engine.drain("flow")
        watermark = self.app.watermarks.status("flow")["watermark"]
        self.assertLessEqual(watermark, 5)
        with self.assertRaises(Conflict):
            self.app.watermarks.declare("flow", 0, 50)

    def test_repair_preserves_order_and_resumes_held(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, iso_reading("p0-good", 0), 0),
                record(1, iso_reading("p0-bad", 1, divisor="0"), 0),
                record(2, iso_reading("p0-after", 2), 0),
            ],
        )
        self.app.engine.drain("flow")
        self.assertEqual(
            [r["event_id"] for r in self.app.engine.held_events("flow")],
            ["p0-after"],
        )
        resolved = self.app.engine.repair(
            "flow", 0, 1, {"value": "6", "attrs.divisor": "2"}
        )
        self.assertEqual(resolved["outcome"], "resolved")
        self.assertEqual(resolved["held_processed"], 1)
        self.assertEqual(self.app.engine.faults("flow"), [])
        self.assertEqual(self.app.engine.held_events("flow"), [])
        chain = self.app.engine.repairs("flow", 0, 1)
        self.assertEqual([row["outcome"] for row in chain], ["resolved"])
        self.assertTrue(self.app.consistency.check("flow")["ok"])
        # journal is untouched
        self.assertEqual(
            self.app.journal.read("readings", partition=0)[1]["document"]["value"],
            "10",
        )

    def test_failed_correction_keeps_fault_and_records_attempt(self):
        self.app.journal.append(
            "readings", "batch", [record(0, iso_reading("bad", 0, divisor="0"), 0)]
        )
        self.app.engine.drain("flow")
        failed = self.app.engine.repair("flow", 0, 0, {"value": "not-a-number"})
        self.assertEqual(failed["outcome"], "failed")
        self.assertEqual(failed["attempt"], 1)
        self.assertEqual(len(self.app.engine.faults("flow")), 1)
        good = self.app.engine.repair(
            "flow", 0, 0, {"value": "8", "attrs.divisor": "2"}
        )
        self.assertEqual(good["outcome"], "resolved")
        self.assertEqual(good["attempt"], 2)
        chain = self.app.engine.repairs("flow", 0, 0)
        self.assertEqual([row["outcome"] for row in chain], ["failed", "resolved"])

    def test_repair_survives_restart_chain(self):
        self.app.journal.append(
            "readings", "batch", [record(0, iso_reading("bad", 0, divisor="0"), 0)]
        )
        self.app.engine.drain("flow")
        self.app.engine.repair("flow", 0, 0, {"value": "bad"})
        reopened = PulseFlow(self.home / "flow")
        self.assertEqual(len(reopened.engine.faults("flow")), 1)
        resolved = reopened.engine.repair(
            "flow", 0, 0, {"value": "4", "attrs.divisor": "2"}
        )
        self.assertEqual(resolved["attempt"], 2)
        self.assertEqual(
            [row["outcome"] for row in reopened.engine.repairs("flow", 0, 0)],
            ["failed", "resolved"],
        )

    def test_resolved_fault_is_not_processed_twice(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, iso_reading("bad", 0, divisor="0"), 0),
                record(1, iso_reading("after", 1), 0),
            ],
        )
        self.app.engine.drain("flow")
        self.app.engine.repair(
            "flow", 0, 0, {"value": "2", "attrs.divisor": "2"}
        )
        before = self.app.engine.status("flow")["processed_events"]
        self.app.engine.drain("flow")
        after = self.app.engine.status("flow")["processed_events"]
        self.assertEqual(before, after)
        with self.assertRaises(Conflict):
            self.app.engine.repair(
            "flow", 0, 0, {"value": "2", "attrs.divisor": "2"}
        )
        with self.assertRaises(Missing):
            self.app.engine.repair("flow", 1, 99, {"value": "2"})

    def test_rollback_mode_still_available(self):
        strict_plan = copy.deepcopy(ISO_PLAN)
        strict_plan["error_mode"] = "rollback"
        self.app.engine.create("strict", "readings", strict_plan)
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, iso_reading("bad", 0, divisor="0"), 0),
                record(0, iso_reading("ok", 0, key="b"), 1),
            ],
        )
        with self.assertRaises(Invalid):
            self.app.engine.drain("strict")
        self.assertEqual(self.app.engine.status("strict")["processed_events"], 0)

    def test_savepoint_restores_fault_chain(self):
        self.app.journal.append(
            "readings", "batch", [record(0, iso_reading("bad", 0, divisor="0"), 0)]
        )
        self.app.engine.drain("flow")
        self.app.engine.repair("flow", 0, 0, {"value": "bad"})
        point = self.app.savepoints.create("flow")
        self.app.savepoints.restore_point(point["point_id"], "restored")
        faults = self.app.engine.faults("restored")
        self.assertEqual(len(faults), 1)
        chain = self.app.engine.repairs("restored", 0, 0)
        self.assertEqual([row["outcome"] for row in chain], ["failed"])
        resolved = self.app.engine.repair(
            "restored", 0, 0, {"value": "7", "attrs.divisor": "2"}
        )
        self.assertEqual(resolved["attempt"], 2)

    def test_order_preserved_across_held_boundary_batches(self):
        self.app.journal.append(
            "readings",
            "first",
            [
                record(0, iso_reading("a", 0), 0),
                record(1, iso_reading("bad", 1, divisor="0"), 0),
            ],
        )
        self.app.engine.drain("flow")
        self.app.journal.append(
            "readings",
            "second",
            [
                record(2, iso_reading("b", 2), 0),
                record(3, iso_reading("c", 3), 0),
            ],
        )
        self.app.engine.drain("flow")
        self.assertEqual(
            [r["event_id"] for r in self.app.engine.held_events("flow")],
            ["b", "c"],
        )
        self.app.engine.repair(
            "flow", 0, 1, {"value": "5", "attrs.divisor": "1"}
        )
        self.assertEqual(self.app.engine.held_events("flow"), [])
        order = [row["id"] for row in self.app.queries.accepted("flow")]
        self.assertEqual(order, ["a", "bad", "b", "c"])
        self.assertTrue(self.app.consistency.check("flow")["ok"])

    def test_successful_partition_not_reprocessed_after_repair(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, iso_reading("p0-bad", 0, divisor="0"), 0),
                record(0, iso_reading("p1-one", 0, key="b"), 1),
            ],
        )
        self.app.engine.drain("flow")
        self.app.engine.repair(
            "flow", 0, 0, {"value": "2", "attrs.divisor": "2"}
        )
        rows = self.app.queries.accepted("flow")
        self.assertEqual(sorted(row["id"] for row in rows), ["p0-bad", "p1-one"])
        self.assertEqual(len(rows), 2)
        self.assertEqual(self.app.engine.status("flow")["processed_events"], 2)
        self.assertEqual(self.app.engine.faults("flow"), [])

    def test_correction_cannot_alter_source_identity_or_time(self):
        self.app.journal.append(
            "readings", "batch", [record(0, iso_reading("bad", 0, divisor="0"), 0)]
        )
        self.app.engine.drain("flow")
        for correction in [{"id": "other"}, {"time": 5}, {"unknown": 1}]:
            with self.subTest(correction=correction):
                outcome = self.app.engine.repair("flow", 0, 0, correction)
                self.assertEqual(outcome["outcome"], "failed")
        self.assertEqual(len(self.app.engine.faults("flow")), 1)
        self.assertEqual(
            [row["outcome"] for row in self.app.engine.repairs("flow", 0, 0)],
            ["failed", "failed", "failed"],
        )

    def test_http_exposes_faults_and_repair(self):
        from pulseflow.service import API

        api = API(self.app)
        self.app.journal.append(
            "readings", "batch", [record(0, iso_reading("bad", 0, divisor="0"), 0)]
        )
        api.dispatch("POST", "/pipelines/flow/run", {}, {})
        faults = api.dispatch("GET", "/pipelines/flow/faults", {}, {})
        self.assertEqual(faults[0]["event_id"], "bad")
        repaired = api.dispatch(
            "POST",
            "/pipelines/flow/repair",
            {},
            {"partition": 0, "offset": 0, "correction": {"attrs.divisor": "2"}},
        )
        self.assertEqual(repaired["outcome"], "resolved")
        self.assertEqual(
            api.dispatch("GET", "/pipelines/flow/faults", {}, {}), []
        )
        self.assertEqual(
            len(api.dispatch("GET", "/pipelines/flow/repairs", {}, {})), 1
        )

    def test_event_query_reports_fault_and_held_state(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, iso_reading("bad", 0, divisor="0"), 0),
                record(1, iso_reading("queued", 1), 0),
            ],
        )
        self.app.engine.drain("flow")
        bad = self.app.queries.event("flow", "bad")
        self.assertIsNone(bad["processed"])
        self.assertEqual(bad["fault"]["event_id"], "bad")
        self.assertFalse(bad["fault"]["resolved"])
        self.assertIn("division by zero", bad["fault"]["reason"])
        queued = self.app.queries.event("flow", "queued")
        self.assertIsNone(queued["processed"])
        self.assertEqual(queued["held"]["event_id"], "queued")

    def test_held_event_blocks_window_closure(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, iso_reading("good", 0), 0),
                record(1, iso_reading("bad", 15, divisor="0"), 0),
                record(2, iso_reading("queued", 25), 0),
                record(0, iso_reading("other", 100, key="b"), 1),
            ],
        )
        self.app.engine.drain("flow")
        # A window [0,10) could close, but watermark is capped at the fault time.
        self.assertLessEqual(
            self.app.watermarks.status("flow")["watermark"], 15
        )
        self.assertEqual(
            [w for w in self.app.windows.inspect("flow") if w["status"] == "closed"],
            [],
        )
        self.app.engine.repair(
            "flow", 0, 1, {"attrs.divisor": "2"}
        )
        self.app.watermarks.declare("flow", 1, 200)
        self.app.engine.drain("flow")
        self.assertTrue(self.app.consistency.check("flow")["ok"])

    def test_replay_fork_keeps_faults_independent(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, iso_reading("bad", 0, divisor="0"), 0),
                record(0, iso_reading("ok", 0, key="b"), 1),
            ],
        )
        self.app.engine.drain("flow")
        forked = self.app.replay.fork("flow", "fork")
        self.assertEqual(forked["status"]["active_faults"], 1)
        # resimulating in an isolated pipeline preserves the same fault chain
        self.assertEqual(
            [f["event_id"] for f in self.app.engine.faults("fork")], ["bad"]
        )
        self.app.engine.repair(
            "fork", 0, 0, {"value": "2", "attrs.divisor": "2"}
        )
        self.assertEqual(self.app.engine.faults("fork"), [])
        # the original pipeline's fault is untouched
        self.assertEqual(len(self.app.engine.faults("flow")), 1)


    def test_held_offset_is_not_repairable(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, iso_reading("fault", 0, divisor="0"), 0),
                record(1, iso_reading("held", 1), 0),
            ],
        )
        self.app.engine.drain("flow")
        # only the faulting offset is a fault; the next one is merely held
        self.assertEqual(
            [(f["event_id"], f["source_offset"]) for f in self.app.engine.faults("flow")],
            [("fault", 0)],
        )
        with self.assertRaises(Missing):
            self.app.engine.repair("flow", 0, 1, {"attrs.divisor": "2"})
        resolved = self.app.engine.repair(
            "flow", 0, 0, {"attrs.divisor": "2"}
        )
        self.assertEqual(resolved["held_processed"], 1)


if __name__ == "__main__":
    unittest.main()
