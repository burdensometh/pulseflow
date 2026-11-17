from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from pulseflow import PulseFlow
from pulseflow.consumer import Consumer
from pulseflow.service import API
from pulseflow.spec import plan_spec, window_spec
from pulseflow.values import Invalid
from test_flow import PLAN, reading, record


SESSION_PLAN = {
    "window": {"kind": "session", "gap_ms": 10},
    "metrics": ["count", "sum", "mean", "min", "max", "p50", "p95", "distinct"],
}


class SessionsTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "flow")
        self.app.journal.create("readings", 1)
        self.offset = 0

    def create(self, name="sess", plan=None):
        return self.app.engine.create(name, "readings", plan or SESSION_PLAN)

    def append(self, rows, batch="batch"):
        records = [record(self.offset + i, row) for i, row in enumerate(rows)]
        self.offset += len(rows)
        return self.app.journal.append("readings", batch, records)

    def spans(self, name):
        return [
            (row["session_start"], row["session_end"], row["group_key"], row["status"])
            for row in self.app.sessions.inspect(name)
        ]

    def test_gap_boundary_close_and_negative_times(self):
        self.create(plan={**SESSION_PLAN, "delay_ms": 1000})
        self.append(
            [
                reading("a", 0, "1"),
                reading("b", 9, "2"),
                reading("c", 19, "4"),
                reading("d", 100, "8"),
                reading("n1", -5, "1", key="b"),
                reading("n2", 3, "2", key="b"),
            ]
        )
        self.app.engine.drain("sess")
        self.assertEqual(
            self.spans("sess"),
            [
                (-5, 13, "b", "open"),
                (0, 19, "a", "open"),
                (19, 29, "a", "open"),
                (100, 110, "a", "open"),
            ],
        )
        self.assertEqual(self.app.results.rows("sess"), [])
        self.app.watermarks.declare("sess", 0, 1013)
        self.app.engine.drain("sess")
        self.assertEqual(
            self.spans("sess"),
            [
                (-5, 13, "b", "closed"),
                (0, 19, "a", "open"),
                (19, 29, "a", "open"),
                (100, 110, "a", "open"),
            ],
        )
        rows = self.app.results.rows("sess")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["document"]["kind"], "session")
        self.assertEqual(
            (rows[0]["document"]["start"], rows[0]["document"]["end"]), (-5, 13)
        )
        self.assertEqual(rows[0]["document"]["metrics"]["sum"], "3")
        self.app.watermarks.declare("sess", 0, 2000)
        self.app.engine.drain("sess")
        final = {row["document"]["start"]: row for row in self.app.results.rows("sess")}
        self.assertEqual(sorted(final), [-5, 0, 19, 100])
        first = final[0]["document"]
        self.assertEqual((first["key"], first["end"]), ("a", 19))
        self.assertEqual(first["metrics"]["count"], 2)
        self.assertEqual(first["metrics"]["sum"], "3")
        self.assertEqual(final[19]["document"]["metrics"]["count"], 1)
        self.assertEqual(final[100]["document"]["metrics"]["sum"], "8")
        self.assertEqual(len(self.app.results.changes("sess")), 4)
        self.assertTrue(self.app.consistency.check("sess")["ok"])

    def test_out_of_order_bridge_merges_open_sessions(self):
        self.create(plan={**SESSION_PLAN, "delay_ms": 1000})
        self.append(
            [
                reading("e1", 0, "1"),
                reading("e2", 25, "2"),
                reading("e3", 8, "3"),
                reading("e4", 16, "4"),
                reading("f1", 1, "5", key="b"),
            ]
        )
        self.app.engine.drain("sess")
        self.assertEqual(
            self.spans("sess"), [(0, 35, "a", "open"), (1, 11, "b", "open")]
        )
        preview = self.app.sessions.preview("sess", 0, "a")
        self.assertEqual(preview["metrics"]["count"], 4)
        self.assertEqual(
            [row["id"] for row in preview["rows"]], ["e1", "e3", "e4", "e2"]
        )
        event = self.app.queries.event("sess", "e3")
        self.assertEqual(event["sessions"][0]["session_start"], 0)
        self.assertEqual(event["windows"], [])
        lineage = self.app.queries.session_lineage("sess", 0, "a")
        self.assertEqual(len(lineage["events"]), 4)
        self.assertEqual(self.app.results.rows("sess"), [])
        self.assertTrue(self.app.consistency.check("sess")["ok"])
        self.app.watermarks.declare("sess", 0, 1035)
        self.app.engine.drain("sess")
        rows = self.app.results.rows("sess")
        self.assertEqual(len(rows), 2)
        document = [row["document"] for row in rows if row["document"]["key"] == "a"][0]
        self.assertEqual(
            (document["kind"], document["start"], document["end"]),
            ("session", 0, 35),
        )
        self.assertEqual(document["metrics"]["sum"], "10")
        self.assertTrue(self.app.consistency.check("sess")["ok"])
        api = API(self.app)
        self.assertEqual(
            len(api.dispatch("GET", "/pipelines/sess/sessions", {}, {})), 2
        )
        self.assertEqual(
            len(
                api.dispatch(
                    "GET",
                    "/pipelines/sess/sessions",
                    {"start": ["0"], "key": ["a"]},
                    {},
                )
            ),
            1,
        )

    def test_closed_session_is_not_rewritten_by_late_events(self):
        self.create()
        self.append(
            [reading("e1", 0, "1"), reading("e2", 5, "2"), reading("e3", 100, "4")]
        )
        self.app.engine.drain("sess")
        rows = self.app.results.rows("sess")
        self.assertEqual(len(rows), 1)
        original = rows[0]
        self.assertEqual(original["revision"], 1)
        self.assertEqual(original["document"]["metrics"]["sum"], "3")
        self.append([reading("e4", 10, "8")], batch="late")
        self.app.engine.drain("sess")
        late = self.app.engine.late("sess")
        self.assertEqual([row["event_id"] for row in late], ["e4"])
        self.assertEqual(late[0]["watermark"], 100)
        self.assertEqual(self.app.results.rows("sess"), rows)
        self.assertEqual(len(self.app.results.changes("sess")), 1)
        self.append([reading("e5", 105, "16")], batch="extend")
        self.app.engine.drain("sess")
        self.app.watermarks.declare("sess", 0, 200)
        self.app.engine.drain("sess")
        final = {row["document"]["start"]: row for row in self.app.results.rows("sess")}
        self.assertEqual(sorted(final), [0, 100])
        self.assertEqual(final[0], original)
        self.assertEqual(final[100]["document"]["end"], 115)
        self.assertEqual(final[100]["document"]["metrics"]["sum"], "20")
        self.assertEqual(self.app.engine.status("sess")["dispositions"]["late"], 1)
        self.assertTrue(self.app.consistency.check("sess")["ok"])

    def test_chunked_replay_and_one_shot_sessions_agree(self):
        plan = {**SESSION_PLAN, "delay_ms": 1000}
        self.app.engine.create("chunked", "readings", plan)
        self.app.engine.create("oneshot", "readings", plan)
        self.append(
            [
                reading("k", 0, "1"),
                reading("l", 30, "2"),
                reading("m", 8, "4"),
                reading("n", 50, "8"),
                reading("o", 20, "16"),
                reading("p", 42, "32"),
                reading("q", 100, "64"),
            ]
        )
        self.app.engine.drain("chunked", limit=1)
        self.app.engine.drain("oneshot", limit=1000)
        forked = self.app.replay.fork("oneshot", "fork")
        self.assertEqual(forked["replayed"], 7)
        for name in ["chunked", "oneshot", "fork"]:
            self.app.watermarks.declare(name, 0, 2000)
            self.app.engine.drain(name)
        self.assertTrue(self.app.replay.compare("chunked", "oneshot")["equal"])
        self.assertTrue(self.app.replay.compare("oneshot", "fork")["equal"])
        self.assertEqual(self.app.replay.disposition_diff("chunked", "fork"), [])
        expected = [(0, 18), (20, 30), (30, 40), (42, 60), (100, 110)]
        for name in ["chunked", "oneshot", "fork"]:
            spans = [
                (row["session_start"], row["session_end"])
                for row in self.app.sessions.inspect(name)
            ]
            self.assertEqual(spans, expected)
            self.assertTrue(self.app.consistency.check(name)["ok"])
        counts = sorted(
            row["document"]["metrics"]["count"]
            for row in self.app.results.rows("chunked")
        )
        self.assertEqual(counts, [1, 1, 1, 2, 2])

    def test_savepoint_preserves_open_sessions_and_resumes(self):
        self.create(plan={**SESSION_PLAN, "delay_ms": 1000})
        self.append(
            [reading("a", 0, "1"), reading("b", 5, "2"), reading("c", 100, "4")]
        )
        self.app.engine.drain("sess")
        saved = self.app.savepoints.create("sess", compressed=True)
        point = self.app.savepoints.load(saved["point_id"])
        self.assertEqual(len(point["state"]["session_status"]), 2)
        self.assertEqual(len(point["state"]["session_events"]), 3)
        self.app.savepoints.restore_point(saved["point_id"], "restored")
        self.assertEqual(self.spans("restored"), self.spans("sess"))
        self.append([reading("d", 12, "8"), reading("e", 108, "16")], batch="more")
        for name in ["sess", "restored"]:
            self.app.engine.drain(name)
            self.app.watermarks.declare(name, 0, 2000)
            self.app.engine.drain(name)
        self.assertEqual(
            self.spans("sess"),
            [(0, 22, "a", "closed"), (100, 118, "a", "closed")],
        )
        self.assertEqual(self.spans("restored"), self.spans("sess"))
        self.assertTrue(self.app.replay.compare("sess", "restored")["equal"])
        self.assertTrue(self.app.consistency.check("restored")["ok"])

    def test_session_results_reach_consumer_and_change_stream(self):
        self.create()
        self.append([reading("a", 0, "2"), reading("b", 5, "3")])
        self.app.engine.drain("sess")
        self.assertEqual(self.app.results.rows("sess"), [])
        self.app.watermarks.declare("sess", 0, 100)
        self.app.engine.drain("sess")
        consumer = Consumer(self.home / "consumer.sqlite")
        result = consumer.catch_up(self.app, "sess")
        self.assertEqual(result["applied"], 1)
        rows = consumer.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["document"]["kind"], "session")
        self.assertEqual(rows[0]["document"]["metrics"]["sum"], "5")
        self.assertTrue(self.app.queries.compare_consumer("sess", consumer)["equal"])
        self.app.rollups.register(
            "region",
            "sess",
            {"groups": {}, "metrics": {"total": {"op": "sum", "metric": "sum"}}},
        )
        self.app.rollups.catch_up("region")
        self.assertEqual(self.app.rollups.rows("region"), [])

    def test_session_capacity_and_spec_validation(self):
        self.create(
            plan={
                "window": {"kind": "session", "gap_ms": 10},
                "max_window_events": 3,
                "delay_ms": 1000,
            }
        )
        self.append([reading("m", 0), reading("n", 5), reading("o", 8)])
        self.app.engine.drain("sess")
        self.append([reading("p", 12)], batch="overflow")
        with self.assertRaises(Invalid):
            self.app.engine.drain("sess")
        self.assertEqual(self.app.engine.status("sess")["processed_events"], 3)
        self.assertEqual(len(self.app.sessions.preview("sess", 0, "a")["rows"]), 3)
        self.assertTrue(self.app.consistency.check("sess")["ok"])
        self.assertEqual(
            window_spec({"kind": "session", "gap_ms": 10}),
            {"kind": "session", "gap_ms": 10},
        )
        for bad in [
            {"kind": "session"},
            {"kind": "session", "gap_ms": 0},
            {"kind": "session", "gap_ms": 86400001},
            {"kind": "session", "gap_ms": "10"},
            {"kind": "session", "gap_ms": 10, "size_ms": 5},
            {"kind": "session", "size_ms": 10},
        ]:
            with self.subTest(bad=bad), self.assertRaises(Invalid):
                window_spec(bad)
        plan = plan_spec({"window": {"kind": "session", "gap_ms": 5}})
        self.assertEqual(plan["window"], {"kind": "session", "gap_ms": 5})
        self.assertEqual(plan["metrics"], ["count", "sum", "mean"])

    def test_fixed_windows_remain_available_alongside_sessions(self):
        self.app.engine.create("fixed", "readings", PLAN)
        self.create()
        self.append(
            [reading("a", 0, "1"), reading("b", 5, "2"), reading("c", 12, "4")]
        )
        self.app.engine.drain("fixed")
        self.app.engine.drain("sess")
        self.assertEqual(len(self.app.windows.inspect("fixed")), 2)
        self.assertEqual(self.spans("sess"), [(0, 22, "a", "open")])
        self.assertTrue(self.app.consistency.check("fixed")["ok"])
        self.assertTrue(self.app.consistency.check("sess")["ok"])
        self.app.watermarks.declare("fixed", 0, 100)
        self.app.watermarks.declare("sess", 0, 100)
        self.app.engine.drain("fixed")
        self.app.engine.drain("sess")
        fixed_kinds = {
            row["document"]["kind"] for row in self.app.results.rows("fixed")
        }
        self.assertEqual(fixed_kinds, {"window"})
        session_rows = self.app.results.rows("sess")
        self.assertEqual({row["document"]["kind"] for row in session_rows}, {"session"})
        self.assertEqual(session_rows[0]["document"]["metrics"]["sum"], "7")
