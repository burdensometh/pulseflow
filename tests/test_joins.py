from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from pulseflow import PulseFlow
from pulseflow.consumer import Consumer
from pulseflow.values import Conflict, Invalid, Missing


def event(event_id, at, key="a", value="1", kind="reading"):
    return {"id": event_id, "key": key, "time": at, "value": value, "kind": kind}


def record(offset, value, partition=0):
    return {"partition": partition, "offset": offset, "event": value}


class JoinTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "join")
        self.app.journal.create("left", 1)
        self.app.journal.create("right", 1)
        self.offsets = {}

    def join(self, name="join", spec=None, buckets=4, left_parts=1, right_parts=1):
        if left_parts != 1:
            self.app.journal.create("wideleft", left_parts)
        if right_parts != 1:
            self.app.journal.create("wideright", right_parts)
        left = "left" if left_parts == 1 else "wideleft"
        right = "right" if right_parts == 1 else "wideright"
        return self.app.joins.create(
            name, left, right, spec or {"before_ms": 5, "after_ms": 10}, buckets
        )

    def append(self, channel, batch, rows, partition=0):
        key = (channel, partition)
        start = self.offsets.get(key, 0)
        self.offsets[key] = start + len(rows)
        return self.app.journal.append(
            channel, batch, [record(start + i, row, partition) for i, row in enumerate(rows)]
        )

    def pairs(self, name="join"):
        return [
            row["document"]
            for row in self.app.results.rows(name)
            if row["document"]["matched"]
        ]

    def unmatched(self, name="join"):
        return [
            row["document"]
            for row in self.app.results.rows(name)
            if not row["document"]["matched"]
        ]

    def test_interval_boundaries_and_multi_match(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "delay_ms": 10})
        self.append("left", "l1", [event("L1", 100), event("L2", 98)])
        self.append(
            "right",
            "r1",
            [
                event("R1", 95),
                event("R2", 109),
                event("R3", 110),
                event("R4", 94),
                event("R5", 100, key="b"),
            ],
        )
        self.app.engine.drain("join")
        pairs = {(p["left"]["id"], p["right"]["id"]): p for p in self.pairs()}
        self.assertEqual(set(pairs), {("L1", "R1"), ("L1", "R2"), ("L2", "R1")})
        self.assertEqual(pairs[("L1", "R1")]["delta_ms"], -5)
        self.assertEqual(pairs[("L1", "R2")]["delta_ms"], 9)
        self.assertEqual(pairs[("L1", "R1")]["kind"], "join")
        self.assertEqual(pairs[("L1", "R1")]["key"], "a")
        self.assertEqual(self.unmatched(), [])
        matches = self.app.joins.matches("join", "right:R1")
        self.assertEqual(
            {row["document"]["left"]["id"] for row in matches}, {"L1", "L2"}
        )
        self.assertEqual(self.app.engine.status("join")["pending"], 0)
        self.assertTrue(self.app.consistency.check("join")["ok"])

    def test_same_event_id_on_both_sides(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "outer": True})
        self.append("left", "l1", [event("shared", 100)])
        self.append("right", "r1", [event("shared", 103), event("shared2", 500, key="b")])
        self.app.engine.drain("join")
        pairs = self.pairs()
        self.assertEqual(
            [(p["left"]["id"], p["right"]["id"]) for p in pairs], [("shared", "shared")]
        )
        dispositions = self.app.engine.status("join")["dispositions"]
        self.assertEqual(dispositions["accepted"], 3)
        self.assertEqual(
            self.app.queries.event("join", "left:shared")["processed"]["disposition"],
            "accepted",
        )
        self.assertEqual(
            self.app.queries.event("join", "right:shared")["processed"]["disposition"],
            "accepted",
        )
        self.assertTrue(self.app.consistency.check("join")["ok"])

    def test_declare_cannot_pass_unread_join_event(self):
        self.join()
        self.append("right", "r1", [event("R1", 100)])
        with self.assertRaises(Conflict):
            self.app.joins.declare("join", "right", 0, 200)
        self.app.engine.drain("join")
        self.app.joins.declare("join", "right", 0, 200)
        self.app.engine.drain("join")
        self.assertEqual(
            self.app.joins.status("join")["sides"]["right"]["watermark"], 200
        )

    def test_replayed_ingress_does_not_duplicate_pairs(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "outer": True})
        lefts = [record(0, event("L1", 100)), record(1, event("L2", 200, key="c"))]
        rights = [record(0, event("R1", 101)), record(1, event("R2", 300, key="b"))]
        self.app.journal.append("left", "l1", lefts)
        self.app.journal.append("right", "r1", rights)
        self.app.engine.drain("join")
        before = self.app.results.rows("join")
        self.assertTrue(self.app.journal.append("left", "l1", lefts)["replayed"])
        self.assertTrue(self.app.journal.append("right", "r1", rights)["replayed"])
        self.app.engine.drain("join")
        self.assertEqual(self.app.results.rows("join"), before)
        self.assertEqual(len(self.app.results.changes("join")), 2)
        self.assertTrue(self.app.consistency.check("join")["ok"])

    def test_left_outer_waits_for_right_watermark_not_wallclock(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "outer": True})
        self.append("left", "l1", [event("L1", 100)])
        self.append("right", "r1", [event("R1", 50, key="b")])
        first = self.app.engine.drain("join")
        self.assertEqual(first["processed"], 2)
        self.assertEqual(self.app.results.rows("join"), [])
        self.assertEqual(
            [row["event_id"] for row in self.app.joins.buffers("join", "left")], ["L1"]
        )
        self.append("right", "r2", [event("R2", 109, key="b")])
        self.app.engine.drain("join")
        self.assertEqual(self.app.results.rows("join"), [])
        self.append("right", "r3", [event("R3", 110, key="b")])
        self.app.engine.drain("join")
        nulls = self.unmatched()
        self.assertEqual(len(nulls), 1)
        self.assertEqual(nulls[0]["left"]["id"], "L1")
        self.assertIsNone(nulls[0]["right"])
        self.assertIsNone(nulls[0]["delta_ms"])
        self.assertEqual(self.app.joins.buffers("join", "left"), [])
        self.assertEqual(
            {row["event_id"] for row in self.app.joins.buffers("join", "right")},
            {"R2", "R3"},
        )
        self.append("right", "r4", [event("R4", 105)])
        self.app.engine.drain("join")
        late = self.app.engine.late("join")
        self.assertEqual([row["event_id"] for row in late], ["right:R4"])
        self.assertEqual(len(self.app.results.rows("join")), 1)
        self.assertTrue(self.app.consistency.check("join")["ok"])

    def test_inner_join_drops_unmatched_silently(self):
        self.join(spec={"before_ms": 5, "after_ms": 10})
        self.append("left", "l1", [event("L1", 100)])
        self.append("right", "r1", [event("R1", 200, key="b")])
        self.app.engine.drain("join")
        self.assertEqual(self.app.results.rows("join"), [])
        self.assertEqual(self.app.joins.buffers("join", "left"), [])
        self.assertEqual(
            [row["event_id"] for row in self.app.joins.buffers("join", "right")],
            ["R1"],
        )
        run = self.app.engine.runs("join")[-1]["details"]
        self.assertEqual(run["expired_left"], 1)
        self.assertEqual(run["unmatched"], 0)

    def test_cleanup_keeps_boundary_events_that_can_still_match(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "outer": True})
        self.append("right", "r1", [event("R1", 100)])
        self.append("left", "l1", [event("L1", 105, key="b")])
        self.app.engine.drain("join")
        right_buffer = self.app.joins.buffers("join", "right")
        self.assertEqual([row["event_id"] for row in right_buffer], ["R1"])
        self.append("left", "l2", [event("L2", 105)])
        self.app.engine.drain("join")
        self.assertEqual(
            {(p["left"]["id"], p["right"]["id"]) for p in self.pairs()},
            {("L2", "R1")},
        )
        self.append("left", "l3", [event("L3", 106, key="b")])
        self.app.engine.drain("join")
        self.assertEqual(self.app.joins.buffers("join", "right"), [])
        left_buffer = self.app.joins.buffers("join", "left")
        self.assertEqual(
            {row["event_id"] for row in left_buffer}, {"L1", "L2", "L3"}
        )
        self.append("right", "r2", [event("R2", 116, key="b")])
        self.app.engine.drain("join")
        self.assertEqual(self.app.joins.buffers("join", "left"), [])
        self.assertEqual(
            {row["left"]["id"] for row in self.unmatched()}, {"L1", "L3"}
        )
        self.assertTrue(self.app.consistency.check("join")["ok"])

    def test_out_of_order_within_delay_and_late_evidence(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "delay_ms": 10, "outer": True})
        self.append(
            "right",
            "r1",
            [event("R1", 100), event("R2", 95), event("R3", 89)],
        )
        self.append("left", "l1", [event("L1", 100), event("L2", 200), event("L3", 150)])
        self.app.engine.drain("join")
        self.assertEqual(
            {(p["left"]["id"], p["right"]["id"]) for p in self.pairs()},
            {("L1", "R1"), ("L1", "R2")},
        )
        late = self.app.engine.late("join")
        self.assertEqual([row["event_id"] for row in late], ["right:R3", "left:L3"])
        self.assertEqual(late[0]["watermark"], 90)
        event_view = self.app.queries.event("join", "right:R3")
        self.assertEqual(event_view["rejection_watermark"], 90)
        self.assertEqual(event_view["processed"]["disposition"], "late")
        self.assertIsNone(event_view["buffered"])
        self.assertEqual(
            self.app.queries.event("join", "left:L1")["source"]["document"]["time"], 100
        )
        with self.assertRaises(Invalid):
            self.app.queries.event("join", "L1")
        with self.assertRaises(Missing):
            self.app.queries.event("join", "left:absent")
        self.assertEqual(self.unmatched(), [])
        self.append("right", "r2", [event("R4", 300, key="b")])
        self.app.engine.drain("join")
        self.assertEqual({row["left"]["id"] for row in self.unmatched()}, {"L2"})
        self.assertTrue(self.app.consistency.check("join")["ok"])

    def test_chunk_size_does_not_change_results(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "delay_ms": 3, "outer": True})
        self.join(
            name="trickle",
            spec={"before_ms": 5, "after_ms": 10, "delay_ms": 3, "outer": True},
        )
        lefts = [event("L" + str(i), 100 + i * 7, key="ab"[i % 2]) for i in range(6)]
        rights = [event("R" + str(i), 99 + i * 6, key="ab"[i % 2]) for i in range(6)]
        self.append("left", "l1", lefts)
        self.append("right", "r1", rights)
        self.app.engine.drain("join")
        self.app.engine.drain("trickle", limit=1)
        self.assertTrue(self.app.replay.compare("join", "trickle")["equal"])
        self.assertEqual(
            len(self.app.results.changes("join")),
            len(self.app.results.changes("trickle")),
        )

    def test_savepoint_restore_resumes_without_duplicate_pairs(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "outer": True})
        self.append("left", "l1", [event("L1", 100), event("L2", 400)])
        self.append("right", "r1", [event("R1", 99)])
        self.app.engine.drain("join")
        saved = self.app.savepoints.create("join", compressed=True)
        document = self.app.savepoints.load(saved["point_id"])
        self.assertEqual(document["channel_partitions"], {"left": 1, "right": 1})
        self.assertEqual(len(document["state"]["join_buffers"]), 2)
        self.app.savepoints.restore_point(saved["point_id"], "restored")
        self.assertTrue(self.app.consistency.check("restored")["ok"])
        self.append("left", "l2", [event("L3", 500)])
        self.append("right", "r2", [event("R2", 498), event("R3", 600, key="b")])
        self.app.engine.drain("join")
        self.app.engine.drain("restored")
        self.assertTrue(self.app.replay.compare("join", "restored")["equal"])
        self.assertEqual(self.app.replay.disposition_diff("join", "restored"), [])
        self.assertEqual(len(self.app.results.changes("join")), 3)
        self.assertEqual(len(self.app.results.changes("restored")), 3)
        pair_keys = [
            row["result_key"] for row in self.app.results.rows("join") if row["document"]["matched"]
        ]
        self.assertEqual(len(pair_keys), len(set(pair_keys)))
        self.assertEqual(
            {(p["left"]["id"], p["right"]["id"]) for p in self.pairs()},
            {("L1", "R1"), ("L3", "R2")},
        )
        self.assertEqual({row["left"]["id"] for row in self.unmatched()}, {"L2"})
        forked = self.app.replay.fork("join", "fork")
        self.assertEqual(forked["replayed"], 6)
        self.assertTrue(self.app.replay.compare("join", "fork")["equal"])
        self.assertTrue(self.app.consistency.check("fork")["ok"])

    def test_savepoint_validation_covers_join_state(self):
        self.join()
        self.append("left", "l1", [event("L1", 100)])
        self.app.engine.drain("join")
        point = self.app.savepoints.load(self.app.savepoints.create("join")["point_id"])
        import copy

        altered = copy.deepcopy(point)
        altered["state"]["join_buffers"][0][1] = 100
        with self.assertRaises(Conflict):
            self.app.savepoints.validate(altered)
        altered = copy.deepcopy(point)
        altered["state"]["join_sides"][0][1] = "other"
        with self.assertRaises(Invalid):
            self.app.savepoints.validate(altered)
        altered = copy.deepcopy(point)
        altered["channel_partitions"] = {"left": 1}
        with self.assertRaises(Invalid):
            self.app.savepoints.validate(altered)
        altered = copy.deepcopy(point)
        altered["state"]["join_progress"] = [
            row for row in altered["state"]["join_progress"] if row[0] != "right"
        ]
        with self.assertRaises(Invalid):
            self.app.savepoints.validate(altered)

    def test_consumer_and_export_read_join_changelog(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "outer": True})
        self.append("left", "l1", [event("L1", 100), event("L2", 200)])
        self.append("right", "r1", [event("R1", 101), event("R2", 300, key="b")])
        self.app.engine.drain("join")
        consumer = Consumer(self.home / "consumer.sqlite")
        consumer.catch_up(self.app, "join", limit=1)
        self.assertTrue(self.app.queries.compare_consumer("join", consumer)["equal"])
        kinds = {row["document"]["kind"] for row in consumer.rows()}
        self.assertEqual(kinds, {"join"})
        exported = self.app.exports.create("join", "jsonl")
        decoded = self.app.exports.decode(exported["export_id"])
        self.assertEqual(len(decoded), len(self.app.results.rows("join")))
        self.assertEqual(
            {row["document"]["matched"] for row in decoded}, {True, False}
        )

    def test_buffer_capacity_rolls_back_chunk(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "max_buffer_events": 2})
        self.append(
            "left",
            "l1",
            [event("L1", 100), event("L2", 200), event("L3", 300)],
        )
        with self.assertRaises(Invalid):
            self.app.engine.drain("join")
        self.assertEqual(self.app.engine.status("join")["processed_events"], 0)
        self.assertEqual(self.app.joins.buffers("join"), [])
        self.join(name="roomy", spec={"before_ms": 5, "after_ms": 10})
        self.app.engine.drain("roomy")
        self.assertEqual(self.app.engine.status("roomy")["processed_events"], 3)

    def test_plan_validation_and_pipeline_identity(self):
        with self.assertRaises(Invalid):
            self.app.joins.create("bad", "left", "left", {"before_ms": 1, "after_ms": 1})
        with self.assertRaises(Invalid):
            self.app.joins.create("bad", "left", "right", {"before_ms": 0, "after_ms": 0})
        with self.assertRaises(Invalid):
            self.app.joins.create(
                "bad", "left", "right", {"before_ms": 1, "after_ms": 1, "outer": "yes"}
            )
        with self.assertRaises(Invalid):
            self.app.joins.create(
                "bad", "left", "right", {"before_ms": 1, "after_ms": 1, "window": {}}
            )
        with self.assertRaises(Missing):
            self.app.joins.create("bad", "left", "nowhere", {"before_ms": 1, "after_ms": 1})
        self.join()
        with self.assertRaises(Invalid):
            self.app.engine.create(
                "other", "right", self.app.catalog.pipeline("join")["plan"]
            )
        self.join()
        with self.assertRaises(Conflict):
            self.join(spec={"before_ms": 6, "after_ms": 10})
        with self.assertRaises(Invalid):
            self.app.watermarks.declare("join", 0, 100)
        with self.assertRaises(Invalid):
            self.app.joins.declare("join", "middle", 0, 100)
        self.app.engine.create("windows", "left", {"window": {"kind": "tumbling", "size_ms": 10}})
        with self.assertRaises(Invalid):
            self.app.joins.declare("windows", "left", 0, 100)
        with self.assertRaises(Invalid):
            self.app.joins.status("windows")
        with self.assertRaises(Invalid):
            self.app.joins.buffers("windows")

    def test_multi_partition_sides_block_watermarks_independently(self):
        self.join(
            spec={"before_ms": 5, "after_ms": 10, "outer": True},
            left_parts=2,
            right_parts=2,
        )
        self.app.journal.append(
            "wideleft", "l1", [record(0, event("L1", 100), 0), record(1, event("L2", 105, key="b"), 0)]
        )
        self.app.journal.append(
            "wideright", "r1", [record(0, event("R1", 101), 0)]
        )
        self.app.engine.drain("join")
        status = self.app.joins.status("join")
        self.assertIsNone(status["sides"]["left"]["watermark"])
        self.assertIsNone(status["sides"]["right"]["watermark"])
        self.assertEqual(
            {(p["left"]["id"], p["right"]["id"]) for p in self.pairs()},
            {("L1", "R1")},
        )
        self.app.joins.declare("join", "left", 1, 105)
        self.app.joins.declare("join", "right", 1, 115)
        self.app.engine.drain("join")
        status = self.app.joins.status("join")
        self.assertEqual(status["sides"]["left"]["watermark"], 105)
        self.assertEqual(status["sides"]["right"]["watermark"], 101)
        self.assertEqual(status["watermark"], 101)
        self.assertEqual(status["pending_unmatched_left"], 1)
        self.assertEqual(self.unmatched(), [])
        self.app.joins.declare("join", "right", 0, 115)
        self.app.engine.drain("join")
        status = self.app.joins.status("join")
        self.assertEqual(status["sides"]["right"]["watermark"], 115)
        self.assertEqual({row["left"]["id"] for row in self.unmatched()}, {"L2"})
        lag = self.app.queries.lag("join")
        self.assertEqual(len(lag), 4)
        self.assertTrue(all(row["pending"] == 0 for row in lag))
        with self.assertRaises(Conflict):
            self.app.joins.declare("join", "left", 1, 99)
        self.assertTrue(self.app.consistency.check("join")["ok"])

    def test_unmatched_left_waits_when_right_side_idle(self):
        self.join(spec={"before_ms": 5, "after_ms": 10, "outer": True})
        self.append("left", "l1", [event("L1", 100)])
        self.app.engine.drain("join")
        self.assertEqual(self.app.results.rows("join"), [])
        self.assertEqual(self.app.joins.status("join")["pending_unmatched_left"], 1)
        self.app.joins.declare("join", "right", 0, 110)
        self.app.engine.drain("join")
        self.assertEqual({row["left"]["id"] for row in self.unmatched()}, {"L1"})
        self.assertEqual(self.app.joins.status("join")["pending_unmatched_left"], 0)

    def test_differential_against_brute_force_model(self):
        import random

        def model(records, before, after, delay, outer):
            marks = {"left": None, "right": None}
            accepted = {"left": [], "right": []}
            for side, eid, key, at in records:
                if marks[side] is not None and at < marks[side]:
                    continue
                accepted[side].append((eid, key, at))
                proposed = max(t for _, _, t in accepted[side]) - delay
                marks[side] = (
                    proposed if marks[side] is None else max(marks[side], proposed)
                )
            pairs, matched = set(), set()
            for lid, lkey, lt in accepted["left"]:
                for rid, rkey, rt in accepted["right"]:
                    if lkey == rkey and lt - before <= rt < lt + after:
                        pairs.add((lid, rid))
                        matched.add(lid)
            nulls = set()
            if outer and marks["right"] is not None:
                for lid, lkey, lt in accepted["left"]:
                    if lid not in matched and lt + after <= marks["right"]:
                        nulls.add(lid)
            return pairs, nulls

        random.seed(20261117)
        for trial in range(8):
            before = random.randint(0, 8)
            after = random.randint(1, 8)
            delay = random.randint(0, 6)
            outer = trial % 2 == 0
            records = [
                (
                    "left" if random.random() < 0.5 else "right",
                    "e" + str(trial) + "_" + str(i),
                    random.choice("ab"),
                    random.randint(0, 60),
                )
                for i in range(random.randint(1, 20))
            ]
            left_channel = "fuzzl" + str(trial)
            right_channel = "fuzzr" + str(trial)
            self.app.journal.create(left_channel, 1)
            self.app.journal.create(right_channel, 1)
            self.app.joins.create(
                "fuzz" + str(trial),
                left_channel,
                right_channel,
                {
                    "before_ms": before,
                    "after_ms": after,
                    "delay_ms": delay,
                    "outer": outer,
                },
            )
            for index, (side, eid, key, at) in enumerate(records):
                channel = left_channel if side == "left" else right_channel
                self.append(
                    channel,
                    "batch-" + str(trial) + "-" + str(index),
                    [event(eid, at, key=key)],
                )
            self.app.engine.drain("fuzz" + str(trial), limit=1)
            expected_pairs, expected_nulls = model(records, before, after, delay, outer)
            pairs = {
                (p["left"]["id"], p["right"]["id"]) for p in self.pairs("fuzz" + str(trial))
            }
            nulls = {row["left"]["id"] for row in self.unmatched("fuzz" + str(trial))}
            self.assertEqual(pairs, expected_pairs)
            self.assertEqual(nulls, expected_nulls)
            self.assertTrue(self.app.consistency.check("fuzz" + str(trial))["ok"])

    def test_join_results_flow_through_service_api(self):
        from pulseflow.service import serving
        from urllib.request import Request, urlopen

        self.join(spec={"before_ms": 5, "after_ms": 10, "outer": True})
        self.append("left", "l1", [event("L1", 100)])
        self.append("right", "r1", [event("R1", 102)])
        with serving(self.app) as base:
            def request(path, body=None):
                raw = None if body is None else json.dumps(body).encode()
                req = Request(
                    base + path, data=raw, headers={"Content-Type": "application/json"}
                )
                with urlopen(req) as response:
                    return json.load(response)

            request("/pipelines/join/run", {})
            results = request("/pipelines/join/results")
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["document"]["delta_ms"], 2)
            status = request("/pipelines/join/watermark")
            self.assertEqual(status["sides"]["left"]["watermark"], 100)
            request(
                "/pipelines/join/declare",
                {"side": "right", "partition": 0, "time": 500},
            )
            request(
                "/pipelines/join/declare",
                {"side": "left", "partition": 0, "time": 500},
            )
            request("/pipelines/join/run", {})
            self.assertEqual(request("/pipelines/join/watermark")["watermark"], 500)
            self.assertTrue(request("/pipelines/join/check")["ok"])
            self.assertEqual(len(request("/pipelines/join/lag")), 2)
            view = request("/pipelines/join/events/left:L1")
            self.assertEqual(view["processed"]["disposition"], "accepted")
            created = request(
                "/pipelines",
                {
                    "name": "httpjoin",
                    "channel": "left",
                    "plan": {
                        "join": {
                            "left_channel": "left",
                            "right_channel": "right",
                            "before_ms": 5,
                            "after_ms": 10,
                        }
                    },
                },
            )
            self.assertEqual(created["name"], "httpjoin")
            request("/pipelines/httpjoin/run", {})
            self.assertEqual(len(request("/pipelines/httpjoin/results")), 1)
