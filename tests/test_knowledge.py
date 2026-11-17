from pathlib import Path
from tempfile import TemporaryDirectory
import copy
import json
import unittest
from urllib.request import Request, urlopen

from pulseflow import PulseFlow
from pulseflow.service import serving
from pulseflow.values import Conflict, Invalid
from test_flow import PLAN, reading, record


DIMENSION_MAPPING = {
    "value": {
        "op": "mul",
        "args": [{"field": "value"}, {"field": "attrs.dimension.factor"}],
    }
}


def version(factor, valid_from=0, revision=1, key="a", deleted=False):
    return {
        "dimension": "stations",
        "key": key,
        "valid_from": valid_from,
        "revision": revision,
        "attrs": {"factor": factor},
        "deleted": deleted,
    }


class KnowledgeCutoffTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "flow")
        self.app.journal.create("readings", 1)
        self.app.dimensions.publish("base", [version("2")])

    def append(self, rows, batch):
        positions = self.app.journal.positions("readings")["0"]
        self.app.journal.append(
            "readings",
            batch,
            [record(positions + i, row) for i, row in enumerate(rows)],
        )

    def knowledge_plan(self, cutoff):
        plan = copy.deepcopy(PLAN)
        plan.update(dimension={"name": "stations", "knowledge": cutoff})
        plan["mapping"] = DIMENSION_MAPPING
        return plan

    def realtime_plan(self):
        plan = copy.deepcopy(PLAN)
        plan.update(dimension={"name": "stations"})
        plan["mapping"] = DIMENSION_MAPPING
        return plan

    def values(self, pipeline):
        with self.app.catalog.transaction() as connection:
            rows = connection.execute(
                "SELECT event_id,transformed,dimension_version FROM processed_events "
                "WHERE pipeline=? ORDER BY source_sequence",
                (pipeline,),
            )
            return {
                row["event_id"]: [
                    json.loads(row["transformed"])["value"],
                    row["dimension_version"],
                ]
                for row in rows
                if row["transformed"] is not None
            }

    def test_chunked_realtime_join_sees_midstream_revision_but_knowledge_join_does_not(
        self,
    ):
        self.append([reading("one", 0)], "first")
        self.app.engine.create("live", "readings", self.realtime_plan())
        self.app.engine.create("pinned", "readings", self.knowledge_plan("base"))
        self.app.engine.drain("live")
        self.app.engine.drain("pinned")
        self.app.dimensions.publish("correction", [version("9", revision=2)])
        # The live join rederives the same valid_from version from current
        # knowledge, while the pinned pipeline keeps the base version.
        self.append([reading("two", 10)], "second")
        self.app.engine.drain("live")
        self.app.engine.drain("pinned")
        self.assertEqual(
            self.values("live"), {"one": ["2", 1], "two": ["9", 2]}
        )
        self.assertEqual(
            self.values("pinned"), {"one": ["2", 1], "two": ["2", 1]}
        )
        self.assertEqual(
            self.app.catalog.pipeline("pinned")["plan"]["dimension"]["knowledge"],
            {"version": 1},
        )

    def test_same_readings_plan_and_cutoff_repeat_exactly(self):
        self.append([reading("one", 0), reading("two", 10)], "batch")
        self.app.dimensions.publish("correction", [version("9", revision=2)])
        self.app.engine.create("left", "readings", self.knowledge_plan("base"))
        self.app.engine.create("right", "readings", self.knowledge_plan("base"))
        self.app.engine.drain("left")
        self.app.engine.drain("right")
        self.assertEqual(
            self.values("left"), {"one": ["2", 1], "two": ["2", 1]}
        )
        self.assertTrue(self.app.replay.compare("left", "right")["equal"])
        # Replaying the same pinned plan after more revisions still matches.
        self.app.dimensions.publish("later", [version("7", valid_from=20, revision=1)])
        self.app.replay.fork("left", "again")
        self.assertTrue(self.app.replay.compare("left", "again")["equal"])
        self.assertEqual(
            self.app.catalog.pipeline("again")["plan"]["dimension"]["knowledge"],
            {"version": 1},
        )

    def test_new_cutoff_builds_a_separate_replay_for_comparison(self):
        self.append([reading("one", 0), reading("two", 10)], "batch")
        self.app.engine.create("base_flow", "readings", self.knowledge_plan("base"))
        self.app.engine.drain("base_flow")
        self.app.dimensions.publish("correction", [version("9", revision=2)])
        result = self.app.replay.fork("base_flow", "newer", knowledge="correction")
        self.assertGreater(result["replayed"], 0)
        self.assertFalse(self.app.replay.compare("base_flow", "newer")["equal"])
        self.assertEqual(
            self.values("newer"), {"one": ["9", 2], "two": ["9", 2]}
        )
        self.assertEqual(
            self.app.catalog.pipeline("newer")["plan"]["dimension"]["knowledge"],
            {"version": 2},
        )
        # An identical cutpoint on another fork is reproducible.
        self.app.replay.fork("base_flow", "newer_copy", knowledge=2)
        self.assertTrue(self.app.replay.compare("newer", "newer_copy")["equal"])

    def test_cutoff_respects_deletion_and_same_valid_from_priority(self):
        self.app.dimensions.publish(
            "second_same_start", [version("5", revision=2)]
        )
        self.assertEqual(
            self.app.dimensions.lookup("stations", "a", 0, cutoff=2)["document"],
            {"factor": "5"},
        )
        self.assertEqual(
            self.app.dimensions.lookup("stations", "a", 0, cutoff=1)["document"],
            {"factor": "2"},
        )
        self.app.dimensions.publish(
            "remove", [version("5", revision=3, deleted=True)]
        )
        self.assertIsNone(self.app.dimensions.lookup("stations", "a", 0))
        self.assertIsNone(self.app.dimensions.lookup("stations", "a", 0, cutoff=3))
        self.assertEqual(
            self.app.dimensions.lookup("stations", "a", 0, cutoff=2)["document"],
            {"factor": "5"},
        )
        # A deletion hidden by the cutoff still resolves the older version.
        self.app.engine.create("pinned", "readings", self.knowledge_plan(2))
        self.append([reading("one", 0)], "batch")
        self.app.engine.drain("pinned")
        self.assertEqual(self.values("pinned"), {"one": ["5", 2]})
        with self.assertRaises(Invalid):
            self.app.dimensions.lookup("stations", "a", 0, cutoff=99)
        with self.assertRaises(Invalid):
            self.app.dimensions.lookup("stations", "a", 0, cutoff={"batch": "absent"})

    def test_head_cutoff_pins_creation_time_and_repeat_create_reuses_it(self):
        plan = copy.deepcopy(PLAN)
        plan["dimension"] = {"name": "stations", "mode": "knowledge"}
        plan["mapping"] = DIMENSION_MAPPING
        first = self.app.engine.create("auto", "readings", plan)
        self.assertEqual(first["plan"]["dimension"]["knowledge"], {"version": 1})
        self.app.dimensions.publish("correction", [version("9", revision=2)])
        again = self.app.engine.create("auto", "readings", plan)
        self.assertEqual(again["plan_hash"], first["plan_hash"])
        self.assertEqual(again["plan"]["dimension"]["knowledge"], {"version": 1})
        self.append([reading("one", 0)], "batch")
        self.app.engine.drain("auto")
        self.assertEqual(self.values("auto"), {"one": ["2", 1]})

    def test_explicit_revision_across_chunks_stays_pinned(self):
        self.append([reading("one", 0)], "first")
        self.app.engine.create("pinned", "readings", self.knowledge_plan("base"))
        self.app.engine.drain("pinned", limit=1)
        self.app.dimensions.publish("correction", [version("9", revision=2)])
        self.append([reading("two", 10), reading("three", 20)], "second")
        self.app.engine.drain("pinned", limit=1)
        self.app.engine.drain("pinned", limit=1)
        self.assertEqual(
            self.values("pinned"),
            {"one": ["2", 1], "two": ["2", 1], "three": ["2", 1]},
        )
        self.assertTrue(self.app.consistency.check("pinned")["ok"])

    def test_named_batch_cutoff_stays_stable_after_later_publish(self):
        self.assertEqual(self.app.dimensions.cutoff("base"), 1)
        self.app.dimensions.publish("correction", [version("9", revision=2)])
        self.assertEqual(self.app.dimensions.cutoff("base"), 1)
        self.assertEqual(self.app.dimensions.cutoff(), 2)
        # Creating a batch-pinned pipeline after the correction still pins
        # the knowledge that existed when the named batch committed.
        self.append([reading("one", 0)], "batch")
        self.app.engine.create("late", "readings", self.knowledge_plan("base"))
        self.app.engine.drain("late")
        self.assertEqual(self.values("late"), {"one": ["2", 1]})

    def test_call_arguments_and_alias_spellings(self):
        self.append([reading("one", 0)], "batch")
        plan = copy.deepcopy(PLAN)
        plan["dimension"] = {"name": "stations"}
        plan["mapping"] = DIMENSION_MAPPING
        created = self.app.engine.create("call_cutoff", "readings", plan, knowledge=1)
        self.assertEqual(
            created["plan"]["dimension"]["knowledge"], {"version": 1}
        )
        alias = copy.deepcopy(PLAN)
        alias["dimension"] = {"name": "stations", "knowledge_version": 1}
        alias["mapping"] = DIMENSION_MAPPING
        self.app.engine.create("alias_cutoff", "readings", alias)
        top_level = copy.deepcopy(PLAN)
        top_level["dimension"] = {"name": "stations"}
        top_level["knowledge"] = {"batch": "base"}
        top_level["mapping"] = DIMENSION_MAPPING
        self.app.engine.create("top_level", "readings", top_level)
        for name in ["call_cutoff", "alias_cutoff", "top_level"]:
            self.assertEqual(
                self.app.catalog.pipeline(name)["plan"]["dimension"]["knowledge"],
                {"version": 1},
            )
        with self.assertRaises(Invalid):
            self.app.engine.create(
                "double",
                "readings",
                {
                    **plan,
                    "dimension": {
                        "name": "stations",
                        "knowledge": 1,
                        "knowledge_version": 1,
                    },
                },
            )

    def test_replay_can_explicitly_fall_back_to_realtime(self):
        self.append([reading("one", 0)], "batch")
        self.app.engine.create("pinned", "readings", self.knowledge_plan("base"))
        self.app.engine.drain("pinned")
        self.app.dimensions.publish("correction", [version("9", revision=2)])
        self.app.replay.fork("pinned", "live", mode="realtime")
        self.assertNotIn(
            "knowledge", self.app.catalog.pipeline("live")["plan"]["dimension"]
        )
        self.assertEqual(
            self.app.queries.dimension_impact("live", "stations", "a", 0)["events"],
            [{"event_id": "one", "event_time": 0, "dimension_version": 2}],
        )
        self.assertEqual(
            self.app.queries.dimension_impact("pinned", "stations", "a", 0)["events"],
            [],
        )

    def test_savepoint_restore_keeps_the_original_cutoff(self):
        self.append([reading("one", 0)], "first")
        self.app.engine.create("pinned", "readings", self.knowledge_plan("base"))
        self.app.engine.drain("pinned")
        point = self.app.savepoints.create("pinned")
        self.app.dimensions.publish("correction", [version("9", revision=2)])
        self.append([reading("two", 10)], "second")
        self.app.savepoints.restore_point(point["point_id"], "resumed")
        self.app.engine.drain("pinned")
        self.app.engine.drain("resumed")
        self.assertEqual(
            self.values("pinned"), {"one": ["2", 1], "two": ["2", 1]}
        )
        self.assertEqual(
            self.values("resumed"), {"one": ["2", 1], "two": ["2", 1]}
        )
        self.assertTrue(self.app.replay.compare("pinned", "resumed")["equal"])
        self.assertEqual(
            self.app.catalog.pipeline("resumed")["plan"]["dimension"]["knowledge"],
            {"version": 1},
        )

    def test_plan_validation_for_knowledge_mode(self):
        plan = copy.deepcopy(PLAN)
        plan["dimension"] = {"name": "stations", "mode": "knowledge"}
        # A knowledge mode without an explicit cutoff pins the current head.
        created = self.app.engine.create("auto_head", "readings", plan)
        self.assertEqual(
            created["plan"]["dimension"]["knowledge"]["version"],
            self.app.dimensions.head(),
        )
        plan["dimension"] = {
            "name": "stations",
            "mode": "realtime",
            "knowledge": 1,
        }
        with self.assertRaises(Invalid):
            self.app.engine.create("bad_mode", "readings", plan)
        plan["dimension"] = {"name": "stations", "knowledge": {"version": 1}}
        with self.assertRaises(Invalid):
            self.app.engine.create("no_dimension", "readings", {**PLAN, "knowledge": 1})
        with self.assertRaises(Invalid):
            self.app.engine.create(
                "too_high", "readings", self.knowledge_plan({"version": 99})
            )
        with self.assertRaises(Invalid):
            self.app.engine.create(
                "unknown_batch", "readings", self.knowledge_plan({"batch": "absent"})
            )
        self.app.engine.create("ok", "readings", self.knowledge_plan({"batch": "base"}))
        self.assertEqual(
            self.app.catalog.pipeline("ok")["plan"]["dimension"]["knowledge"],
            {"version": 1},
        )

    def test_http_lookup_and_pipeline_creation_accept_cutoffs(self):
        with serving(self.app) as base:

            def request(path, body=None):
                raw = None if body is None else json.dumps(body).encode()
                req = Request(
                    base + path, data=raw, headers={"Content-Type": "application/json"}
                )
                with urlopen(req) as response:
                    return json.load(response)

            self.app.dimensions.publish("correction", [version("9", revision=2)])
            self.assertEqual(
                request("/dimensions/stations/a?at=0&knowledge=1")["document"],
                {"factor": "2"},
            )
            self.assertEqual(
                request("/dimensions/stations/a?at=0&knowledge=base")["document"],
                {"factor": "2"},
            )
            self.assertEqual(
                request("/dimensions/stations/a?at=0")["document"], {"factor": "9"}
            )
            self.append([reading("one", 0)], "batch")
            request("/channels", {"name": "http", "partitions": 1})
            request(
                "/ingress/http",
                {"batch_id": "b", "records": [record(0, reading("h1", 0))]},
            )
            plan = self.knowledge_plan({"batch": "base"})
            request("/pipelines", {"name": "pinned", "channel": "http", "plan": plan})
            request("/pipelines/pinned/run", {})
            self.assertEqual(
                request("/pipelines/pinned")["plan"]["dimension"]["knowledge"],
                {"version": 1},
            )
            request(
                "/replay",
                {"source": "pinned", "destination": "forked", "knowledge": "correction"},
            )
            self.assertEqual(
                request("/pipelines/forked")["plan"]["dimension"]["knowledge"],
                {"version": 2},
            )
