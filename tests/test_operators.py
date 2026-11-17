from pathlib import Path
from tempfile import TemporaryDirectory
import copy
import json
import unittest

from pulseflow import PulseFlow
from pulseflow.operators import Operator, OperatorRegistry
from pulseflow.values import Conflict, Invalid, canonical
from test_flow import PLAN, reading, record


class KindCounter(Operator):
    """A stateful operator defined entirely outside the engine."""

    name = "kindcounter"
    event_order = 35
    watermark_order = 40
    plan_key = "kind_count"
    state_tables = {"kind_counts": ["group_key", "kind", "count"]}
    ddl = (
        """
        CREATE TABLE IF NOT EXISTS kind_counts (
         pipeline TEXT NOT NULL REFERENCES pipelines(name),
         group_key TEXT NOT NULL,
         kind TEXT NOT NULL,
         count INTEGER NOT NULL,
         PRIMARY KEY(pipeline,group_key,kind)
        )
        """,
    )

    @staticmethod
    def plan_validator(value):
        if value != {"enabled": True}:
            raise Invalid("kind_count must be {'enabled': true}")
        return {"enabled": True}

    def applies(self, plan):
        return "kind_count" in plan

    def process(self, connection, pipeline, row):
        connection.execute(
            "INSERT INTO kind_counts VALUES (?,?,?,1) "
            "ON CONFLICT(pipeline,group_key,kind) DO UPDATE SET count=count+1",
            (pipeline["name"], row["key"], row["kind"]),
        )
        return None

    def watermark(self, connection, pipeline):
        return 0

    def check(self, connection, pipeline, problems):
        return None


class DroppingOperator(Operator):
    name = "drop_kind"
    event_order = 15
    plan_key = "drop"
    ddl = ()

    @staticmethod
    def plan_validator(value):
        if not isinstance(value, str) or not value:
            raise Invalid("drop requires an event kind")
        return value

    def applies(self, plan):
        return "drop" in plan

    def process(self, connection, pipeline, row):
        return "filtered" if row["kind"] == pipeline["plan"]["drop"] else None


class OperatorLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "ops")
        self.app.register_operator(KindCounter())
        self.app.journal.create("readings", 1)

    def append(self, rows):
        start = len(self.app.journal.read("readings", partition=0))
        return self.app.journal.append(
            "readings",
            "batch-" + str(start),
            [record(start + i, row) for i, row in enumerate(rows)],
        )

    def counts(self, pipeline):
        with self.app.catalog.transaction() as connection:
            return [
                tuple(row)
                for row in connection.execute(
                    "SELECT group_key,kind,count FROM kind_counts "
                    "WHERE pipeline=? ORDER BY group_key,kind",
                    (pipeline,),
                )
            ]

    def test_operator_processes_events_tracks_watermark_and_restores(self):
        plan = copy.deepcopy(PLAN)
        plan["kind_count"] = {"enabled": True}
        self.app.engine.create("flow", "readings", plan)
        self.append(
            [
                reading("one", 0, kind="x"),
                reading("two", 1, kind="y"),
                reading("three", 2, kind="x", key="b"),
            ]
        )
        self.app.engine.drain("flow")
        self.assertEqual(
            self.counts("flow"), [("a", "x", 1), ("a", "y", 1), ("b", "x", 1)]
        )
        saved = self.app.savepoints.create("flow")
        point = self.app.savepoints.load(saved["point_id"])
        self.assertEqual(
            point["state"]["kind_counts"],
            [["a", "x", 1], ["a", "y", 1], ["b", "x", 1]],
        )
        self.app.savepoints.restore_point(saved["point_id"], "restored")
        self.assertEqual(self.counts("restored"), self.counts("flow"))
        self.assertTrue(self.app.consistency.check("restored")["ok"])
        self.append([reading("four", 3, kind="x")])
        self.app.engine.drain("flow")
        self.app.engine.drain("restored")
        self.assertEqual(self.counts("restored"), self.counts("flow"))
        self.assertEqual(self.counts("flow")[0], ("a", "x", 2))

    def test_operator_disposition_stops_the_chain(self):
        self.app.register_operator(DroppingOperator())
        plan = copy.deepcopy(PLAN)
        plan["kind_count"] = {"enabled": True}
        plan["drop"] = "skip"
        self.app.engine.create("flow", "readings", plan)
        self.append([reading("keep", 0, kind="x"), reading("gone", 1, kind="skip")])
        self.app.engine.drain("flow")
        self.assertEqual(
            self.app.engine.status("flow")["dispositions"],
            {"accepted": 1, "filtered": 1},
        )
        # The dropping operator runs first, so the counter never sees "skip".
        self.assertEqual(self.counts("flow"), [("a", "x", 1)])

    def test_registration_is_closed_after_pipeline_creation(self):
        self.app.engine.create("flow", "readings", copy.deepcopy(PLAN))
        with self.assertRaises(Conflict):
            self.app.register_operator(DroppingOperator())

    def test_registry_rejects_conflicting_declarations(self):
        registry = OperatorRegistry([KindCounter()])
        duplicate = KindCounter()
        with self.assertRaises(Invalid):
            registry.register(duplicate)

        class BadKeys(KindCounter):
            name = "badkeys"
            state_tables = {"kind_counts": ["group_key", "kind", "count", "extra"]}

        with self.assertRaises(Invalid):
            registry.register(BadKeys())

        class BadSql(KindCounter):
            name = "badsql"
            state_tables = {"kind; drop table": ["group_key"]}

        with self.assertRaises(Invalid):
            registry.register(BadSql())

    def test_savepoint_rejects_state_without_an_owner(self):
        plan = copy.deepcopy(PLAN)
        plan["kind_count"] = {"enabled": True}
        self.app.engine.create("flow", "readings", plan)
        self.append([reading("one", 0, kind="x")])
        self.app.engine.drain("flow")
        point = self.app.savepoints.load(
            self.app.savepoints.create("flow")["point_id"]
        )

        # A section that no registered operator declares.
        altered = copy.deepcopy(point)
        altered["state"]["orphaned"] = []
        with self.assertRaises(Invalid):
            self.app.savepoints.validate(altered)

        # A v2 document that names an operator this catalog does not have.
        altered = copy.deepcopy(point)
        altered["format_version"] = 2
        altered["state"] = {
            table: rows
            for table, rows in point["state"].items()
            if table in {
                "progress",
                "late_events",
                "processed_events",
                "results",
                "changes",
            }
        }
        altered["operators"] = {"ghost": {"schema_version": 1, "state": {}}}
        with self.assertRaises(Invalid):
            self.app.savepoints.validate(altered)

        # A version the catalog cannot interpret.
        altered = copy.deepcopy(point)
        altered["format_version"] = 3
        with self.assertRaises(Invalid):
            self.app.savepoints.validate(altered)


class LegacySavepointTest(unittest.TestCase):
    """A v1 document written before operators declared their own state."""

    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)

    def legacy_document(self):
        plan = copy.deepcopy(PLAN)
        plan.update(
            dedup={"field": "attrs.token", "ttl_ms": 10},
            sequence={"first_kind": "A", "then_kind": "B", "within_ms": 10},
        )
        return {
            "format_version": 1,
            "point_id": "legacy",
            "pipeline": {
                "name": "flow",
                "channel": "readings",
                "plan": plan,
                "plan_hash": None,
                "buckets": 4,
                "hash_version": 1,
                "watermark": 10,
                "last_sequence": 2,
                "generation": 1,
            },
            "channel_partitions": 1,
            "source_prefix_sha256": "0" * 64,
            "state": {
                "progress": [[0, 2, 10, None]],
                "window_events": [],
                "window_status": [],
                "keyed_state": [],
                "seen_keys": [],
                "late_events": [],
                "processed_events": [],
                "results": [],
                "changes": [],
            },
        }

    def test_legacy_layout_is_still_accepted(self):
        from pulseflow.catalog import Catalog
        from pulseflow.state import KeyedState, Leases
        from pulseflow.results import Results
        from pulseflow.windows import Windows
        from pulseflow.dimensions import Dimensions
        from pulseflow.operators import (
            DedupOperator,
            LatestOperator,
            SequenceOperator,
            WindowOperator,
        )
        from pulseflow.artifacts import Artifacts
        from pulseflow.journal import Journal
        from pulseflow.savepoints import Savepoints, LEGACY_STATE_TABLES
        from pulseflow.values import fingerprint
        from pulseflow.spec import plan_spec

        catalog = Catalog(self.home / "legacy")
        results = Results(catalog)
        state = KeyedState(catalog)
        windows = Windows(catalog, results)
        registry = OperatorRegistry(
            [
                WindowOperator(windows),
                DedupOperator(),
                LatestOperator(state),
                SequenceOperator(state, results),
            ]
        )
        savepoints = Savepoints(
            catalog, Journal(catalog), Artifacts(self.home / "artifacts"), registry
        )
        document = self.legacy_document()
        document["pipeline"]["plan_hash"] = fingerprint(
            plan_spec(document["pipeline"]["plan"])
        )
        self.assertEqual(
            set(document["state"]), set(LEGACY_STATE_TABLES)
        )
        savepoints.validate(document)

        # Dropping a section is still a layout error.
        broken = copy.deepcopy(document)
        del broken["state"]["seen_keys"]
        with self.assertRaises(Invalid):
            savepoints.validate(broken)


if __name__ == "__main__":
    unittest.main()
