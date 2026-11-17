from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import unittest

from pulseflow import PulseFlow
from pulseflow.series import integral, ordered_samples, resample, value_at
from pulseflow.sqlite_source import column, definition
from pulseflow.values import Conflict, Invalid, Missing
from test_flow import PLAN, reading, record


class SourcesSeriesTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "flow")
        self.app.journal.create("readings", 1)
        self.app.engine.create("flow", "readings", PLAN)

    def test_sql_source_atomic_progress_and_frozen_prefix(self):
        path = self.home / "source.sqlite"
        with sqlite3.connect(path) as source:
            source.execute(
                "CREATE TABLE samples(seq INTEGER PRIMARY KEY, station TEXT, observed INTEGER, reading TEXT, kind TEXT, region TEXT)"
            )
            source.executemany(
                "INSERT INTO samples VALUES (?,?,?,?,?,?)",
                [
                    (0, "a", 0, "2.5", "reading", "north"),
                    (4, "a", 10, "3", "reading", "north"),
                ],
            )
        spec = {
            "table": "samples",
            "sequence": "seq",
            "key": "station",
            "time": "observed",
            "value": "reading",
            "kind": "kind",
            "attrs": {"region": "region"},
        }
        self.app.sqlite.register("source", "readings", 0, spec)
        self.app.sqlite.register("source", "readings", 0, spec)
        self.assertEqual(self.app.sqlite.poll("source", path, 1)["cursor"], 0)
        self.assertEqual(self.app.sqlite.poll("source", path, 1)["cursor"], 4)
        self.assertEqual(self.app.sqlite.poll("source", path)["rows"], 0)
        self.assertEqual(self.app.sqlite.status("source")["imported_rows"], 2)
        self.assertEqual(len(self.app.sqlite.runs("source")), 3)
        self.app.engine.drain("flow")
        self.assertEqual(
            self.app.results.rows("flow")[0]["document"]["metrics"]["sum"], "2.5"
        )
        self.assertEqual(
            self.app.journal.read("readings")[0]["document"]["attrs"]["region"], "north"
        )
        with sqlite3.connect(path) as source:
            source.execute("UPDATE samples SET reading='99' WHERE seq=0")
        with self.assertRaises(Conflict):
            self.app.sqlite.poll("source", path)
        self.assertEqual(self.app.sqlite.status("source")["source_cursor"], 4)
        with self.assertRaises(Conflict):
            self.app.sqlite.register(
                "source", "readings", 0, {**spec, "value": "region"}
            )
        with self.assertRaises(Missing):
            self.app.sqlite.status("absent")
        with self.assertRaises(Invalid):
            self.app.sqlite.poll("source", self.app.catalog.path)
        with self.assertRaises(Invalid):
            definition({})
        with self.assertRaises(Invalid):
            column("a.b")

    def test_sql_source_failure_does_not_advance_either_database_cursor(self):
        path = self.home / "bad.sqlite"
        with sqlite3.connect(path) as source:
            source.execute(
                "CREATE TABLE samples(seq INTEGER, station TEXT, observed INTEGER, reading TEXT)"
            )
            source.executemany(
                "INSERT INTO samples VALUES (?,?,?,?)",
                [(0, "a", 0, "2"), (1, "a", 10, "bad")],
            )
        spec = {
            "table": "samples",
            "sequence": "seq",
            "key": "station",
            "time": "observed",
            "value": "reading",
        }
        self.app.sqlite.register("bad", "readings", 0, spec)
        with self.assertRaises(Invalid):
            self.app.sqlite.poll("bad", path)
        self.assertEqual(self.app.sqlite.status("bad")["source_cursor"], -1)
        self.assertEqual(self.app.journal.positions("readings"), {"0": 0})
        with sqlite3.connect(path) as source:
            source.execute("UPDATE samples SET reading='3' WHERE seq=1")
        self.assertEqual(self.app.sqlite.poll("bad", path)["rows"], 2)
        with sqlite3.connect(path) as source:
            source.execute("DELETE FROM samples")
        with self.assertRaises(Conflict):
            self.app.sqlite.poll("bad", path)
        with self.assertRaises(Conflict):
            self.app.sqlite.normalize(
                "bad",
                spec,
                [
                    {"seq": 0, "station": "a", "observed": 0, "reading": "1"},
                    {"seq": 0, "station": "a", "observed": 1, "reading": "2"},
                ],
            )

    def test_series_grid_tie_break_gap_and_exact_integrals(self):
        rows = [
            reading("a", 0, "2"),
            reading("b", 10, "4"),
            reading("c", 10, "6"),
            reading("null", 20, None),
        ]
        samples = ordered_samples(rows)
        self.assertEqual(str(value_at(samples, 5, "linear")), "4.0")
        self.assertEqual(
            resample(rows, -5, 20, 5),
            [
                {"time": -5, "value": None},
                {"time": 0, "value": "2"},
                {"time": 5, "value": "2"},
                {"time": 10, "value": "6"},
                {"time": 15, "value": "6"},
            ],
        )
        self.assertEqual(integral(rows, 0, 20)["area_value_ms"], "80")
        self.assertEqual(integral(rows, 0, 10, "linear")["time_weighted_mean"], "4")
        self.assertEqual(integral(rows, -10, 0)["covered_ms"], 0)
        self.assertEqual(integral(rows, 0, 20, max_gap=3)["covered_ms"], 6)
        self.assertIsNone(value_at(samples, 5, "step", 3))
        self.assertIsNone(value_at(samples, 5, "linear", 3))
        self.assertIsNone(value_at(samples, 15, "linear"))
        with self.assertRaises(Invalid):
            ordered_samples([reading("a", 0), reading("b", 1, key="other")])
        with self.assertRaises(Invalid):
            value_at(samples, 0, "unknown")
        with self.assertRaises(Invalid):
            resample(rows, 20, 0, 1)
        with self.assertRaises(Invalid):
            integral(rows, 0, 0)
        self.app.journal.append(
            "readings", "series", [record(i, row) for i, row in enumerate(rows)]
        )
        self.app.engine.drain("flow")
        self.assertEqual(
            self.app.queries.series("flow", "a", 0, 20, 5)["integral"]["area_value_ms"],
            "80",
        )
