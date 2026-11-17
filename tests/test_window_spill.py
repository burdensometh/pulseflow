from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest

from pulseflow import PulseFlow
from pulseflow.statistics import summarize
from pulseflow.windows import Windows


METRICS = ["count", "count_value", "sum", "mean", "min", "max", "p50", "p95", "distinct"]


def reading(event_id, at, value=None, key="a"):
    return {"id": event_id, "key": key, "time": at, "value": value}


class WindowSpillTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "spill")
        self.app.journal.create("readings", 1)

    def create(self, name="flow", budget=256, window=None):
        self.app.engine.create(
            name,
            "readings",
            {
                "window": window or {"kind": "tumbling", "size_ms": 100000},
                "metrics": METRICS,
                "max_window_events": budget,
            },
        )

    def append(self, rows, batch="batch"):
        return self.app.journal.append(
            "readings",
            batch,
            [{"partition": 0, "offset": i, "event": row} for i, row in enumerate(rows)],
        )

    def reference(self, pipeline, key, start, end):
        rows = [
            row
            for row in self.app.queries.accepted(pipeline, key)
            if start <= row["time"] < end
        ]
        rows.sort(key=lambda row: (row["time"], row["id"]))
        return summarize(rows, METRICS)

    def results_by_key(self, pipeline):
        return {
            row["document"]["key"]: row["document"]
            for row in self.app.results.rows(pipeline)
        }

    def test_large_window_spills_and_stays_exact(self):
        self.create(budget=256)
        rows = []
        for i in range(1000):
            if i % 17 == 0:
                value = None
            elif i % 5 == 0:
                value = ["1.0", "1.00", "1"][i % 3]
            else:
                value = f"{(i % 199) - 99}.{i % 10:03d}"
            rows.append(reading(f"e{i}", i, value, ["a", "b", "c"][i % 3]))
        self.append(rows)
        chunks = []
        stream = Windows.value_chunks

        def spy(self_, *args):
            for chunk in stream(self_, *args):
                chunks.append(len(chunk))
                yield chunk

        blocked = patch.object(
            Windows, "rows_in", side_effect=AssertionError("full window loaded")
        )
        with patch.object(Windows, "value_chunks", spy), blocked:
            self.app.engine.drain("flow")
        self.assertEqual(
            self.app.engine.status("flow")["dispositions"]["accepted"], 1000
        )
        self.app.watermarks.declare("flow", 0, 100000)
        with patch.object(Windows, "value_chunks", spy), blocked:
            self.app.engine.drain("flow")
        self.assertTrue(chunks)
        self.assertLessEqual(max(chunks), 256)
        self.assertIn(256, chunks)
        documents = self.results_by_key("flow")
        self.assertEqual(set(documents), {"a", "b", "c"})
        for key, document in documents.items():
            self.assertEqual(document["metrics"], self.reference("flow", key, 0, 100000))
            preview = self.app.windows.preview("flow", 0, key)
            self.assertGreater(len(preview["rows"]), 256)
            self.assertEqual(preview["metrics"], document["metrics"])
        for row in self.app.windows.inspect("flow"):
            self.assertEqual(row["status"], "closed")
        self.assertTrue(self.app.consistency.check("flow")["ok"])

    def test_nulls_equal_decimals_and_null_only_key(self):
        self.create(budget=4)
        self.append(
            [
                reading("n1", 0, None),
                reading("n2", 1, None),
                reading("a", 2, "1.0"),
                reading("b", 3, "1.00"),
                reading("c", 4, "1"),
                reading("d", 5, "2.5"),
                reading("e", 6, "-3.75"),
                reading("z1", 7, None, "n"),
                reading("z2", 8, None, "n"),
                reading("z3", 9, None, "n"),
            ]
        )
        self.app.engine.drain("flow")
        self.app.watermarks.declare("flow", 0, 100000)
        self.app.engine.drain("flow")
        documents = self.results_by_key("flow")
        self.assertEqual(
            documents["a"]["metrics"],
            {
                "count": 7,
                "count_value": 5,
                "sum": "1.75",
                "mean": "0.35",
                "min": "-3.75",
                "max": "2.5",
                "p50": "1",
                "p95": "2.5",
                "distinct": 3,
            },
        )
        self.assertEqual(
            documents["n"]["metrics"],
            {
                "count": 3,
                "count_value": 0,
                "sum": None,
                "mean": None,
                "min": None,
                "max": None,
                "p50": None,
                "p95": None,
                "distinct": 0,
            },
        )

    def test_savepoint_restore_matches_full_computation(self):
        self.create(budget=256)
        rows = []
        for i in range(700):
            value = None if i % 11 == 0 else f"{i}.{i % 7}"
            rows.append(reading(f"s{i}", i, value, ["x", "y"][i % 2]))
        self.append(rows)
        self.app.engine.drain("flow")
        saved = self.app.savepoints.create("flow")
        self.app.savepoints.restore_point(saved["point_id"], "restored")
        self.app.watermarks.declare("flow", 0, 100000)
        self.app.watermarks.declare("restored", 0, 100000)
        self.app.engine.drain("flow")
        self.app.engine.drain("restored")
        self.assertTrue(self.app.replay.compare("flow", "restored")["equal"])
        for key, document in self.results_by_key("restored").items():
            self.assertEqual(
                document["metrics"], self.reference("restored", key, 0, 100000)
            )
        closed = self.app.savepoints.create("flow")
        self.app.savepoints.restore_point(closed["point_id"], "restored_closed")
        self.app.engine.drain("restored_closed")
        self.assertTrue(self.app.replay.compare("flow", "restored_closed")["equal"])
        self.assertTrue(self.app.consistency.check("restored")["ok"])

    def test_hopping_interleaved_keys_spill(self):
        self.create(budget=8, window={"kind": "hopping", "size_ms": 40, "slide_ms": 10})
        self.append(
            [
                reading(f"h{i}", i, f"{i % 13}.{i % 5}", ["a", "b"][i % 2])
                for i in range(120)
            ]
        )
        self.app.engine.drain("flow")
        self.app.watermarks.declare("flow", 0, 1000)
        self.app.engine.drain("flow")
        inspected = self.app.windows.inspect("flow")
        self.assertTrue(inspected)
        for row in inspected:
            self.assertEqual(row["status"], "closed")
        documents = self.app.results.rows("flow")
        self.assertEqual(len(documents), len(inspected))
        for row in documents:
            document = row["document"]
            self.assertEqual(
                document["metrics"],
                self.reference("flow", document["key"], document["start"], document["end"]),
            )
        self.assertTrue(self.app.consistency.check("flow")["ok"])


if __name__ == "__main__":
    unittest.main()
