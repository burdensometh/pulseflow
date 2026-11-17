from pathlib import Path
from tempfile import TemporaryDirectory
import copy
import hashlib
import json
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pulseflow import PulseFlow
from pulseflow.artifacts import Artifacts
from pulseflow.service import API, query_integer, serving
from pulseflow.values import Conflict, Invalid, Missing, canonical
from test_flow import PLAN, reading, record


class ServicesTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "flow")
        self.app.journal.create("readings", 1)
        self.app.engine.create("flow", "readings", PLAN)

    def populated(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, reading("first", 0, "2")),
                record(1, reading("second", 10, "3")),
            ],
        )
        self.app.engine.drain("flow")

    def test_savepoint_restores_open_and_closed_state_then_resumes(self):
        self.populated()
        saved = self.app.savepoints.create("flow", compressed=True)
        self.assertEqual(self.app.savepoints.get(saved["point_id"])["generation"], 1)
        point = self.app.savepoints.load(saved["point_id"])
        self.assertEqual(point["format_version"], 1)
        self.app.savepoints.restore_point(saved["point_id"], "restored")
        self.assertTrue(self.app.replay.compare("flow", "restored")["equal"])
        self.assertTrue(self.app.consistency.check("restored")["ok"])
        with self.assertRaises(Conflict):
            self.app.savepoints.restore(point, "scaled", buckets=7)
        with self.assertRaises(Conflict):
            self.app.savepoints.restore(point, "restored")
        self.app.journal.append(
            "readings", "more", [record(2, reading("third", 20, "5"))]
        )
        self.app.engine.drain("flow")
        self.app.engine.drain("restored")
        self.assertTrue(self.app.replay.compare("flow", "restored")["equal"])
        self.assertTrue(self.app.consistency.check("restored")["ok"])
        self.assertEqual(len(self.app.savepoints.list("flow")), 1)
        uncompressed = self.app.savepoints.create("flow")
        self.assertFalse(uncompressed["compressed"])
        self.assertEqual(
            self.app.savepoints.load(uncompressed["point_id"])["pipeline"]["watermark"],
            20,
        )

    def test_savepoint_validation_rejects_corrupt_source_and_layout(self):
        self.populated()
        point = self.app.savepoints.load(self.app.savepoints.create("flow")["point_id"])
        altered = copy.deepcopy(point)
        altered["source_prefix_sha256"] = "0" * 64
        with self.assertRaises(Conflict):
            self.app.savepoints.restore(altered, "badprefix")
        for mutate in [
            lambda p: p.update(format_version=2),
            lambda p: p["pipeline"].update(hash_version=2),
            lambda p: p["state"]["progress"].clear(),
            lambda p: p["state"]["results"][0].append("extra"),
        ]:
            altered = copy.deepcopy(point)
            mutate(altered)
            with self.assertRaises(Invalid):
                self.app.savepoints.validate(altered)
        altered = copy.deepcopy(point)
        altered["pipeline"]["plan_hash"] = "0" * 64
        with self.assertRaises(Conflict):
            self.app.savepoints.validate(altered)
        altered = copy.deepcopy(point)
        altered["state"]["window_events"][0][0] = 100
        with self.assertRaises(Conflict):
            self.app.savepoints.validate(altered)
        with self.assertRaises(Missing):
            self.app.savepoints.load("missing")

    def test_file_ingress_complete_lines_restart_and_changed_prefix(self):
        source = self.home / "events.jsonl"
        source.write_bytes(
            (
                canonical(reading("one", 0)) + "\n" + canonical(reading("two", 10))
            ).encode()
        )
        first = self.app.files.tail("source", "readings", 0, source)
        self.assertEqual(first["records"], 1)
        self.assertGreater(first["incomplete_bytes"], 0)
        self.assertEqual(
            self.app.files.tail("source", "readings", 0, source)["records"], 0
        )
        source.write_bytes(source.read_bytes() + b"\n")
        second = self.app.files.tail("source", "readings", 0, source)
        self.assertEqual(second["records"], 1)
        self.assertEqual(self.app.files.status("source")["next_offset"], 2)
        self.app.engine.drain("flow")
        self.assertEqual(
            self.app.results.rows("flow")[0]["document"]["metrics"]["count"], 1
        )
        source.write_bytes(b"changed\n")
        with self.assertRaises(Conflict):
            self.app.files.tail("source", "readings", 0, source)
        self.assertIsNone(self.app.files.status("notbound"))

    def test_frozen_file_chunk_retry_and_invalid_complete_record(self):
        source = self.home / "frozen.jsonl"
        source.write_text(
            "".join(canonical(reading(str(i), i)) + "\n" for i in range(4))
        )
        result = self.app.files.import_frozen(
            "frozen", "readings", 0, source, chunk_size=2
        )
        self.assertEqual(result["records"], 4)
        self.assertEqual(
            self.app.files.import_frozen("frozen", "readings", 0, source)["records"], 0
        )
        self.assertEqual(
            result["input_sha256"], hashlib.sha256(source.read_bytes()).hexdigest()
        )
        broken = self.home / "bad.jsonl"
        broken.write_bytes(b"{}\n\n")
        with self.assertRaises(Invalid):
            self.app.files.tail("badfile", "readings", 0, broken)
        broken.write_bytes(b"no-json\n")
        with self.assertRaises(Invalid):
            self.app.files.tail("badfile", "readings", 0, broken)
        broken.write_bytes(b"no-newline")
        with self.assertRaises(Invalid):
            self.app.files.import_frozen("incomplete", "readings", 0, broken)

    def test_exports_are_immutable_jsonl_and_csv(self):
        self.populated()
        for format in ["jsonl", "csv"]:
            exported = self.app.exports.create("flow", format)
            raw = self.app.exports.read(exported["export_id"])
            decoded = self.app.exports.decode(exported["export_id"])
            self.assertEqual(decoded[0]["document"]["metrics"]["sum"], "2")
            self.app.results.replace_visible("flow", self.app.results.head("flow"), {})
            self.assertEqual(self.app.exports.read(exported["export_id"]), raw)
            with self.app.catalog.transaction(True) as connection:
                self.app.results.publish(
                    connection, "flow", decoded[0]["result_key"], decoded[0]["document"]
                )
        self.assertEqual(len(self.app.exports.list("flow")), 2)
        with self.assertRaises(Invalid):
            self.app.exports.create("flow", "xlsx")
        with self.assertRaises(Missing):
            self.app.exports.get("absent")

    def test_artifact_integrity_bounds_and_compression(self):
        store = Artifacts(self.home / "objects", max_bytes=1000)
        saved = store.put("one.json", b'{"ok":true}')
        self.assertEqual(store.put("one.json", b'{"ok":true}'), saved)
        self.assertTrue(store.read_document(saved["filename"], saved["sha256"])["ok"])
        compressed = store.put_document("two.gz", {"a": "x" * 300}, compressed=True)
        self.assertEqual(
            store.read_document("two.gz", compressed["sha256"], True)["a"], "x" * 300
        )
        self.assertEqual(len(store.inventory()), 2)
        for name in ["../bad", "", "a/b"]:
            with self.assertRaises(Invalid):
                store.path(name)
        with self.assertRaises(Invalid):
            store.put("large", b"x" * 1001)
        with self.assertRaises(Conflict):
            store.put("one.json", b"changed")
        with self.assertRaises(Conflict):
            store.read("one.json", "0" * 64)
        with self.assertRaises(Missing):
            store.read("missing", "0" * 64)
        invalid = store.put("not-json", b"not-json")
        with self.assertRaises(Invalid):
            store.read_document("not-json", invalid["sha256"])

    def test_exchange_preserves_partition_offsets_and_rejects_corruption(self):
        self.populated()
        package = self.app.exchange.export("readings")
        self.app.journal.create("imported", 1)
        result = self.app.exchange.import_artifact(
            package["filename"], package["sha256"], "imported", "import1"
        )
        self.assertEqual(result["accepted"], 2)
        self.assertTrue(
            self.app.exchange.import_artifact(
                package["filename"], package["sha256"], "imported", "import1"
            )["replayed"]
        )
        document = self.app.artifacts.read_document(
            package["filename"], package["sha256"], True
        )
        changed = copy.deepcopy(document)
        changed["positions"]["0"] = 99
        with self.assertRaises(Conflict):
            self.app.exchange.validate(changed)
        changed = copy.deepcopy(document)
        changed["rows"][0]["event"]["value"] = "99"
        with self.assertRaises(Conflict):
            self.app.exchange.validate(changed)
        with self.assertRaises(Invalid):
            self.app.exchange.export("readings", limit=1)
        plain = self.app.exchange.export("readings", compressed=False)
        self.assertFalse(plain["compressed"])

    def test_rollup_applies_result_changes_and_deletions(self):
        self.app.journal.append(
            "readings",
            "batch",
            [
                record(0, reading("a", 0, "2", key="a")),
                record(1, reading("b", 0, "3", key="b")),
                record(2, reading("c", 10)),
            ],
        )
        self.app.engine.drain("flow")
        definition = {
            "groups": {"a": "region", "b": "region"},
            "metrics": {
                "total": {"op": "sum", "metric": "sum"},
                "minimum": {"op": "min", "metric": "sum"},
                "maximum": {"op": "max", "metric": "sum"},
                "contributors": {"op": "count", "metric": "sum"},
                "empty": {"op": "sum", "metric": "absent"},
            },
        }
        self.app.rollups.register("region", "flow", definition)
        self.app.rollups.register("region", "flow", definition)
        self.app.rollups.catch_up("region", limit=1)
        self.assertEqual(
            self.app.rollups.rows("region")[0]["metrics"],
            {
                "total": "5",
                "minimum": "2",
                "maximum": "3",
                "contributors": 2,
                "empty": None,
            },
        )
        rows = self.app.results.rows("flow")
        self.app.results.replace_visible(
            "flow",
            self.app.results.head("flow"),
            {rows[0]["result_key"]: rows[0]["document"]},
        )
        self.app.rollups.catch_up("region")
        self.assertEqual(
            self.app.rollups.rows("region")[0]["metrics"]["contributors"], 1
        )
        self.assertTrue(self.app.rollups.runs("region"))
        with self.assertRaises(Conflict):
            self.app.rollups.register("region", "flow", {**definition, "groups": {}})
        with self.assertRaises(Missing):
            self.app.rollups.status("absent")
        with self.assertRaises(Invalid):
            self.app.rollups.register("bad", "flow", {})

    def test_release_lifecycle_over_http(self):
        import copy

        from pulseflow.consumer import Consumer

        with serving(self.app) as base:

            def request(path, body=None):
                raw = None if body is None else json.dumps(body).encode()
                req = Request(
                    base + path, data=raw, headers={"Content-Type": "application/json"}
                )
                with urlopen(req) as response:
                    return json.load(response)

            self.populated()
            cutoff = self.app.catalog.pipeline("flow")["last_sequence"]
            request("/bindings", {"logical_name": "metrics", "pipeline": "flow"})
            self.assertEqual(request("/bindings/metrics")["version"], 0)
            updated = copy.deepcopy(PLAN)
            updated["mapping"] = {"value": {"literal": "7"}}
            release = request(
                "/releases/prepare",
                {"logical_name": "metrics", "plan": updated, "cutoff_sequence": cutoff},
            )
            release_id = release["release_id"]
            self.assertEqual(request("/releases/" + release_id)["state"], "preparing")
            request("/releases/" + release_id + "/ready", {})
            request("/releases/" + release_id + "/publish", {})
            self.assertEqual(
                request("/bindings/metrics")["pipeline"], release["shadow_pipeline"]
            )
            descriptor = request(
                "/releases/" + release_id + "/descriptor", {}
            )
            self.assertEqual(descriptor["version"], 1)
            self.assertTrue(request("/releases"))
            # The consumer can switch using exactly the descriptor served over HTTP.
            consumer = Consumer(self.home / "http_consumer.sqlite")
            consumer.catch_up(self.app, "flow")
            outcome = consumer.promote(self.app, descriptor)
            self.assertEqual(outcome["release_id"], release_id)
            self.assertTrue(
                self.app.queries.compare_consumer(
                    release["shadow_pipeline"], consumer
                )["equal"]
            )

    def test_real_http_operational_chain(self):
        with serving(self.app) as base:

            def request(path, body=None):
                raw = None if body is None else json.dumps(body).encode()
                req = Request(
                    base + path, data=raw, headers={"Content-Type": "application/json"}
                )
                with urlopen(req) as response:
                    return json.load(response)

            self.assertTrue(request("/health")["ok"])
            request("/channels", {"name": "http", "partitions": 1})
            request(
                "/ingress/http",
                {
                    "batch_id": "batch",
                    "records": [
                        record(0, reading("h1", 0)),
                        record(1, reading("h2", 10)),
                    ],
                },
            )
            request("/pipelines", {"name": "httpflow", "channel": "http", "plan": PLAN})
            request("/pipelines/httpflow/run", {})
            for suffix in [
                "",
                "/results",
                "/changes",
                "/late",
                "/watermark",
                "/windows",
                "/check",
                "/lag",
                "/statistics",
                "/top",
                "/events/h1",
            ]:
                self.assertIsNotNone(request("/pipelines/httpflow" + suffix))
            self.assertEqual(request("/channels/http/positions"), {"0": 2})
            self.assertEqual(
                len(request("/channels/http/events?after=0&limit=1&partition=0")), 1
            )
            request("/pipelines/httpflow/declare", {"partition": 0, "time": 20})
            point = request("/pipelines/httpflow/savepoint", {})
            self.assertTrue(request("/savepoints/" + point["point_id"]))
            request(
                "/savepoints/" + point["point_id"] + "/restore",
                {"destination": "restored_http"},
            )
            exported = request("/pipelines/httpflow/export", {})
            self.assertTrue(request("/exports/" + exported["export_id"]))
            request(
                "/dimensions",
                {
                    "batch_id": "dimhttp",
                    "records": [
                        {
                            "dimension": "d",
                            "key": "a",
                            "valid_from": 0,
                            "revision": 1,
                            "attrs": {},
                        }
                    ],
                },
            )
            self.assertEqual(request("/dimensions/d/a?at=0")["revision"], 1)
            request(
                "/rollups",
                {
                    "name": "r",
                    "pipeline": "httpflow",
                    "definition": {
                        "groups": {},
                        "metrics": {"total": {"op": "sum", "metric": "sum"}},
                    },
                },
            )
            request("/rollups/r/refresh", {})
            self.assertTrue(request("/rollups/r")["rows"])
            package = request("/channels/http/export", {})
            request("/channels", {"name": "httpcopy", "partitions": 1})
            request(
                "/exchange/import",
                {**package, "destination": "httpcopy", "batch_id": "copy"},
            )
            request("/replay", {"source": "httpflow", "destination": "httpfork"})
            request(
                "/pipelines/httpflow/replace-results",
                {"expected_head": self.app.results.head("httpflow"), "documents": {}},
            )
            with self.assertRaises(HTTPError) as context:
                request("/unknown")
            self.assertEqual(context.exception.code, 404)
            with self.assertRaises(HTTPError) as context:
                request("/channels", {"name": "http", "partitions": 2})
            self.assertEqual(context.exception.code, 409)
            with self.assertRaises(HTTPError) as context:
                request("/channels/http/events?limit=no")
            self.assertEqual(context.exception.code, 400)
        with self.assertRaises(Invalid):
            with serving(self.app, host="0.0.0.0"):
                pass
        with self.assertRaises(Invalid):
            query_integer({"x": ["1", "2"]}, "x")
        api = API(self.app)
        for method, path in [
            ("DELETE", "/health"),
            ("POST", "/unknown"),
            ("POST", "/pipelines/flow/unknown"),
        ]:
            with self.assertRaises(Missing):
                api.dispatch(method, path, {}, {})
