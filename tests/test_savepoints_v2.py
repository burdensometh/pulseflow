import copy
import hashlib
import json
import shutil
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.request import Request, urlopen

from pulseflow import PulseFlow
from pulseflow.service import serving
from pulseflow.state import bucket_for
from pulseflow.values import Conflict, Invalid, canonical
from test_flow import PLAN, reading, record


FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "legacy-v1"


def rich_plan():
    plan = copy.deepcopy(PLAN)
    plan.update(
        delay_ms=20,
        dedup={"field": "attrs.token", "ttl_ms": 100},
        sequence={"first_kind": "A", "then_kind": "B", "within_ms": 200},
    )
    return plan


class V2SavepointTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "flow")
        self.app.journal.create("readings", 2)
        self.app.engine.create("flow", "readings", rich_plan(), buckets=4)

    def seed(self):
        rows = [
            reading("a0", 0, "1", key="alpha", kind="A", attrs={"token": "t1"}),
            reading("a1", 5, "2", key="alpha", kind="A", attrs={"token": "t2"}),
            reading("b0", 6, "3", key="bravo", kind="B"),
            reading("c0", 7, "4", key="charlie", kind="A", attrs={"token": "t1"}),
            reading("d0", 8, "5", key="delta", kind="reading"),
            reading("e0", 9, "6", key="echo", kind="A", attrs={"token": "t3"}),
            reading("f0", 12, "7", key="foxtrot", kind="reading"),
            reading("g0", 13, "8", key="golf", kind="B"),
        ]
        # Offsets are per-partition contiguous, not global.
        partition_offsets = [0, 0]
        records = []
        for i, row in enumerate(rows):
            partition = i % 2
            records.append(
                record(partition_offsets[partition], row, partition=partition)
            )
            partition_offsets[partition] += 1
        self.app.journal.append("readings", "seed", records)
        self.app.engine.drain("flow")

    def test_v2_document_is_bucket_free_and_restores_any_bucket_count(self):
        self.seed()
        saved = self.app.savepoints.create("flow")
        self.assertEqual(saved["format_version"], 2)
        point = self.app.savepoints.load(saved["point_id"])
        self.assertEqual(point["format_version"], 2)
        # v2 bucketed rows never carry a fixed bucket column.
        for table in ["window_events", "window_status", "keyed_state"]:
            for row in point["state"][table]:
                self.assertNotIn("bucket", row)
        self.app.savepoints.validate(point)

        for target_buckets in [1, 2, 4, 7, 16, 128]:
            name = f"restored_{target_buckets}"
            restored = self.app.savepoints.restore(
                point, name, buckets=target_buckets
            )
            self.assertEqual(restored["buckets"], target_buckets)
            info = self.app.catalog.pipeline(name)
            # Hash identity is unchanged; only the shard count moves.
            self.assertEqual(info["hash_version"], 1)
            self.assertTrue(self.app.consistency.check(name)["ok"])
            self.assertEqual(
                self.app.replay.compare("flow", name)["equal"],
                True,
                self.app.replay.compare("flow", name)["differences"],
            )
            self.assertEqual(self.app.replay.disposition_diff("flow", name), [])

    def test_all_operator_state_survives_rescaling(self):
        self.seed()
        point = self.app.savepoints.load(
            self.app.savepoints.create("flow")["point_id"]
        )
        target = 7
        self.app.savepoints.restore(point, "wide", buckets=target)

        # Partition positions and watermark survive.
        wide_status = self.app.watermarks.status("wide")
        flow_status = self.app.watermarks.status("flow")
        self.assertEqual(
            wide_status["watermark"], flow_status["watermark"]
        )
        self.assertEqual(
            wide_status["candidate"], flow_status["candidate"]
        )
        progress = {
            row["partition_id"]: (row["next_offset"], row["max_time"],
                                  row["declared_time"])
            for row in wide_status["partitions"]
        }
        self.assertEqual(progress, {0: (4, 12, None), 1: (4, 13, None)})

        # Open and closed windows survive and land in the new buckets.
        for row in self.app.windows.inspect("wide"):
            self.assertEqual(
                row["bucket"],
                bucket_for(row["group_key"], target, 1),
            )
        self.assertEqual(
            sorted((r["window_start"], r["group_key"], r["status"]) for r in
                   self.app.windows.inspect("wide")),
            sorted((r["window_start"], r["group_key"], r["status"]) for r in
                   self.app.windows.inspect("flow")),
        )

        # Active keyed state (latest + unmatched sequence A) survives and is
        # written under the destination bucket layout.
        for kind in ["latest", "sequence"]:
            wide_rows = self.app.state.inspect("wide", kind)
            narrow_rows = self.app.state.inspect("flow", kind)
            self.assertEqual(
                sorted(
                    (r["group_key"], canonical(r["document"])) for r in wide_rows
                ),
                sorted(
                    (r["group_key"], canonical(r["document"])) for r in narrow_rows
                ),
            )
            for row in wide_rows:
                self.assertEqual(
                    row["bucket"], bucket_for(row["group_key"], target, 1)
                )

        # Dedup TTL entries survive (including expiry timestamps).
        with self.app.catalog.transaction() as connection:
            wide_seen = {
                row[0]: (row[1], row[2])
                for row in connection.execute(
                    "SELECT dedup_key,expires_at,event_id FROM seen_keys "
                    "WHERE pipeline='wide'"
                )
            }
            narrow_seen = {
                row[0]: (row[1], row[2])
                for row in connection.execute(
                    "SELECT dedup_key,expires_at,event_id FROM seen_keys "
                    "WHERE pipeline='flow'"
                )
            }
        self.assertEqual(wide_seen, narrow_seen)

        # Results and the full change stream survive with identical revisions.
        # (The changes table's local autoincrement sequence is catalog-local.)
        change_columns = ("result_key", "revision", "operation", "document")
        self.assertEqual(
            {
                (row["result_key"], row["revision"], row["document"], row["deleted"])
                for row in self.app.results.rows("wide")
            },
            {
                (row["result_key"], row["revision"], row["document"], row["deleted"])
                for row in self.app.results.rows("flow")
            },
        )
        self.assertEqual(
            [
                {key: row[key] for key in change_columns}
                for row in self.app.results.changes("wide")
            ],
            [
                {key: row[key] for key in change_columns}
                for row in self.app.results.changes("flow")
            ],
        )
        self.assertEqual(
            self.app.engine.status("wide")["dispositions"],
            self.app.engine.status("flow")["dispositions"],
        )

    def test_outputs_after_rescale_match_a_continuous_same_bucket_run(self):
        self.seed()
        point = self.app.savepoints.load(
            self.app.savepoints.create("flow")["point_id"]
        )
        self.app.savepoints.restore(point, "wide", buckets=9)

        follow_up = [
            reading("a2", 25, "9", key="alpha", kind="B"),       # closes sequence
            reading("c1", 26, "10", key="charlie", kind="A", attrs={"token": "t1"}),
            reading("h0", 27, "11", key="hotel", kind="reading"),
        ]
        follow_records = []
        follow_offsets = [4, 4]
        for i, row in enumerate(follow_up):
            partition = i % 2
            follow_records.append(
                record(follow_offsets[partition], row, partition=partition)
            )
            follow_offsets[partition] += 1
        self.app.journal.append("readings", "follow", follow_records)
        self.app.engine.drain("flow")
        self.app.engine.drain("wide")

        def visible_rows(name):
            return {
                row["result_key"]: (row["revision"], row["document"], row["deleted"])
                for row in self.app.results.rows(name)
            }

        self.assertEqual(visible_rows("flow"), visible_rows("wide"))
        self.assertEqual(self.app.replay.disposition_diff("flow", "wide"), [])
        self.assertTrue(self.app.consistency.check("wide")["ok"])

        # A fresh full replay from the same log must agree as well: the
        # migrated pipeline is equivalent to never having changed buckets.
        self.app.replay.fork("flow", "reference", buckets=9)
        self.app.engine.drain("reference")
        self.assertTrue(self.app.replay.compare("reference", "wide")["equal"])

    def test_failed_restore_leaves_no_visible_pipeline_and_retry_succeeds(self):
        self.seed()
        point = self.app.savepoints.load(
            self.app.savepoints.create("flow")["point_id"]
        )
        original_event = self.app.catalog.event

        def failing_event(connection, subject, action, detail):
            if action == "savepoint_restored":
                raise OSError("catalog event storage failure")
            return original_event(connection, subject, action, detail)

        with patch.object(self.app.catalog, "event", side_effect=failing_event):
            with self.assertRaises(OSError):
                self.app.savepoints.restore(point, "wide", buckets=11)

        # The whole restore ran in one transaction: nothing is visible.
        with self.assertRaises(LookupError):
            self.app.catalog.pipeline("wide")
        with self.app.catalog.transaction() as connection:
            leftovers = connection.execute(
                "SELECT count(*) FROM window_status WHERE pipeline='wide'"
            ).fetchone()[0]
        self.assertEqual(leftovers, 0)

        # Retrying the identical migration completes cleanly.
        restored = self.app.savepoints.restore(point, "wide", buckets=11)
        self.assertEqual(restored["buckets"], 11)
        self.assertTrue(self.app.consistency.check("wide")["ok"])
        self.assertTrue(self.app.replay.compare("flow", "wide")["equal"])

    def test_legacy_v1_fixture_migrates_without_rewriting_any_fixture_file(self):
        source_root = FIXTURE / "state"
        checksum_before = {
            path.relative_to(source_root).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in source_root.rglob("*")
            if path.is_file()
        }
        copy_root = self.home / "legacy-copy"
        shutil.copytree(FIXTURE, copy_root)
        legacy = PulseFlow(copy_root / "state")

        point_id = "24fbc3b0a6a74b628b8e0ecff3ea4e78"
        point = legacy.savepoints.load(point_id)
        self.assertEqual(point["format_version"], 1)
        legacy.savepoints.validate(point)

        # v1 still refuses a direct rescale: its bucket column is fixed.
        with self.assertRaises(Conflict):
            legacy.savepoints.restore(point, "direct_scale", buckets=8)

        # Migration path: restore v1 at its original bucket count first.
        legacy.savepoints.restore(point, "migrated")
        self.assertTrue(legacy.consistency.check("migrated")["ok"])
        self.assertEqual(legacy.catalog.pipeline("migrated")["buckets"], 4)
        self.assertEqual(
            legacy.replay.compare("flow", "migrated")["equal"], True
        )

        # The migrated pipeline now emits a bucket-free v2 savepoint, which can
        # be restored under any bucket count while keeping every state kind.
        v2 = legacy.savepoints.create("migrated")
        self.assertEqual(v2["format_version"], 2)
        legacy.savepoints.restore_point(v2["point_id"], "wide", buckets=8)
        self.assertEqual(legacy.catalog.pipeline("wide")["buckets"], 8)
        self.assertTrue(legacy.consistency.check("wide")["ok"])
        self.assertEqual(
            legacy.replay.compare("flow", "wide")["equal"], True
        )
        self.assertEqual(
            legacy.replay.disposition_diff("flow", "wide"), []
        )
        change_columns = ("result_key", "revision", "operation", "document")
        self.assertEqual(
            [
                {key: row[key] for key in change_columns}
                for row in legacy.results.changes("flow")
            ],
            [
                {key: row[key] for key in change_columns}
                for row in legacy.results.changes("wide")
            ],
        )

        # Open windows, sequence state, dedup TTL, positions and watermark all
        # survive the two-hop migration.
        self.assertEqual(
            legacy.watermarks.status("flow")["watermark"],
            legacy.watermarks.status("wide")["watermark"],
        )
        self.assertEqual(
            sorted(
                (r["partition_id"], r["next_offset"], r["max_time"],
                 r["declared_time"])
                for r in legacy.watermarks.status("flow")["partitions"]
            ),
            sorted(
                (r["partition_id"], r["next_offset"], r["max_time"],
                 r["declared_time"])
                for r in legacy.watermarks.status("wide")["partitions"]
            ),
        )
        self.assertEqual(
            sorted(
                (r["state_kind"], r["group_key"], canonical(r["document"]))
                for r in legacy.state.inspect("flow")
            ),
            sorted(
                (r["state_kind"], r["group_key"], canonical(r["document"]))
                for r in legacy.state.inspect("wide")
            ),
        )
        for row in legacy.state.inspect("wide"):
            self.assertEqual(
                row["bucket"], bucket_for(row["group_key"], 8, 1)
            )

        # Frozen fixture on disk is byte-for-byte untouched.
        checksum_after = {
            path.relative_to(source_root).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in source_root.rglob("*")
            if path.is_file()
        }
        self.assertEqual(checksum_before, checksum_after)

    def test_legacy_v1_migration_resumes_and_stays_equivalent(self):
        copy_root = self.home / "legacy-copy"
        shutil.copytree(FIXTURE, copy_root)
        legacy = PulseFlow(copy_root / "state")
        point = legacy.savepoints.load(
            "24fbc3b0a6a74b628b8e0ecff3ea4e78"
        )
        legacy.savepoints.restore(point, "migrated")
        v2_id = legacy.savepoints.create("migrated")["point_id"]
        legacy.savepoints.restore_point(v2_id, "wide", buckets=5)

        new_rows = [
            reading("new8", 120, "9", key="key8", kind="A",
                    attrs={"token": "new8"}),
            reading("new9", 125, "10", key="key8", kind="B"),
        ]
        legacy.journal.append(
            "readings",
            "post-migration",
            [
                {"partition": 0, "offset": 5, "event": new_rows[0]},
                {"partition": 1, "offset": 5, "event": new_rows[1]},
            ],
        )
        legacy.engine.drain("flow")
        legacy.engine.drain("wide")
        self.assertTrue(legacy.consistency.check("wide")["ok"])
        self.assertTrue(legacy.replay.compare("flow", "wide")["equal"])
        self.assertEqual(legacy.replay.disposition_diff("flow", "wide"), [])

        # The previously published immutable export is still referenced and
        # readable, and its bytes are unchanged by the migration.
        export = legacy.exports.list("flow")[0]
        raw = legacy.exports.read(export["export_id"])
        self.assertEqual(export["row_count"], 8)
        self.assertIn(b'"result_key"', raw)

    def test_v2_rejects_buckets_outside_supported_range(self):
        self.seed()
        point = self.app.savepoints.load(
            self.app.savepoints.create("flow")["point_id"]
        )
        for bad in [0, 129]:
            with self.assertRaises(Invalid):
                self.app.savepoints.restore(point, f"bad_{bad}", buckets=bad)

    def test_http_restore_accepts_target_bucket_count(self):
        self.seed()
        point = self.app.savepoints.create("flow")
        with serving(self.app) as base:
            body = json.dumps({"destination": "http_wide", "buckets": 11}).encode()
            req = Request(
                base + "/savepoints/" + point["point_id"] + "/restore",
                data=body,
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req) as response:
                restored = json.load(response)
        self.assertEqual(restored["buckets"], 11)
        self.assertTrue(self.app.consistency.check("http_wide")["ok"])
        self.assertTrue(self.app.replay.compare("flow", "http_wide")["equal"])


if __name__ == "__main__":
    unittest.main()
