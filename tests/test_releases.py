from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import copy
import unittest

from pulseflow import PulseFlow
from pulseflow.consumer import Consumer
from pulseflow.values import Conflict, Invalid, Missing
from test_flow import PLAN, reading, record


UPDATED_PLAN = copy.deepcopy(PLAN)
UPDATED_PLAN["mapping"] = {"value": {"literal": "7"}}


class ReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.app = PulseFlow(self.home / "flow")
        self.app.journal.create("readings", 1)
        self.app.engine.create("flow", "readings", PLAN)
        self.app.releases.bind("metrics", "flow")
        self.consumer = Consumer(self.home / "consumer.sqlite")

    def append(self, rows, batch):
        start = self.app.journal.positions("readings")["0"]
        self.app.journal.append(
            "readings", batch, [record(start + i, row) for i, row in enumerate(rows)]
        )

    def seed(self):
        # First two events form the explicit cutoff prefix.
        self.append([reading("one", 0, "2"), reading("two", 10, "3")], "batch-1")
        self.app.engine.drain("flow")
        self.consumer.catch_up(self.app, "flow")

    def test_full_lifecycle_backfill_catchup_publish_and_promote(self):
        self.seed()
        cutoff = self.app.catalog.pipeline("flow")["last_sequence"]
        release = self.app.releases.prepare("metrics", UPDATED_PLAN, cutoff)
        self.assertEqual(release["state"], "preparing")
        self.assertEqual(release["version"], 1)
        self.assertEqual(release["cutoff_sequence"], cutoff)
        shadow = release["shadow_pipeline"]
        # The shadow backfilled exactly the declared source prefix.
        self.assertEqual(
            self.app.catalog.pipeline(shadow)["last_sequence"], cutoff
        )
        self.assertEqual(self.app.engine.status(shadow)["pending"], 0)
        # Old results stay readable while the release is being prepared.
        old_rows = self.app.results.rows("flow")
        self.assertTrue(old_rows)
        # Every mapped event carries the literal value 7, one event per window.
        self.assertEqual(
            self.app.results.rows(shadow)[0]["document"]["metrics"]["sum"], "7"
        )
        # Events appended after the cutoff must be caught, not lost or doubled.
        self.append([reading("three", 20, "5")], "batch-2")
        with self.app.catalog.transaction() as connection:
            head_sequence = connection.execute(
                "SELECT max(sequence) FROM journal WHERE channel='readings'"
            ).fetchone()[0]
        caught = self.app.releases.catch_up(release["release_id"])
        self.assertTrue(caught["caught_up"])
        self.assertEqual(caught["shadow_last_sequence"], head_sequence)
        ready = self.app.releases.ready(release["release_id"])
        self.assertEqual(ready["state"], "ready")
        # Readiness requires the complete journal.
        self.append([reading("four", 30, "11")], "batch-3")
        with self.assertRaises(Conflict):
            self.app.releases.publish(release["release_id"])
        self.app.releases.catch_up(release["release_id"])
        published = self.app.releases.publish(release["release_id"])
        self.assertEqual(published["state"], "published")
        # The logical name now points at the new version; old pipeline remains.
        binding = self.app.releases.current("metrics")
        self.assertEqual(binding["pipeline"], shadow)
        self.assertEqual(binding["version"], 1)
        self.assertEqual(self.app.results.rows("flow"), old_rows)
        # Consumer switches wholesale onto the new result and new cursor.
        descriptor = self.app.releases.descriptor(release["release_id"])
        before_cursor = self.consumer.status()["cursor"]
        outcome = self.consumer.promote(self.app, descriptor)
        self.assertFalse(outcome["replayed"])
        self.assertEqual(outcome["cursor"], descriptor["change_head"])
        self.assertGreater(descriptor["change_head"], 0)
        # Materialized content is exactly the new version, no old entries mixed.
        self.assertTrue(
            self.app.queries.compare_consumer(shadow, self.consumer)["equal"]
        )
        self.assertFalse(
            self.app.queries.compare_consumer("flow", self.consumer)["equal"]
        )
        for row in self.consumer.rows():
            self.assertEqual(row["document"]["metrics"]["sum"], "7")
        # Resuming after the switch continues from the new pipeline changelog.
        self.append([reading("five", 40, "13")], "batch-4")
        self.app.engine.drain("flow")
        self.app.engine.drain(shadow)
        self.consumer.catch_up(self.app, shadow)
        self.assertTrue(
            self.app.queries.compare_consumer(shadow, self.consumer)["equal"]
        )
        # Switch ledger records exactly one version transition.
        switches = self.consumer.switches()
        self.assertEqual(len(switches), 1)
        self.assertEqual(switches[0]["release_id"], release["release_id"])

    def test_promote_is_idempotent_and_rejects_older_releases(self):
        self.seed()
        cutoff = self.app.catalog.pipeline("flow")["last_sequence"]
        first = self.app.releases.prepare("metrics", UPDATED_PLAN, cutoff)
        self.app.releases.publish(first["release_id"])
        descriptor_one = self.app.releases.descriptor(first["release_id"])
        self.consumer.promote(self.app, descriptor_one)
        repeated = self.consumer.promote(self.app, descriptor_one)
        self.assertTrue(repeated["replayed"])
        self.assertEqual(len(self.consumer.switches()), 1)
        # A second, newer release can be applied.
        second_plan = copy.deepcopy(PLAN)
        second_plan["mapping"] = {"value": {"literal": "9"}}
        second = self.app.releases.prepare("metrics", second_plan, cutoff)
        self.assertEqual(second["version"], 2)
        self.app.releases.publish(second["release_id"])
        self.consumer.promote(self.app, self.app.releases.descriptor(second["release_id"]))
        self.assertEqual(len(self.consumer.switches()), 2)
        # The older descriptor is now fenced off at the consumer side.
        with self.assertRaises(Conflict):
            self.consumer.promote(self.app, descriptor_one)

    def test_cancelled_and_failed_releases_keep_the_old_version(self):
        self.seed()
        cutoff = self.app.catalog.pipeline("flow")["last_sequence"]
        release = self.app.releases.prepare("metrics", UPDATED_PLAN, cutoff)
        self.assertEqual(self.app.releases.cancel(release["release_id"])["state"], "cancelled")
        with self.assertRaises(Conflict):
            self.app.releases.publish(release["release_id"])
        # The logical binding and old results are untouched.
        self.assertEqual(self.app.releases.current("metrics")["pipeline"], "flow")
        self.assertTrue(self.app.results.rows("flow"))
        with self.assertRaises(Conflict):
            self.app.releases.descriptor(release["release_id"])
        # A failure while backfilling rolls the whole chunk back and leaves no
        # published version: retrying produces the same shadow state.
        with patch.object(
            self.app.results, "publish", side_effect=OSError("storage failure")
        ):
            with self.assertRaises(OSError):
                self.app.releases.prepare("metrics", UPDATED_PLAN, cutoff)
        self.assertEqual(self.app.releases.current("metrics")["version"], 0)

    def test_stale_release_cannot_overwrite_a_newer_version(self):
        self.seed()
        cutoff = self.app.catalog.pipeline("flow")["last_sequence"]
        older = self.app.releases.prepare("metrics", UPDATED_PLAN, cutoff)
        newer_plan = copy.deepcopy(PLAN)
        newer_plan["mapping"] = {"value": {"literal": "1"}}
        newer = self.app.releases.prepare("metrics", newer_plan, cutoff)
        self.app.releases.publish(newer["release_id"])
        with self.assertRaises(Conflict):
            self.app.releases.publish(older["release_id"])
        # Cancelling the loser is still allowed and changes nothing visible.
        self.app.releases.cancel(older["release_id"])
        self.assertEqual(
            self.app.releases.current("metrics")["pipeline"], newer["shadow_pipeline"]
        )
        self.assertEqual(
            self.app.releases.list("metrics")[0]["state"], "cancelled"
        )
        self.assertEqual(
            self.app.releases.list("metrics")[1]["state"], "published"
        )

    def test_cutoff_guard_and_prefix_tampering_are_rejected(self):
        self.seed()
        cutoff = self.app.catalog.pipeline("flow")["last_sequence"]
        # Cutoff beyond the journal is not a valid source point.
        with self.assertRaises(Invalid):
            self.app.releases.prepare("metrics", UPDATED_PLAN, cutoff + 100)
        release = self.app.releases.prepare("metrics", UPDATED_PLAN, cutoff)
        self.append([reading("three", 20, "5")], "batch-2")
        # Tamper with the recorded cutoff fingerprint: readiness must refuse.
        with self.app.catalog.transaction(True) as connection:
            connection.execute(
                "UPDATE releases SET source_prefix_sha256=? WHERE release_id=?",
                ("0" * 64, release["release_id"]),
            )
        with self.assertRaises(Conflict):
            self.app.releases.ready(release["release_id"])
        with self.assertRaises(Conflict):
            self.app.releases.publish(release["release_id"])

    def test_ready_requires_complete_journal_and_unknown_names_fail(self):
        self.seed()
        cutoff = self.app.catalog.pipeline("flow")["last_sequence"]
        release = self.app.releases.prepare("metrics", UPDATED_PLAN, cutoff)
        self.append([reading("three", 20, "5")], "batch-2")
        with self.assertRaises(Conflict):
            self.app.releases.ready(release["release_id"])
        self.app.releases.catch_up(release["release_id"])
        self.assertEqual(self.app.releases.ready(release["release_id"])["state"], "ready")
        with self.assertRaises(Missing):
            self.app.releases.current("unbound-logical")
        with self.assertRaises(Missing):
            self.app.releases.status("deadbeef")
        with self.assertRaises(Conflict):
            self.app.releases.bind("metrics", "flow")

    def test_fixed_pipeline_consumption_still_works_after_releases(self):
        self.seed()
        cutoff = self.app.catalog.pipeline("flow")["last_sequence"]
        release = self.app.releases.prepare("metrics", UPDATED_PLAN, cutoff)
        self.app.releases.publish(release["release_id"])
        # A second consumer keeps the legacy fixed-pipeline contract end to end.
        legacy = Consumer(self.home / "legacy.sqlite")
        legacy.catch_up(self.app, "flow")
        self.assertTrue(self.app.queries.compare_consumer("flow", legacy)["equal"])
        self.append([reading("three", 20, "5")], "batch-2")
        self.app.engine.drain("flow")
        legacy.catch_up(self.app, "flow")
        self.assertTrue(self.app.queries.compare_consumer("flow", legacy)["equal"])
        legacy.bootstrap(self.app, "flow")
        self.assertTrue(self.app.queries.compare_consumer("flow", legacy)["equal"])
        # Promoting an unknown subscription is rejected.
        with self.assertRaises(Missing):
            Consumer(self.home / "blank.sqlite").promote(
                self.app, self.app.releases.descriptor(release["release_id"])
            )

    def test_cross_instance_and_corrupt_descriptor_are_rejected(self):
        self.seed()
        cutoff = self.app.catalog.pipeline("flow")["last_sequence"]
        release = self.app.releases.prepare("metrics", UPDATED_PLAN, cutoff)
        self.app.releases.publish(release["release_id"])
        descriptor = self.app.releases.descriptor(release["release_id"])
        other_home = PulseFlow(self.home / "other")
        other_home.journal.create("readings", 1)
        other_home.engine.create("flow", "readings", PLAN)
        with self.assertRaises(Conflict):
            self.consumer.promote(other_home, descriptor)
        corrupt = copy.deepcopy(descriptor)
        corrupt["source_prefix_sha256"] = "f" * 64
        with self.assertRaises(Conflict):
            self.consumer.promote(self.app, corrupt)
        malformed = {"format_version": 99}
        with self.assertRaises(Invalid):
            self.consumer.promote(self.app, malformed)


if __name__ == "__main__":
    unittest.main()
