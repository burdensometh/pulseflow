import uuid

from .spec import plan_spec
from .values import (
    Conflict,
    Invalid,
    Missing,
    identifier,
    integer,
    utc_now,
)


class Releases:
    """Versioned plan publishing over the same source channel.

    A release creates an independent shadow pipeline that first backfills a
    declared source prefix (the cutoff), then catches the events appended while
    preparation was running. Publishing atomically moves a logical name onto the
    new results; consumers switch their whole materialized state via
    ``Consumer.promote``. Failing or cancelling keeps the previously published
    version readable, and stale releases can never overwrite a newer one.
    """

    def __init__(self, app):
        self.app = app

    def get(self, release_id, connection=None):
        if connection is None:
            with self.app.catalog.transaction() as connection:
                return self.get(release_id, connection)
        row = connection.execute(
            "SELECT * FROM releases WHERE release_id=?", (release_id,)
        ).fetchone()
        if row is None:
            raise Missing("release does not exist")
        return dict(row)

    def prepare(self, logical_name, plan, cutoff_sequence=0, buckets=None):
        """Create a shadow pipeline for a new plan and backfill the source prefix.

        ``cutoff_sequence`` is the explicit source cutoff: the shadow drains
        exactly the journal prefix whose last sequence is that value, recording
        its fingerprint. Events appended afterwards are drained separately via
        :meth:`catch_up`, which keeps the prefix evidence immutable.
        """
        logical_name = identifier(logical_name, "logical name")
        plan = plan_spec(plan)
        integer(cutoff_sequence, "source cutoff sequence", 0)
        with self.app.catalog.transaction(True) as connection:
            base = self._binding(connection, logical_name)
            if base is None:
                raise Missing(
                    "logical name has no current pipeline; bind one with engine.create"
                )
            channel = base["pipeline"]
            info = self.app.catalog.pipeline(channel, connection)
            channel_name = info["channel"]
            maximum = connection.execute(
                "SELECT coalesce(max(sequence),0) FROM journal WHERE channel=?",
                (channel_name,),
            ).fetchone()[0]
            if cutoff_sequence > maximum:
                raise Invalid("source cutoff is beyond the available journal")
            prefix_digest = self.app.journal.digest_prefix(
                connection, channel_name, cutoff_sequence
            )
            version = (
                connection.execute(
                    "SELECT coalesce(max(version),0) FROM releases WHERE logical_name=?",
                    (logical_name,),
                ).fetchone()[0]
                + 1
            )
            release_id = uuid.uuid4().hex
            shadow = identifier(
                logical_name + "__v" + str(version) + "__" + release_id[:8],
                "shadow pipeline name",
            )
        shadow_info = self.app.engine.create(
            shadow,
            channel_name,
            plan,
            info["buckets"] if buckets is None else buckets,
        )
        if cutoff_sequence:
            self._drain_to(shadow, cutoff_sequence)
        with self.app.catalog.transaction(True) as connection:
            current = self.app.catalog.pipeline(shadow, connection)
            if current["last_sequence"] != cutoff_sequence:
                raise Conflict("shadow backfill did not stop at the source cutoff")
            if (
                self.app.journal.digest_prefix(
                    connection, channel_name, cutoff_sequence
                )
                != prefix_digest
            ):
                raise Conflict("source prefix changed during backfill")
            head = self.app.results.head(shadow, connection)
            connection.execute(
                "INSERT INTO releases VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    release_id,
                    logical_name,
                    version,
                    channel_name,
                    shadow,
                    shadow_info["plan_hash"],
                    cutoff_sequence,
                    prefix_digest,
                    head,
                    "preparing",
                    utc_now(),
                    None,
                ),
            )
            connection.execute(
                "INSERT INTO release_shadows VALUES (?,?)", (release_id, shadow)
            )
            self.app.catalog.event(
                connection,
                logical_name,
                "release_prepared",
                {
                    "release_id": release_id,
                    "version": version,
                    "shadow": shadow,
                    "cutoff_sequence": cutoff_sequence,
                },
            )
        return self.get(release_id)

    def _drain_to(self, pipeline, target_sequence):
        """Drain until the pipeline cursor reaches exactly ``target_sequence``."""
        while True:
            status = self.app.engine.status(pipeline)
            if status["last_sequence"] == target_sequence:
                return
            if status["last_sequence"] > target_sequence:
                raise Conflict("pipeline cursor overshot the requested cutoff")
            self.app.engine.drain(pipeline)

    def catch_up(self, release_id, target_sequence=None):
        """Drain journal events appended after the cutoff into the shadow.

        With no target it drains everything currently available. The cutoff
        prefix itself is never reprocessed: the shadow simply continues from its
        committed cursor, so appended data cannot be missed or counted twice.
        """
        with self.app.catalog.transaction() as connection:
            release = self.get(release_id, connection)
        if release["state"] not in {"preparing", "ready"}:
            raise Conflict("only active releases can catch up appended data")
        with self.app.catalog.transaction() as connection:
            maximum = connection.execute(
                "SELECT coalesce(max(sequence),0) FROM journal WHERE channel=?",
                (release["channel"],),
            ).fetchone()[0]
        if target_sequence is None:
            target_sequence = maximum
        integer(target_sequence, "catch-up target sequence", 0)
        if target_sequence < release["cutoff_sequence"]:
            raise Invalid("catch-up target cannot move before the source cutoff")
        if target_sequence > maximum:
            raise Invalid("catch-up target is beyond the available journal")
        self._drain_to(release["shadow_pipeline"], target_sequence)
        return self.status(release_id)

    def ready(self, release_id):
        """Mark the release ready once the shadow has consumed every event."""
        with self.app.catalog.transaction(True) as connection:
            release = self.get(release_id, connection)
            if release["state"] == "ready":
                return release
            if release["state"] != "preparing":
                raise Conflict("only preparing releases can be marked ready")
            shadow = self.app.catalog.pipeline(
                release["shadow_pipeline"], connection
            )
            maximum = connection.execute(
                "SELECT coalesce(max(sequence),0) FROM journal WHERE channel=?",
                (release["channel"],),
            ).fetchone()[0]
            if shadow["last_sequence"] != maximum:
                raise Conflict(
                    "shadow pipeline must catch the complete journal before readiness"
                )
            if (
                self.app.journal.digest_prefix(
                    connection, release["channel"], release["cutoff_sequence"]
                )
                != release["source_prefix_sha256"]
            ):
                raise Conflict("source prefix fingerprint no longer matches")
            connection.execute(
                "UPDATE releases SET state='ready',change_head=? WHERE release_id=?",
                (self.app.results.head(release["shadow_pipeline"], connection), release_id),
            )
            self.app.catalog.event(
                connection,
                release["logical_name"],
                "release_ready",
                {"release_id": release_id, "version": release["version"]},
            )
        return self.get(release_id)

    def publish(self, release_id):
        """Atomically move the logical name onto the shadow results.

        A release only wins while it is still the newest version for the logical
        name and remains in an active state: a stale or cancelled task is fenced
        off and can never overwrite a newer version.
        """
        with self.app.catalog.transaction(True) as connection:
            release = self.get(release_id, connection)
            if release["state"] == "published":
                return release
            if release["state"] not in {"preparing", "ready"}:
                raise Conflict("cancelled releases cannot be published")
            newest = connection.execute(
                "SELECT max(version) FROM releases WHERE logical_name=?",
                (release["logical_name"],),
            ).fetchone()[0]
            if release["version"] != newest:
                raise Conflict(
                    "a newer release exists; this stale publish is fenced"
                )
            shadow = self.app.catalog.pipeline(
                release["shadow_pipeline"], connection
            )
            maximum = connection.execute(
                "SELECT coalesce(max(sequence),0) FROM journal WHERE channel=?",
                (release["channel"],),
            ).fetchone()[0]
            if shadow["last_sequence"] != maximum:
                raise Conflict(
                    "shadow pipeline must catch the complete journal before publishing"
                )
            if (
                self.app.journal.digest_prefix(
                    connection, release["channel"], release["cutoff_sequence"]
                )
                != release["source_prefix_sha256"]
            ):
                raise Conflict("source prefix fingerprint no longer matches")
            head = self.app.results.head(release["shadow_pipeline"], connection)
            now = utc_now()
            connection.execute(
                "UPDATE releases SET state='published',published_at=?,change_head=? "
                "WHERE release_id=?",
                (now, head, release_id),
            )
            connection.execute(
                "INSERT INTO release_bindings VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(logical_name) DO UPDATE SET "
                "pipeline=excluded.pipeline,plan_hash=excluded.plan_hash,"
                "release_id=excluded.release_id,version=excluded.version,"
                "switched_at=excluded.switched_at",
                (
                    release["logical_name"],
                    release["shadow_pipeline"],
                    release["plan_hash"],
                    release_id,
                    release["version"],
                    now,
                ),
            )
            self.app.catalog.event(
                connection,
                release["logical_name"],
                "release_published",
                {
                    "release_id": release_id,
                    "version": release["version"],
                    "shadow": release["shadow_pipeline"],
                    "change_head": head,
                },
            )
        return self.get(release_id)

    def cancel(self, release_id, reason=None):
        """Abandon an unpublished release; the current version stays untouched."""
        if reason is not None and not isinstance(reason, str):
            raise Invalid("cancel reason must be a string")
        with self.app.catalog.transaction(True) as connection:
            release = self.get(release_id, connection)
            if release["state"] in {"published", "cancelled"}:
                raise Conflict("only active releases can be cancelled")
            connection.execute(
                "UPDATE releases SET state='cancelled' WHERE release_id=?",
                (release_id,),
            )
            self.app.catalog.event(
                connection,
                release["logical_name"],
                "release_cancelled",
                {
                    "release_id": release_id,
                    "version": release["version"],
                    "reason": reason,
                },
            )
        return self.get(release_id)

    def _binding(self, connection, logical_name):
        return connection.execute(
            "SELECT * FROM release_bindings WHERE logical_name=?", (logical_name,)
        ).fetchone()

    def bind(self, logical_name, pipeline):
        """Point a logical name at an existing fixed pipeline.

        This is the entry point that lets an originally fixed pipeline become
        the version 0 target of later releases.
        """
        logical_name = identifier(logical_name, "logical name")
        pipeline = identifier(pipeline)
        with self.app.catalog.transaction(True) as connection:
            info = self.app.catalog.pipeline(pipeline, connection)
            now = utc_now()
            old = self._binding(connection, logical_name)
            if old:
                raise Conflict("logical name is already bound; publish a release instead")
            connection.execute(
                "INSERT INTO release_bindings VALUES (?,?,?,?,?,?)",
                (logical_name, pipeline, info["plan_hash"], None, 0, now),
            )
            self.app.catalog.event(
                connection,
                logical_name,
                "logical_bound",
                {"pipeline": pipeline, "plan_hash": info["plan_hash"]},
            )
            return dict(
                connection.execute(
                    "SELECT * FROM release_bindings WHERE logical_name=?",
                    (logical_name,),
                ).fetchone()
            )

    def current(self, logical_name):
        with self.app.catalog.transaction() as connection:
            row = self._binding(connection, logical_name)
            if row is None:
                raise Missing("logical name is not bound to a pipeline")
            return dict(row)

    def descriptor(self, release_id):
        """Self-contained switch instructions handed to a consumer.

        A consumer only needs this document (plus source catalog access) to
        replace its materialized state with the new version and resume from the
        matching changelog cursor.
        """
        with self.app.catalog.transaction() as connection:
            release = self.get(release_id, connection)
            if release["state"] != "published":
                raise Conflict("only published releases provide a switch descriptor")
            info = self.app.catalog.pipeline(
                release["shadow_pipeline"], connection
            )
            head = self.app.results.head(release["shadow_pipeline"], connection)
            rows = self.app.results.rows(
                release["shadow_pipeline"], include_deleted=True, connection=connection
            )
        return {
            "format_version": 1,
            "origin": self.app.catalog.identity(),
            "logical_name": release["logical_name"],
            "release_id": release["release_id"],
            "version": release["version"],
            "pipeline": release["shadow_pipeline"],
            "plan_hash": release["plan_hash"],
            "channel": release["channel"],
            "cutoff_sequence": release["cutoff_sequence"],
            "source_prefix_sha256": release["source_prefix_sha256"],
            "change_head": head,
            "buckets": info["buckets"],
            "hash_version": info["hash_version"],
            "rows": [
                {
                    "result_key": row["result_key"],
                    "revision": row["revision"],
                    "document": row["document"],
                    "deleted": row["deleted"],
                }
                for row in rows
            ],
        }

    def status(self, release_id):
        with self.app.catalog.transaction() as connection:
            release = self.get(release_id, connection)
            shadow = self.app.catalog.pipeline(
                release["shadow_pipeline"], connection
            )
            maximum = connection.execute(
                "SELECT coalesce(max(sequence),0) FROM journal WHERE channel=?",
                (release["channel"],),
            ).fetchone()[0]
            head = self.app.results.head(release["shadow_pipeline"], connection)
        return {
            **release,
            "shadow_last_sequence": shadow["last_sequence"],
            "journal_head": maximum,
            "caught_up": shadow["last_sequence"] == maximum,
            "change_head": head,
        }

    def list(self, logical_name=None):
        with self.app.catalog.transaction() as connection:
            if logical_name is None:
                rows = connection.execute(
                    "SELECT * FROM releases ORDER BY logical_name,version"
                )
            else:
                rows = connection.execute(
                    "SELECT * FROM releases WHERE logical_name=? ORDER BY version",
                    (logical_name,),
                )
            return [dict(row) for row in rows]
