from contextlib import contextmanager
from pathlib import Path
import json
import sqlite3

from .values import Conflict, Invalid, Missing, canonical, integer, utc_now


DDL = """
CREATE TABLE IF NOT EXISTS subscriptions (
 name TEXT PRIMARY KEY,
 origin TEXT NOT NULL,
 pipeline TEXT NOT NULL,
 plan_hash TEXT NOT NULL,
 cursor INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS materialized (
 subscription TEXT NOT NULL,
 result_key TEXT NOT NULL,
 revision INTEGER NOT NULL,
 document TEXT NOT NULL,
 deleted INTEGER NOT NULL,
 PRIMARY KEY(subscription,result_key)
);
CREATE TABLE IF NOT EXISTS applied_changes (
 subscription TEXT NOT NULL,
 source_sequence INTEGER NOT NULL,
 fingerprint TEXT NOT NULL,
 PRIMARY KEY(subscription,source_sequence)
);
CREATE TABLE IF NOT EXISTS switch_ledger (
 subscription TEXT NOT NULL,
 release_id TEXT NOT NULL,
 version INTEGER NOT NULL,
 pipeline TEXT NOT NULL,
 plan_hash TEXT NOT NULL,
 change_head INTEGER NOT NULL,
 PRIMARY KEY(subscription,release_id)
);
CREATE TABLE IF NOT EXISTS binding_history (
 subscription TEXT NOT NULL,
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 release_id TEXT,
 pipeline TEXT NOT NULL,
 plan_hash TEXT NOT NULL,
 cursor INTEGER NOT NULL,
 switched_at TEXT NOT NULL
);
"""


class Consumer:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as connection:
            connection.executescript(DDL)

    @contextmanager
    def transaction(self):
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=20)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def bind(self, connection, name, source, pipeline):
        identity = source.catalog.identity()
        info = source.catalog.pipeline(pipeline)
        old = connection.execute(
            "SELECT * FROM subscriptions WHERE name=?", (name,)
        ).fetchone()
        if old:
            if (old["origin"], old["pipeline"], old["plan_hash"]) != (
                identity,
                pipeline,
                info["plan_hash"],
            ):
                raise Conflict("subscription belongs to a different source")
            return dict(old)
        connection.execute(
            "INSERT INTO subscriptions VALUES (?,?,?,?,0)",
            (name, identity, pipeline, info["plan_hash"]),
        )
        return {
            "name": name,
            "origin": identity,
            "pipeline": pipeline,
            "plan_hash": info["plan_hash"],
            "cursor": 0,
        }

    def apply(self, source, pipeline, name="default", limit=1000):
        integer(limit, "consumer page size", 1, 10000)
        from .values import fingerprint

        with self.transaction() as destination:
            subscription = self.bind(destination, name, source, pipeline)
            with source.catalog.transaction() as connection:
                head = source.results.head(pipeline, connection)
                if subscription["cursor"] > head:
                    raise Conflict("source changelog moved behind consumer cursor")
                changes = source.results.changes(
                    pipeline, subscription["cursor"], limit, head, connection
                )
            for change in changes:
                old = destination.execute(
                    "SELECT * FROM materialized WHERE subscription=? AND result_key=?",
                    (name, change["result_key"]),
                ).fetchone()
                if old and change["revision"] <= old["revision"]:
                    raise Conflict("source returned a nonincreasing result revision")
                destination.execute(
                    "INSERT INTO materialized VALUES (?,?,?,?,?) ON CONFLICT(subscription,result_key) DO UPDATE SET revision=excluded.revision,document=excluded.document,deleted=excluded.deleted",
                    (
                        name,
                        change["result_key"],
                        change["revision"],
                        canonical(change["document"]),
                        int(change["operation"] == "delete"),
                    ),
                )
                destination.execute(
                    "INSERT INTO applied_changes VALUES (?,?,?)",
                    (name, change["sequence"], fingerprint(change)),
                )
            cursor = changes[-1]["sequence"] if changes else subscription["cursor"]
            destination.execute(
                "UPDATE subscriptions SET cursor=? WHERE name=?", (cursor, name)
            )
        return {
            "subscription": name,
            "applied": len(changes),
            "cursor": cursor,
            "head": head,
        }

    def catch_up(self, source, pipeline, name="default", limit=1000):
        total = 0
        while True:
            result = self.apply(source, pipeline, name, limit)
            total += result["applied"]
            if result["applied"] < limit:
                return {**result, "applied": total}

    def bootstrap(self, source, pipeline, name="default"):
        with self.transaction() as destination:
            old = self.bind(destination, name, source, pipeline)
            with source.catalog.transaction() as connection:
                head = source.results.head(pipeline, connection)
                rows = source.results.rows(
                    pipeline, include_deleted=True, connection=connection
                )
            if old["cursor"] > head:
                raise Conflict("cannot bootstrap from older source state")
            destination.execute(
                "DELETE FROM materialized WHERE subscription=?", (name,)
            )
            destination.execute(
                "DELETE FROM applied_changes WHERE subscription=?", (name,)
            )
            destination.executemany(
                "INSERT INTO materialized VALUES (?,?,?,?,?)",
                (
                    (
                        name,
                        row["result_key"],
                        row["revision"],
                        canonical(row["document"]),
                        row["deleted"],
                    )
                    for row in rows
                ),
            )
            destination.execute(
                "UPDATE subscriptions SET cursor=? WHERE name=?", (head, name)
            )
        return {"subscription": name, "cursor": head, "rows": len(rows)}

    def _verify_descriptor(self, source, descriptor):
        """Validate a release descriptor against the live source catalog."""
        if not isinstance(descriptor, dict) or descriptor.get("format_version") != 1:
            raise Invalid("unsupported release descriptor")
        required = {
            "format_version",
            "origin",
            "logical_name",
            "release_id",
            "version",
            "pipeline",
            "plan_hash",
            "channel",
            "cutoff_sequence",
            "source_prefix_sha256",
            "change_head",
            "rows",
        }
        if set(descriptor) < required:
            raise Invalid("release descriptor is missing fields")
        if descriptor["origin"] != source.catalog.identity():
            raise Conflict("release belongs to a different source instance")
        integer(descriptor["version"], "release version", 1)
        integer(descriptor["change_head"], "release change head", 0)
        integer(descriptor["cutoff_sequence"], "release cutoff", 0)
        with source.catalog.transaction() as connection:
            info = source.catalog.pipeline(descriptor["pipeline"], connection)
            if info["plan_hash"] != descriptor["plan_hash"]:
                raise Conflict("release plan fingerprint does not match the pipeline")
            head = source.results.head(descriptor["pipeline"], connection)
            if head < descriptor["change_head"]:
                raise Conflict("source changelog moved behind the release cursor")
            actual_prefix = source.journal.digest_prefix(
                connection, descriptor["channel"], descriptor["cutoff_sequence"]
            )
        if actual_prefix != descriptor["source_prefix_sha256"]:
            raise Conflict("release source prefix fingerprint does not match")
        return descriptor

    def promote(self, source, descriptor, name="default"):
        """Switch one subscription onto a published release.

        The whole materialized result is replaced inside a single transaction
        and the cursor is reset to the release changelog position, so no old
        entry can survive, no appended event is lost and no change is applied
        twice. Repeating the same release is a no-op; an older release is
        rejected once a newer one has been applied.
        """
        descriptor = self._verify_descriptor(source, descriptor)
        with self.transaction() as destination:
            subscription = destination.execute(
                "SELECT * FROM subscriptions WHERE name=?", (name,)
            ).fetchone()
            if subscription is None:
                raise Missing("subscription does not exist; consume a pipeline first")
            if subscription["origin"] != descriptor["origin"]:
                raise Conflict("subscription belongs to a different source")
            prior = destination.execute(
                "SELECT max(version) AS version FROM switch_ledger WHERE subscription=?",
                (name,),
            ).fetchone()
            if prior["version"] is not None:
                if descriptor["version"] < prior["version"]:
                    raise Conflict(
                        "stale release cannot replace a newer applied version"
                    )
                already = destination.execute(
                    "SELECT 1 FROM switch_ledger WHERE subscription=? AND release_id=?",
                    (name, descriptor["release_id"]),
                ).fetchone()
                if already:
                    return {
                        "subscription": name,
                        "release_id": descriptor["release_id"],
                        "version": descriptor["version"],
                        "replayed": True,
                        "rows": destination.execute(
                            "SELECT count(*) FROM materialized WHERE subscription=?",
                            (name,),
                        ).fetchone()[0],
                        "cursor": subscription["cursor"],
                    }
            destination.execute(
                "DELETE FROM materialized WHERE subscription=?", (name,)
            )
            destination.execute(
                "DELETE FROM applied_changes WHERE subscription=?", (name,)
            )
            destination.executemany(
                "INSERT INTO materialized VALUES (?,?,?,?,?)",
                (
                    (
                        name,
                        row["result_key"],
                        integer(row["revision"], "release result revision", 1),
                        canonical(row["document"]),
                        int(bool(row["deleted"])),
                    )
                    for row in descriptor["rows"]
                ),
            )
            cursor = descriptor["change_head"]
            destination.execute(
                "UPDATE subscriptions SET pipeline=?,plan_hash=?,cursor=? WHERE name=?",
                (
                    descriptor["pipeline"],
                    descriptor["plan_hash"],
                    cursor,
                    name,
                ),
            )
            destination.execute(
                "INSERT INTO switch_ledger VALUES (?,?,?,?,?,?)",
                (
                    name,
                    descriptor["release_id"],
                    descriptor["version"],
                    descriptor["pipeline"],
                    descriptor["plan_hash"],
                    cursor,
                ),
            )
            destination.execute(
                "INSERT INTO binding_history"
                "(subscription,release_id,pipeline,plan_hash,cursor,switched_at) "
                "VALUES (?,?,?,?,?,?)",
                (
                    name,
                    descriptor["release_id"],
                    descriptor["pipeline"],
                    descriptor["plan_hash"],
                    cursor,
                    utc_now(),
                ),
            )
        return {
            "subscription": name,
            "release_id": descriptor["release_id"],
            "version": descriptor["version"],
            "replayed": False,
            "rows": len(descriptor["rows"]),
            "cursor": cursor,
        }

    def switches(self, name="default"):
        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM binding_history WHERE subscription=? ORDER BY sequence",
                (name,),
            )
            return [dict(row) for row in rows]

    def rows(self, name="default", include_deleted=False):
        with self.transaction() as connection:
            sql = "SELECT * FROM materialized WHERE subscription=?"
            if not include_deleted:
                sql += " AND deleted=0"
            sql += " ORDER BY result_key"
            return [
                {**dict(row), "document": json.loads(row["document"])}
                for row in connection.execute(sql, (name,))
            ]

    def status(self, name="default"):
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM subscriptions WHERE name=?", (name,)
            ).fetchone()
            return dict(row) if row else None