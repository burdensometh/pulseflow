import json

from .spec import cutoff_from, knowledge_spec
from .values import (
    Conflict,
    Invalid,
    canonical,
    fingerprint,
    identifier,
    integer,
    json_value,
    millis,
)


class Dimensions:
    def __init__(self, catalog):
        self.catalog = catalog

    def publish(self, batch_id, records):
        identifier(batch_id)
        if not isinstance(records, list) or len(records) > 10000:
            raise Invalid("dimension batch supports at most 10000 versions")
        checksum = fingerprint(records)
        with self.catalog.transaction(True) as connection:
            old = connection.execute(
                "SELECT * FROM dimension_receipts WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if old:
                if old["fingerprint"] != checksum:
                    raise Conflict("dimension batch id reused")
                return {
                    "versions": json.loads(old["versions"]),
                    "replayed": True,
                    "cutoff": self.cutoff_for_batch(batch_id, connection),
                }
            ids = []
            for value in records:
                if not isinstance(value, dict) or set(value) - {
                    "dimension",
                    "key",
                    "valid_from",
                    "revision",
                    "attrs",
                    "deleted",
                }:
                    raise Invalid("invalid dimension version")
                dimension = identifier(value.get("dimension"))
                key = value.get("key")
                if not isinstance(key, str) or not key:
                    raise Invalid("dimension key is required")
                valid = millis(value.get("valid_from"))
                revision = integer(value.get("revision"), "dimension revision", 1)
                attrs = json_value(value.get("attrs", {}))
                if (
                    not isinstance(attrs, dict)
                    or type(value.get("deleted", False)) is not bool
                ):
                    raise Invalid("dimension attrs/deletion flag is invalid")
                encoded = canonical(attrs)
                deleted = int(value.get("deleted", False))
                previous = connection.execute(
                    "SELECT * FROM dimension_versions WHERE dimension_name=? AND entity_key=? AND valid_from=? AND revision=?",
                    (dimension, key, valid, revision),
                ).fetchone()
                if previous:
                    if (
                        previous["document"] != encoded
                        or previous["deleted"] != deleted
                    ):
                        raise Conflict(
                            "same dimension identity has conflicting content"
                        )
                    ids.append(previous["version_id"])
                    continue
                cursor = connection.execute(
                    "INSERT INTO dimension_versions(dimension_name,entity_key,valid_from,revision,document,deleted) VALUES (?,?,?,?,?,?)",
                    (dimension, key, valid, revision, encoded, deleted),
                )
                ids.append(cursor.lastrowid)
            connection.execute(
                "INSERT INTO dimension_receipts VALUES (?,?,?)",
                (batch_id, checksum, canonical(ids)),
            )
            cutoff = self.head(connection)
            connection.execute(
                "INSERT INTO dimension_knowledge VALUES (?,?)", (batch_id, cutoff)
            )
            self.catalog.event(
                connection,
                "dimensions",
                "versions_published",
                {"batch": batch_id, "versions": ids},
            )
            return {"versions": ids, "replayed": False, "cutoff": cutoff}

    def head(self, connection=None):
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.head(connection)
        return connection.execute(
            "SELECT coalesce(max(version_id),0) FROM dimension_versions"
        ).fetchone()[0]

    def cutoff_for_batch(self, batch_id, connection=None):
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.cutoff_for_batch(batch_id, connection)
        identifier(batch_id, "dimension batch id")
        row = connection.execute(
            "SELECT cutoff FROM dimension_knowledge WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if row is not None:
            return row[0]
        legacy = connection.execute(
            "SELECT versions FROM dimension_receipts WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if legacy is None:
            raise Invalid("dimension knowledge batch does not exist")
        return max(json.loads(legacy[0]), default=0)

    def cutoff(self, batch_id=None):
        """Return the knowledge cutoff for a batch, or the current head.

        The returned integer is the inclusive ``version_id`` ceiling a
        pipeline can name as its knowledge cutoff.
        """
        if batch_id is None:
            return self.head()
        return self.cutoff_for_batch(batch_id)

    def resolve_knowledge(self, plan, connection=None):
        """Resolve a named cutoff to its immutable, inclusive version id.

        ``version`` is already immutable, ``batch`` resolves through the
        cutoff captured when that batch committed, and ``head`` pins the
        knowledge head observed at pipeline creation. The resolved plan
        always stores a concrete version, so every chunk, savepoint restore
        and replay of that pipeline sees exactly the same knowledge.
        """
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.resolve_knowledge(plan, connection)
        if not isinstance(plan, dict) or not isinstance(plan.get("dimension"), dict):
            return plan
        dimension = plan["dimension"]
        if "knowledge" not in dimension:
            return plan
        knowledge = knowledge_spec(dimension["knowledge"])
        if "batch" in knowledge:
            cutoff = self.cutoff_for_batch(knowledge["batch"], connection)
        elif "head" in knowledge:
            cutoff = self.head(connection)
        else:
            cutoff = knowledge["version"]
        if cutoff > self.head(connection):
            raise Invalid("dimension knowledge cutoff exceeds known versions")
        return {
            **plan,
            "dimension": {**dimension, "knowledge": {"version": cutoff}},
        }

    def _cutoff(self, knowledge, connection):
        if knowledge is None:
            return None
        knowledge = knowledge_spec(knowledge)
        if "batch" in knowledge:
            cutoff = self.cutoff_for_batch(knowledge["batch"], connection)
        elif "head" in knowledge:
            cutoff = self.head(connection)
        else:
            cutoff = knowledge["version"]
        if cutoff > self.head(connection):
            raise Invalid("dimension knowledge cutoff exceeds known versions")
        return cutoff

    def _lookup(self, connection, dimension, key, at, cutoff):
        sql = (
            "SELECT * FROM dimension_versions WHERE dimension_name=? AND entity_key=? AND valid_from<=?"
        )
        values = [dimension, key, at]
        if cutoff is not None:
            sql += " AND version_id<=?"
            values.append(cutoff)
        sql += " ORDER BY valid_from DESC,revision DESC LIMIT 1"
        row = connection.execute(sql, values).fetchone()
        if row is None or row["deleted"]:
            return None
        return {**dict(row), "document": json.loads(row["document"])}

    def _requested_cutoff(self, cutoff, knowledge, options):
        """Merge the accepted cutoff argument spellings into one value."""
        selected = cutoff_from(options) if options else None
        if selected is not None:
            if knowledge is not None or cutoff is not None:
                raise Invalid("dimension knowledge cutoff was specified more than once")
            cutoff = selected
        if knowledge is not None:
            if cutoff is not None:
                raise Invalid("dimension knowledge cutoff was specified more than once")
            cutoff = knowledge
        return cutoff

    def lookup(
        self, dimension, key, at, connection=None, cutoff=None, knowledge=None, **options
    ):
        at = millis(at)
        cutoff = self._requested_cutoff(cutoff, knowledge, options)
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.lookup(dimension, key, at, connection, cutoff)
        cutoff = self._cutoff(cutoff, connection)
        return self._lookup(connection, dimension, key, at, cutoff)

    def history(self, dimension, key, cutoff=None, knowledge=None, **options):
        cutoff = self._requested_cutoff(cutoff, knowledge, options)
        with self.catalog.transaction() as connection:
            cutoff = self._cutoff(cutoff, connection)
            sql = (
                "SELECT * FROM dimension_versions WHERE dimension_name=? AND entity_key=?"
            )
            values = [dimension, key]
            if cutoff is not None:
                sql += " AND version_id<=?"
                values.append(cutoff)
            sql += " ORDER BY valid_from,revision,version_id"
            rows = connection.execute(sql, values)
            return [
                {**dict(row), "document": json.loads(row["document"])} for row in rows
            ]

    def enrich(self, connection, row, spec):
        cutoff = self._cutoff(spec.get("knowledge"), connection)
        version = self._lookup(
            connection, spec["name"], row["key"], row["time"], cutoff
        )
        if version is None:
            if spec["missing"] == "error":
                raise Invalid("dimension version is missing for event time")
            if spec["missing"] == "drop":
                return None, None
        result = {
            **row,
            "attrs": {
                **row["attrs"],
                "dimension": version["document"] if version else None,
            },
        }
        return result, version["version_id"] if version else None

    def intervals(self, dimension, key, cutoff=None, knowledge=None, **options):
        versions = self.history(dimension, key, cutoff, knowledge, **options)
        winners = {}
        for row in versions:
            winners[row["valid_from"]] = row
        times = sorted(winners)
        return [
            {**winners[start], "valid_to": times[i + 1] if i + 1 < len(times) else None}
            for i, start in enumerate(times)
        ]
