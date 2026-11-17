import json

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
                return {"versions": json.loads(old["versions"]), "replayed": True}
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
            self.catalog.event(
                connection,
                "dimensions",
                "versions_published",
                {"batch": batch_id, "versions": ids},
            )
        return {"versions": ids, "replayed": False}

    def lookup(self, dimension, key, at, connection=None):
        at = millis(at)
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.lookup(dimension, key, at, connection)
        row = connection.execute(
            "SELECT * FROM dimension_versions WHERE dimension_name=? AND entity_key=? AND valid_from<=? ORDER BY valid_from DESC,revision DESC LIMIT 1",
            (dimension, key, at),
        ).fetchone()
        if row is None or row["deleted"]:
            return None
        return {**dict(row), "document": json.loads(row["document"])}

    def history(self, dimension, key):
        with self.catalog.transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM dimension_versions WHERE dimension_name=? AND entity_key=? ORDER BY valid_from,revision",
                (dimension, key),
            )
            return [
                {**dict(row), "document": json.loads(row["document"])} for row in rows
            ]

    def enrich(self, connection, row, spec):
        version = self.lookup(spec["name"], row["key"], row["time"], connection)
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

    def intervals(self, dimension, key):
        versions = self.history(dimension, key)
        winners = {}
        for row in versions:
            winners[row["valid_from"]] = row
        times = sorted(winners)
        return [
            {**winners[start], "valid_to": times[i + 1] if i + 1 < len(times) else None}
            for i, start in enumerate(times)
        ]
