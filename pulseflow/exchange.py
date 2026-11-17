import uuid

from .values import Conflict, Invalid, event, fingerprint, identifier, integer


class JournalExchange:
    def __init__(self, catalog, journal, artifacts):
        self.catalog = catalog
        self.journal = journal
        self.artifacts = artifacts

    def export(self, channel, limit=10000, compressed=True):
        integer(limit, "exchange record limit", 1, 10000)
        with self.catalog.transaction() as connection:
            source = self.catalog.channel(channel, connection)
            count = connection.execute(
                "SELECT count(*) FROM journal WHERE channel=?", (channel,)
            ).fetchone()[0]
            if count > limit:
                raise Invalid("channel exceeds atomic exchange record limit")
            records = self.journal.read(channel, 0, limit, connection=connection)
            positions = {
                str(row[0]): row[1]
                for row in connection.execute(
                    "SELECT partition_id,next_offset FROM source_positions WHERE channel=?",
                    (channel,),
                )
            }
        rows = [
            {
                "partition": row["partition_id"],
                "offset": row["source_offset"],
                "event": row["document"],
            }
            for row in records
        ]
        document = {
            "format_version": 1,
            "channel": channel,
            "partitions": source["partitions"],
            "positions": positions,
            "rows": rows,
            "rows_sha256": fingerprint(rows),
        }
        self.validate(document)
        name = "journal-" + uuid.uuid4().hex + (".json.gz" if compressed else ".json")
        artifact = self.artifacts.put_document(name, document, compressed)
        return {**artifact, "records": len(rows), "compressed": compressed}

    def validate(self, document):
        if not isinstance(document, dict) or set(document) != {
            "format_version",
            "channel",
            "partitions",
            "positions",
            "rows",
            "rows_sha256",
        }:
            raise Invalid("invalid journal exchange envelope")
        if document["format_version"] != 1:
            raise Invalid("unsupported journal exchange format")
        identifier(document["channel"])
        partitions = integer(document["partitions"], "source partitions", 1, 128)
        rows = document["rows"]
        if not isinstance(rows, list) or len(rows) > 10000:
            raise Invalid("exchange rows exceed atomic limit")
        if fingerprint(rows) != document["rows_sha256"]:
            raise Conflict("journal exchange row checksum mismatch")
        positions = {str(p): 0 for p in range(partitions)}
        identities = set()
        for record in rows:
            if not isinstance(record, dict) or set(record) != {
                "partition",
                "offset",
                "event",
            }:
                raise Invalid("invalid journal exchange record")
            partition = integer(
                record["partition"], "source partition", 0, partitions - 1
            )
            offset = integer(record["offset"], "source offset", 0)
            if offset != positions[str(partition)]:
                raise Conflict("journal exchange is not a complete partition prefix")
            value = event(record["event"])
            if value != record["event"]:
                raise Conflict("journal exchange event is not canonical")
            if value["id"] in identities:
                raise Conflict("journal exchange repeats an event identity")
            identities.add(value["id"])
            positions[str(partition)] += 1
        if positions != document["positions"]:
            raise Conflict("journal exchange position summary mismatch")
        return document

    def import_document(self, document, destination, batch_id):
        self.validate(document)
        destination = identifier(destination)
        identifier(batch_id)
        with self.catalog.transaction(True) as connection:
            channel = self.catalog.channel(destination, connection)
            if channel["partitions"] != document["partitions"]:
                raise Conflict("destination partition layout differs")
            return self.journal.append_in(
                connection, destination, batch_id, document["rows"]
            )

    def import_artifact(self, filename, checksum, destination, batch_id):
        document = self.artifacts.read_document(
            filename, checksum, filename.endswith(".gz")
        )
        return self.import_document(document, destination, batch_id)
