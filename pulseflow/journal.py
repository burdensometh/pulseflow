import json

from .values import (
    Conflict,
    Invalid,
    canonical,
    event,
    fingerprint,
    identifier,
    integer,
    utc_now,
)


class Journal:
    def __init__(self, catalog):
        self.catalog = catalog

    def create(self, name, partitions):
        name = identifier(name)
        partitions = integer(partitions, "partitions", 1, 128)
        with self.catalog.transaction(True) as connection:
            old = connection.execute(
                "SELECT partitions FROM channels WHERE name=?", (name,)
            ).fetchone()
            if old:
                if old[0] != partitions:
                    raise Conflict("channel partition count is immutable")
                return self.catalog.channel(name, connection)
            connection.execute(
                "INSERT INTO channels VALUES (?,?,?)", (name, partitions, utc_now())
            )
            connection.executemany(
                "INSERT INTO source_positions VALUES (?,?,0)",
                ((name, p) for p in range(partitions)),
            )
            self.catalog.event(
                connection, name, "channel_created", {"partitions": partitions}
            )
        return self.catalog.channel(name)

    def positions(self, channel):
        with self.catalog.transaction() as connection:
            self.catalog.channel(channel, connection)
            rows = connection.execute(
                "SELECT partition_id,next_offset FROM source_positions WHERE channel=? ORDER BY partition_id",
                (channel,),
            )
            return {str(row[0]): row[1] for row in rows}

    def append(self, channel, batch_id, records):
        identifier(batch_id, "batch id")
        if not isinstance(records, list) or len(records) > 10000:
            raise Invalid("ingress requires at most 10000 records")
        digest = fingerprint(records)
        with self.catalog.transaction(True) as connection:
            return self.append_in(connection, channel, batch_id, records, digest)

    def append_in(self, connection, channel, batch_id, records, digest=None):
        info = self.catalog.channel(channel, connection)
        digest = digest or fingerprint(records)
        old = connection.execute(
            "SELECT * FROM ingress_receipts WHERE channel=? AND batch_id=?",
            (channel, batch_id),
        ).fetchone()
        if old:
            if old["fingerprint"] != digest:
                raise Conflict("ingress batch identity reused with different input")
            return {**dict(old), "replayed": True}
        positions = {
            row[0]: row[1]
            for row in connection.execute(
                "SELECT partition_id,next_offset FROM source_positions WHERE channel=?",
                (channel,),
            )
        }
        sequences = []
        accepted = 0
        for record in records:
            if not isinstance(record, dict) or set(record) != {
                "partition",
                "offset",
                "event",
            }:
                raise Invalid("record requires partition, offset and event")
            partition = integer(
                record["partition"], "partition", 0, info["partitions"] - 1
            )
            offset = integer(record["offset"], "source offset", 0)
            value = event(record["event"])
            checksum = fingerprint(value)
            previous = connection.execute(
                "SELECT * FROM journal WHERE channel=? AND partition_id=? AND source_offset=?",
                (channel, partition, offset),
            ).fetchone()
            if previous:
                if previous["checksum"] != checksum:
                    raise Conflict("source offset reused with different event")
                sequences.append(previous["sequence"])
                continue
            if offset != positions[partition]:
                raise Conflict("source partition requires contiguous offsets")
            identity = connection.execute(
                "SELECT partition_id,source_offset FROM journal WHERE channel=? AND event_id=?",
                (channel, value["id"]),
            ).fetchone()
            if identity:
                raise Conflict("event identity reused at a different source position")
            cursor = connection.execute(
                "INSERT INTO journal(channel,partition_id,source_offset,event_id,event_key,event_time,document,checksum) VALUES (?,?,?,?,?,?,?,?)",
                (
                    channel,
                    partition,
                    offset,
                    value["id"],
                    value["key"],
                    value["time"],
                    canonical(value),
                    checksum,
                ),
            )
            sequences.append(cursor.lastrowid)
            positions[partition] += 1
            accepted += 1
        for partition, offset in positions.items():
            connection.execute(
                "UPDATE source_positions SET next_offset=? WHERE channel=? AND partition_id=?",
                (offset, channel, partition),
            )
        result = {
            "channel": channel,
            "batch_id": batch_id,
            "fingerprint": digest,
            "accepted": accepted,
            "first_sequence": min(sequences) if sequences else None,
            "last_sequence": max(sequences) if sequences else None,
            "created_at": utc_now(),
        }
        connection.execute(
            "INSERT INTO ingress_receipts VALUES (?,?,?,?,?,?,?)",
            tuple(
                result[key]
                for key in [
                    "channel",
                    "batch_id",
                    "fingerprint",
                    "accepted",
                    "first_sequence",
                    "last_sequence",
                    "created_at",
                ]
            ),
        )
        self.catalog.event(connection, channel, "ingress_committed", result)
        return {**result, "replayed": False}

    def read(self, channel, after=0, limit=1000, partition=None, connection=None):
        integer(after, "after sequence", 0)
        integer(limit, "read limit", 1, 10000)
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.read(channel, after, limit, partition, connection)
        info = self.catalog.channel(channel, connection)
        sql = "SELECT * FROM journal WHERE channel=? AND sequence>?"
        values = [channel, after]
        if partition is not None:
            integer(partition, "partition", 0, info["partitions"] - 1)
            sql += " AND partition_id=?"
            values.append(partition)
        sql += " ORDER BY sequence LIMIT ?"
        values.append(limit)
        return [
            {**dict(row), "document": json.loads(row["document"])}
            for row in connection.execute(sql, values)
        ]

    def key_history(self, channel, key):
        with self.catalog.transaction() as connection:
            self.catalog.channel(channel, connection)
            rows = connection.execute(
                "SELECT * FROM journal WHERE channel=? AND event_key=? ORDER BY event_time,sequence",
                (channel, key),
            )
            return [
                {**dict(row), "document": json.loads(row["document"])} for row in rows
            ]

    def receipt(self, channel, batch_id):
        with self.catalog.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM ingress_receipts WHERE channel=? AND batch_id=?",
                (channel, batch_id),
            ).fetchone()
            return dict(row) if row else None

    def digest_prefix(self, connection, channel, sequence):
        rows = connection.execute(
            "SELECT partition_id,source_offset,checksum FROM journal WHERE channel=? AND sequence<=? ORDER BY sequence",
            (channel, sequence),
        )
        return fingerprint([list(row) for row in rows])
