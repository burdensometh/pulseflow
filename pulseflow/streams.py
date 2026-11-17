from pathlib import Path
import hashlib
import json

from .values import Conflict, Invalid, fingerprint, identifier, integer


class FileIngress:
    def __init__(self, catalog, journal):
        self.catalog = catalog
        self.journal = journal

    def tail(self, source_id, channel, partition, path, max_records=1000):
        source_id = identifier(source_id)
        integer(max_records, "tail record limit", 1, 10000)
        path = Path(path)
        if not path.is_file() or path.stat().st_size > 64 * 1024 * 1024:
            raise Invalid("input must be a regular file of at most 64 MiB")
        raw = path.read_bytes()
        with self.catalog.transaction(True) as connection:
            info = self.catalog.channel(channel, connection)
            integer(partition, "partition", 0, info["partitions"] - 1)
            receipt = connection.execute(
                "SELECT * FROM file_receipts WHERE source_id=?",
                (source_id,),
            ).fetchone()
            if receipt:
                if (receipt["channel"], receipt["partition_id"]) != (
                    channel,
                    partition,
                ):
                    raise Conflict("file source identity is bound to another partition")
                offset = receipt["byte_offset"]
                checksum = hashlib.sha256(raw[:offset]).hexdigest()
                if len(raw) < offset or checksum != receipt["prefix_sha256"]:
                    raise Conflict(
                        "already ingested file prefix changed or was truncated"
                    )
                next_offset = receipt["next_offset"]
            else:
                offset = 0
                next_offset = connection.execute(
                    "SELECT next_offset FROM source_positions WHERE channel=? AND partition_id=?",
                    (channel, partition),
                ).fetchone()[0]
            start = offset
            records = []
            while len(records) < max_records:
                end = raw.find(b"\n", offset)
                if end == -1:
                    break
                line = raw[offset:end]
                if not line.strip():
                    raise Invalid("blank JSONL records are not accepted")
                try:
                    document = json.loads(line)
                except (ValueError, UnicodeError) as error:
                    raise Invalid("invalid complete JSONL record") from error
                records.append(
                    {
                        "partition": partition,
                        "offset": next_offset + len(records),
                        "event": document,
                    }
                )
                offset = end + 1
            if not records:
                return {
                    "source_id": source_id,
                    "records": 0,
                    "byte_offset": offset,
                    "incomplete_bytes": len(raw) - offset,
                }
            input_hash = fingerprint(records)
            batch = "file." + fingerprint([source_id, start, offset, input_hash])[:64]
            result = self.journal.append_in(
                connection, channel, batch, records, input_hash
            )
            connection.execute(
                "INSERT INTO file_receipts VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(source_id) DO UPDATE SET "
                "byte_offset=excluded.byte_offset,prefix_sha256=excluded.prefix_sha256,"
                "next_offset=excluded.next_offset",
                (
                    source_id,
                    channel,
                    partition,
                    offset,
                    hashlib.sha256(raw[:offset]).hexdigest(),
                    next_offset + len(records),
                ),
            )
            return {
                "source_id": source_id,
                "records": len(records),
                "byte_offset": offset,
                "incomplete_bytes": len(raw) - offset,
                "ingress": result,
            }

    def status(self, source_id):
        with self.catalog.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM file_receipts WHERE source_id=?", (source_id,)
            ).fetchone()
            return dict(row) if row else None

    def import_frozen(self, source_id, channel, partition, path, chunk_size=1000):
        path = Path(path)
        if not path.is_file():
            raise Invalid("frozen input does not exist")
        before = hashlib.sha256(path.read_bytes()).hexdigest()
        total = 0
        while True:
            if hashlib.sha256(path.read_bytes()).hexdigest() != before:
                raise Conflict("frozen input changed during import")
            result = self.tail(source_id, channel, partition, path, chunk_size)
            total += result["records"]
            if result["records"] < chunk_size:
                break
        if result["incomplete_bytes"]:
            raise Invalid("frozen input must end at a complete newline")
        return {
            "source_id": source_id,
            "records": total,
            "input_sha256": before,
            "status": self.status(source_id),
        }
