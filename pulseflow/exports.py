import csv
import io
import json
import uuid

from .values import Conflict, Invalid, Missing, canonical, utc_now


class Exports:
    def __init__(self, catalog, results, artifacts):
        self.catalog = catalog
        self.results = results
        self.artifacts = artifacts

    def create(self, pipeline, format="jsonl"):
        if format not in {"jsonl", "csv"}:
            raise Invalid("result export format must be jsonl or csv")
        with self.catalog.transaction() as connection:
            rows = self.results.rows(pipeline, connection=connection)
            head = self.results.head(pipeline, connection)
        stream = io.StringIO(newline="")
        if format == "csv":
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(["result_key", "revision", "document"])
            for row in rows:
                writer.writerow(
                    [row["result_key"], row["revision"], canonical(row["document"])]
                )
        else:
            for row in rows:
                stream.write(
                    canonical(
                        {
                            "result_key": row["result_key"],
                            "revision": row["revision"],
                            "document": row["document"],
                        }
                    )
                    + "\n"
                )
        export_id = uuid.uuid4().hex
        artifact = self.artifacts.put(
            export_id + "." + format, stream.getvalue().encode("utf-8")
        )
        with self.catalog.transaction(True) as connection:
            connection.execute(
                "INSERT INTO dataset_exports VALUES (?,?,?,?,?,?,?)",
                (
                    export_id,
                    pipeline,
                    artifact["filename"],
                    artifact["sha256"],
                    len(rows),
                    head,
                    utc_now(),
                ),
            )
        return {
            "export_id": export_id,
            "pipeline": pipeline,
            "rows": len(rows),
            "head": head,
            **artifact,
        }

    def get(self, export_id):
        with self.catalog.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM dataset_exports WHERE export_id=?", (export_id,)
            ).fetchone()
            if row is None:
                raise Missing("result export does not exist")
            return dict(row)

    def read(self, export_id):
        item = self.get(export_id)
        return self.artifacts.read(item["filename"], item["sha256"])

    def decode(self, export_id):
        item = self.get(export_id)
        raw = self.read(export_id).decode("utf-8")
        if item["filename"].endswith(".jsonl"):
            return [json.loads(line) for line in raw.splitlines()]
        reader = csv.DictReader(io.StringIO(raw))
        if reader.fieldnames != ["result_key", "revision", "document"]:
            raise Conflict("export CSV header is not recognized")
        return [
            {
                "result_key": row["result_key"],
                "revision": int(row["revision"]),
                "document": json.loads(row["document"]),
            }
            for row in reader
        ]

    def list(self, pipeline):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            rows = connection.execute(
                "SELECT * FROM dataset_exports WHERE pipeline=? ORDER BY created_at,export_id",
                (pipeline,),
            )
            return [dict(row) for row in rows]
