import json
import time
import uuid

from .spec import plan_spec
from .values import (
    Conflict,
    Invalid,
    canonical,
    fingerprint,
    identifier,
    integer,
    utc_now,
)


class Engine:
    def __init__(self, catalog, journal, watermarks, windows, operators, leases, joins):
        self.catalog = catalog
        self.journal = journal
        self.watermarks = watermarks
        self.windows = windows
        self.operators = operators
        self.leases = leases
        self.joins = joins

    def create(self, name, channel, plan, buckets=4):
        name = identifier(name)
        buckets = integer(buckets, "state buckets", 1, 128)
        plan = plan_spec(plan)
        checksum = fingerprint(plan)
        with self.catalog.transaction(True) as connection:
            if "join" in plan and channel != plan["join"]["left_channel"]:
                raise Invalid("join pipeline channel must be the left join channel")
            source = self.catalog.channel(channel, connection)
            old = connection.execute(
                "SELECT * FROM pipelines WHERE name=?", (name,)
            ).fetchone()
            if old:
                if (old["channel"], old["plan_hash"], old["buckets"]) != (
                    channel,
                    checksum,
                    buckets,
                ):
                    raise Conflict(
                        "pipeline identity is already bound to another plan or state layout"
                    )
                return self.catalog.pipeline(name, connection)
            connection.execute(
                "INSERT INTO pipelines VALUES (?,?,?,?,?,?,NULL,0,0,?)",
                (name, channel, canonical(plan), checksum, buckets, 1, utc_now()),
            )
            if "join" in plan:
                self.joins.initialize(connection, name, plan["join"])
            else:
                connection.executemany(
                    "INSERT INTO progress VALUES (?,?,0,NULL,NULL)",
                    ((name, p) for p in range(source["partitions"])),
                )
            self.catalog.event(
                connection,
                name,
                "pipeline_created",
                {"channel": channel, "plan_hash": checksum},
            )
        return self.catalog.pipeline(name)

    def step(self, pipeline, lease, now_ms, limit=1000):
        integer(limit, "processing chunk", 1, 10000)
        if lease["pipeline"] != pipeline:
            raise Conflict("worker lease belongs to another pipeline")
        with self.catalog.transaction(True) as connection:
            self.leases.check(connection, lease, now_ms)
            info = self.catalog.pipeline(pipeline, connection)
            join = "join" in info["plan"]
            if join:
                self.joins.load(connection, info)
                records = self.joins.read_inputs(connection, info, limit)
            else:
                records = self.journal.read(
                    info["channel"], info["last_sequence"], limit, connection=connection
                )
            before = info["last_sequence"]
            counts = {"accepted": 0, "late": 0, "filtered": 0, "duplicate": 0}
            finalized = {"unmatched": 0, "expired_left": 0, "expired_right": 0}
            for record in records:
                if join:
                    disposition = self.joins.process(connection, info, record)
                else:
                    disposition = self.process(connection, info, record)
                counts[disposition] += 1
                info["last_sequence"] = record["sequence"]
                connection.execute(
                    "UPDATE pipelines SET last_sequence=? WHERE name=?",
                    (info["last_sequence"], pipeline),
                )
                if join:
                    self.joins.advance(connection, info)
                    step_done = self.joins.finalize(connection, info)
                    for key, value in step_done.items():
                        finalized[key] += value
                else:
                    self.watermarks.advance_in(connection, info)
                    self.windows.close(connection, info)
                    self.operators.expire(connection, info)
            if join:
                self.joins.advance(connection, info)
                step_done = self.joins.finalize(connection, info)
                for key, value in step_done.items():
                    finalized[key] += value
                closed = 0
            else:
                self.watermarks.advance_in(connection, info)
                closed = self.windows.close(connection, info)
                self.operators.expire(connection, info)
            generation = info["generation"] + 1
            connection.execute(
                "UPDATE pipelines SET generation=? WHERE name=?", (generation, pipeline)
            )
            run_id = uuid.uuid4().hex
            details = {
                **counts,
                "watermark": info["watermark"],
                "windows_closed_at_flush": closed,
            }
            if join:
                details["join_watermarks"] = dict(info["join_watermarks"])
                details.update(finalized)
            connection.execute(
                "INSERT INTO engine_runs VALUES (?,?,?,?,?,?,?)",
                (
                    run_id,
                    pipeline,
                    before,
                    info["last_sequence"],
                    len(records),
                    canonical(details),
                    utc_now(),
                ),
            )
            return {
                "run_id": run_id,
                "pipeline": pipeline,
                "processed": len(records),
                "last_sequence": info["last_sequence"],
                "generation": generation,
                **details,
            }

    def process(self, connection, pipeline, record):
        row = record["document"]
        watermark = pipeline["watermark"]
        transformed = None
        dimension_version = None
        if watermark is not None and row["time"] < watermark:
            disposition = "late"
            connection.execute(
                "INSERT INTO late_events VALUES (?,?,?,?,?,?)",
                (
                    pipeline["name"],
                    row["id"],
                    record["sequence"],
                    row["time"],
                    watermark,
                    canonical(row),
                ),
            )
        else:
            transformed, dimension_version = self.operators.transform(
                connection, pipeline, row
            )
            if transformed is None:
                disposition = "filtered"
            elif self.operators.duplicate(connection, pipeline, transformed):
                disposition = "duplicate"
            else:
                disposition = "accepted"
                self.windows.add(connection, pipeline, transformed)
                self.operators.latest(connection, pipeline, transformed)
                self.operators.sequence(connection, pipeline, transformed)
        connection.execute(
            "INSERT INTO processed_events VALUES (?,?,?,?,?,?)",
            (
                pipeline["name"],
                row["id"],
                record["sequence"],
                disposition,
                canonical(transformed) if transformed is not None else None,
                dimension_version,
            ),
        )
        self.watermarks.observe(
            connection,
            pipeline,
            record["partition_id"],
            record["source_offset"],
            row["time"],
        )
        return disposition

    def drain(self, pipeline, limit=1000, now_ms=None):
        dynamic_clock = now_ms is None
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        lease = self.leases.acquire(pipeline, uuid.uuid4().hex, now_ms)
        runs = []
        try:
            while True:
                result = self.step(pipeline, lease, now_ms, limit)
                runs.append(result)
                if result["processed"] < limit:
                    break
                current = int(time.time() * 1000) if dynamic_clock else now_ms
                lease = self.leases.renew(lease, current)
                now_ms = current
        finally:
            self.leases.release(lease, now_ms)
        return {
            "pipeline": pipeline,
            "processed": sum(run["processed"] for run in runs),
            "runs": runs,
        }

    def status(self, pipeline):
        with self.catalog.transaction() as connection:
            info = self.catalog.pipeline(pipeline, connection)
            if "join" in info["plan"]:
                spec = info["plan"]["join"]
                source_count = connection.execute(
                    "SELECT count(*) FROM journal WHERE channel IN (?,?)",
                    (spec["left_channel"], spec["right_channel"]),
                ).fetchone()[0]
            else:
                source_count = connection.execute(
                    "SELECT count(*) FROM journal WHERE channel=?", (info["channel"],)
                ).fetchone()[0]
            processed = connection.execute(
                "SELECT disposition,count(*) FROM processed_events WHERE pipeline=? GROUP BY disposition",
                (pipeline,),
            )
            counts = {row[0]: row[1] for row in processed}
            return {
                **info,
                "source_events": source_count,
                "processed_events": sum(counts.values()),
                "dispositions": counts,
                "pending": source_count - sum(counts.values()),
            }

    def runs(self, pipeline):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            rows = connection.execute(
                "SELECT * FROM engine_runs WHERE pipeline=? ORDER BY rowid", (pipeline,)
            )
            return [
                {**dict(row), "details": json.loads(row["details"])} for row in rows
            ]

    def late(self, pipeline):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            rows = connection.execute(
                "SELECT * FROM late_events WHERE pipeline=? ORDER BY source_sequence",
                (pipeline,),
            )
            return [
                {**dict(row), "document": json.loads(row["document"])} for row in rows
            ]
