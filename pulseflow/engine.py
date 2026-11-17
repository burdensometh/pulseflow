import json
import time
import uuid

from .spec import (
    CUTOFF_KEYS,
    JOIN_MODE_KEYS,
    PLAN_KNOWLEDGE_KEYS,
    call_knowledge_spec,
    normalize_join_mode,
    plan_spec,
)
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
    def __init__(
        self, catalog, journal, watermarks, windows, operators, leases, dimensions=None
    ):
        self.catalog = catalog
        self.journal = journal
        self.watermarks = watermarks
        self.windows = windows
        self.operators = operators
        self.leases = leases
        self.dimensions = dimensions

    def create(
        self, name, channel, plan, buckets=4, knowledge=None, mode=None, **options
    ):
        name = identifier(name)
        buckets = integer(buckets, "state buckets", 1, 128)
        call_cutoff = call_knowledge_spec(knowledge, options)
        plan_dimension = plan.get("dimension") if isinstance(plan, dict) else None
        plan_has_cutoff = isinstance(plan_dimension, dict) and any(
            key in plan_dimension and plan_dimension[key] is not None
            for key in CUTOFF_KEYS
        )
        if call_cutoff is not None or mode is not None:
            if not isinstance(plan_dimension, dict):
                raise Invalid("knowledge cutoff requires a dimension join plan")
            dimension = dict(plan_dimension)
            if call_cutoff is not None and (
                plan_has_cutoff
                or any(field in plan for field in PLAN_KNOWLEDGE_KEYS)
            ):
                raise Invalid("dimension knowledge cutoff was specified twice")
            if mode is not None:
                if any(field in dimension for field in JOIN_MODE_KEYS):
                    raise Invalid("dimension join mode was specified twice")
                mode_kind = normalize_join_mode(mode)
                if mode_kind == "realtime" and call_cutoff is None:
                    # Explicitly asking for realtime drops any plan cutoff,
                    # matching the historical live-join behavior.
                    for key in CUTOFF_KEYS:
                        dimension.pop(key, None)
                dimension["mode"] = mode
            if call_cutoff is not None:
                dimension["knowledge"] = call_cutoff
            plan = {**plan, "dimension": dimension}
        plan = plan_spec(plan)
        with self.catalog.transaction(True) as connection:
            source = self.catalog.channel(channel, connection)
            old = connection.execute(
                "SELECT * FROM pipelines WHERE name=?", (name,)
            ).fetchone()
            if old is not None:
                # A head-relative cutoff means "the knowledge captured when
                # this pipeline was created". A repeat create with the same
                # plan therefore reuses the originally pinned cutoff rather
                # than silently moving forward to newer knowledge.
                stored = json.loads(old["plan"])
                stored_dimension = stored.get("dimension", {})
                requested = plan.get("dimension", {})
                if (
                    old["channel"] == channel
                    and old["buckets"] == buckets
                    and requested.get("knowledge") == {"head": True}
                    and requested.get("name") == stored_dimension.get("name")
                    and "knowledge" in stored_dimension
                ):
                    candidate = {
                        **plan,
                        "dimension": {
                            **requested,
                            "knowledge": stored_dimension["knowledge"],
                        },
                    }
                    if fingerprint(candidate) == old["plan_hash"]:
                        return self.catalog.pipeline(name, connection)
            if "dimension" in plan and "knowledge" in plan["dimension"]:
                if self.dimensions is None:
                    raise Invalid("dimension knowledge cutoff is unavailable")
                # Resolve inside the write transaction so the pinned cutoff
                # is exactly the knowledge head observed by this commit.
                plan = self.dimensions.resolve_knowledge(plan, connection)
            checksum = fingerprint(plan)
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
            records = self.journal.read(
                info["channel"], info["last_sequence"], limit, connection=connection
            )
            before = info["last_sequence"]
            counts = {"accepted": 0, "late": 0, "filtered": 0, "duplicate": 0}
            for record in records:
                disposition = self.process(connection, info, record)
                counts[disposition] += 1
                info["last_sequence"] = record["sequence"]
                connection.execute(
                    "UPDATE pipelines SET last_sequence=? WHERE name=?",
                    (info["last_sequence"], pipeline),
                )
                self.watermarks.advance_in(connection, info)
                self.windows.close(connection, info)
                self.operators.expire(connection, info)
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
