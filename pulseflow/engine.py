from contextlib import contextmanager
from itertools import count
import json
import time
import uuid

from .spec import plan_spec
from .values import (
    Conflict,
    Invalid,
    Missing,
    canonical,
    event,
    fingerprint,
    identifier,
    integer,
    utc_now,
)


CORRECTION_SIMPLE = {"value", "key", "kind", "attrs"}
CORRECTION_PREFIX = "attrs."
FAULT_ERRORS = (Invalid, ArithmeticError)


class Engine:
    def __init__(self, catalog, journal, watermarks, windows, operators, leases):
        self.catalog = catalog
        self.journal = journal
        self.watermarks = watermarks
        self.windows = windows
        self.operators = operators
        self.leases = leases

    def create(self, name, channel, plan, buckets=4):
        name = identifier(name)
        buckets = integer(buckets, "state buckets", 1, 128)
        plan = plan_spec(plan)
        checksum = fingerprint(plan)
        with self.catalog.transaction(True) as connection:
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
            mode = self.mode(info)
            counts = {
                "accepted": 0,
                "late": 0,
                "filtered": 0,
                "duplicate": 0,
                "faulted": 0,
                "held": 0,
            }
            scope = count()
            for record in records:
                disposition = self.consume(connection, info, record, mode, scope)
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
                "error_mode": mode,
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

    @staticmethod
    def mode(pipeline):
        return pipeline["plan"].get("error_mode", "rollback")

    @contextmanager
    def _record_scope(self, connection, token):
        name = "pfrecord" + str(token)
        connection.execute("SAVEPOINT " + name)
        try:
            yield
        except BaseException:
            connection.execute("ROLLBACK TO " + name)
            connection.execute("RELEASE " + name)
            raise
        else:
            connection.execute("RELEASE " + name)

    def blocked_cap(self, connection, pipeline, partition):
        row = connection.execute(
            "SELECT min(t) FROM ("
            " SELECT event_time AS t FROM partition_faults"
            " WHERE pipeline=? AND partition_id=? AND resolved=0"
            " UNION ALL"
            " SELECT event_time AS t FROM held_events"
            " WHERE pipeline=? AND partition_id=?"
            ")",
            (pipeline["name"], partition, pipeline["name"], partition),
        ).fetchone()
        return row[0]

    def hold(self, connection, pipeline, record, reason):
        row = record["document"]
        connection.execute(
            "INSERT INTO held_events VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(pipeline,partition_id,source_offset) DO NOTHING",
            (
                pipeline["name"],
                record["partition_id"],
                record["source_offset"],
                record["sequence"],
                row["id"],
                row["time"],
                reason,
                canonical(row),
                utc_now(),
            ),
        )

    def open_fault(self, connection, pipeline, record, error):
        row = record["document"]
        reason = type(error).__name__ + ": " + str(error)
        connection.execute(
            "INSERT INTO partition_faults VALUES (?,?,?,?,?,?,?,?,?,0,0,?,NULL) "
            "ON CONFLICT(pipeline,partition_id,source_offset) DO UPDATE SET "
            "reason=excluded.reason,document=excluded.document,resolved=0,resolved_at=NULL",
            (
                pipeline["name"],
                record["partition_id"],
                record["source_offset"],
                record["sequence"],
                row["id"],
                row["time"],
                reason,
                canonical(row),
                None,
                utc_now(),
            ),
        )
        self.catalog.event(
            connection,
            pipeline["name"],
            "partition_fault_opened",
            {
                "partition": record["partition_id"],
                "offset": record["source_offset"],
                "sequence": record["sequence"],
                "event_id": row["id"],
                "reason": reason,
            },
        )
        return reason

    def consume(self, connection, pipeline, record, mode, scope):
        partition = record["partition_id"]
        if mode == "isolate":
            cap = self.blocked_cap(connection, pipeline, partition)
            if cap is not None:
                self.hold(
                    connection,
                    pipeline,
                    record,
                    "partition is held behind an unresolved fault",
                )
                return "held"
            try:
                with self._record_scope(connection, next(scope)):
                    return self.process(connection, pipeline, record)
            except FAULT_ERRORS as error:
                self.open_fault(connection, pipeline, record, error)
                return "faulted"
        return self.process(connection, pipeline, record)

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
            "faulted": sum(run["faulted"] for run in runs),
            "held": sum(run["held"] for run in runs),
            "runs": runs,
        }

    def faults(self, pipeline, include_resolved=False):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            sql = "SELECT * FROM partition_faults WHERE pipeline=?"
            if not include_resolved:
                sql += " AND resolved=0"
            sql += " ORDER BY partition_id,source_offset"
            result = []
            for row in connection.execute(sql, (pipeline,)):
                value = {**dict(row), "document": json.loads(row["document"])}
                value["correction"] = (
                    json.loads(row["correction"]) if row["correction"] else None
                )
                value["resolved"] = bool(row["resolved"])
                result.append(value)
            return result

    def repairs(self, pipeline, partition=None, offset=None):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            sql = "SELECT * FROM fault_repairs WHERE pipeline=?"
            values = [pipeline]
            if partition is not None:
                sql += " AND partition_id=?"
                values.append(integer(partition, "partition", 0))
            if offset is not None:
                sql += " AND source_offset=?"
                values.append(integer(offset, "source offset", 0))
            sql += " ORDER BY partition_id,source_offset,attempt"
            return [
                {
                    **dict(row),
                    "correction": (
                        json.loads(row["correction"]) if row["correction"] else None
                    ),
                }
                for row in connection.execute(sql, values)
            ]

    def held_events(self, pipeline, partition=None):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            sql = "SELECT * FROM held_events WHERE pipeline=?"
            values = [pipeline]
            if partition is not None:
                sql += " AND partition_id=?"
                values.append(integer(partition, "partition", 0))
            sql += " ORDER BY partition_id,source_offset"
            return [
                {**dict(row), "document": json.loads(row["document"])}
                for row in connection.execute(sql, values)
            ]

    def corrected(self, document, correction):
        base = dict(document)
        attrs = dict(base.get("attrs") or {})
        for key, value in correction.items():
            if key in CORRECTION_SIMPLE - {"attrs"}:
                base[key] = value
            elif key == "attrs":
                if not isinstance(value, dict):
                    raise Invalid("correction attrs must be an object")
                attrs.update(value)
            elif isinstance(key, str) and key.startswith(CORRECTION_PREFIX):
                name = key[len(CORRECTION_PREFIX) :]
                if not name:
                    raise Invalid("correction attribute name is empty")
                attrs[name] = value
            else:
                raise Invalid(
                    "correction may only change value, key, kind or attrs"
                )
        base["attrs"] = attrs
        return event(base)

    def _attempt(
        self, connection, pipeline, partition, offset, event_id, attempt, outcome, correction, reason
    ):
        connection.execute(
            "INSERT INTO fault_repairs VALUES (?,?,?,?,?,?,?,?,?)",
            (
                pipeline,
                partition,
                offset,
                event_id,
                attempt,
                outcome,
                canonical(correction),
                reason,
                utc_now(),
            ),
        )

    def _advance(self, connection, pipeline):
        self.watermarks.advance_in(connection, pipeline)
        self.windows.close(connection, pipeline)
        self.operators.expire(connection, pipeline)

    def repair(self, pipeline, partition, offset, correction=None):
        correction = {} if correction is None else correction
        if not isinstance(correction, dict):
            raise Invalid("correction must be an object")
        try:
            encoded = canonical(correction)
        except (TypeError, ValueError) as error:
            raise Invalid("correction must contain JSON values") from error
        with self.catalog.transaction(True) as connection:
            info = self.catalog.pipeline(pipeline, connection)
            channel = self.catalog.channel(info["channel"], connection)
            partition = integer(partition, "partition", 0, channel["partitions"] - 1)
            offset = integer(offset, "source offset", 0)
            fault = connection.execute(
                "SELECT * FROM partition_faults WHERE pipeline=? AND partition_id=? AND source_offset=?",
                (pipeline, partition, offset),
            ).fetchone()
            if fault is None:
                raise Missing("no partition fault exists at that source offset")
            if fault["resolved"]:
                raise Conflict("partition fault is already resolved")
            earlier = connection.execute(
                "SELECT min(source_offset) FROM ("
                " SELECT source_offset FROM partition_faults"
                " WHERE pipeline=? AND partition_id=? AND resolved=0"
                " UNION ALL"
                " SELECT source_offset FROM held_events"
                " WHERE pipeline=? AND partition_id=?"
                ")",
                (pipeline, partition, pipeline, partition),
            ).fetchone()[0]
            if earlier is not None and earlier < offset:
                raise Conflict(
                    "an earlier unresolved fault must be repaired first"
                )
            attempt = fault["attempts"] + 1
            connection.execute(
                "UPDATE partition_faults SET attempts=?,correction=? "
                "WHERE pipeline=? AND partition_id=? AND source_offset=?",
                (attempt, encoded, pipeline, partition, offset),
            )
            try:
                document = self.corrected(json.loads(fault["document"]), correction)
            except FAULT_ERRORS as error:
                reason = type(error).__name__ + ": " + str(error)
                self._attempt(
                    connection,
                    pipeline,
                    partition,
                    offset,
                    fault["event_id"],
                    attempt,
                    "failed",
                    correction,
                    reason,
                )
                return {
                    "pipeline": pipeline,
                    "partition": partition,
                    "offset": offset,
                    "outcome": "failed",
                    "attempt": attempt,
                    "processed": None,
                    "held_processed": 0,
                    "reason": reason,
                    "fault": self._fault(connection, pipeline, partition, offset),
                }
            record = {
                "sequence": fault["sequence"],
                "partition_id": partition,
                "source_offset": offset,
                "document": document,
            }
            scope = count()
            try:
                with self._record_scope(connection, next(scope)):
                    disposition = self.process(connection, info, record)
            except FAULT_ERRORS as error:
                reason = type(error).__name__ + ": " + str(error)
                self._attempt(
                    connection,
                    pipeline,
                    partition,
                    offset,
                    fault["event_id"],
                    attempt,
                    "failed",
                    correction,
                    reason,
                )
                return {
                    "pipeline": pipeline,
                    "partition": partition,
                    "offset": offset,
                    "outcome": "failed",
                    "attempt": attempt,
                    "processed": None,
                    "held_processed": 0,
                    "reason": reason,
                    "fault": self._fault(connection, pipeline, partition, offset),
                }
            connection.execute(
                "UPDATE partition_faults SET resolved=1,resolved_at=? "
                "WHERE pipeline=? AND partition_id=? AND source_offset=?",
                (utc_now(), pipeline, partition, offset),
            )
            self._attempt(
                connection,
                pipeline,
                partition,
                offset,
                fault["event_id"],
                attempt,
                "resolved",
                correction,
                None,
            )
            self._advance(connection, info)
            held_processed = 0
            while True:
                held = connection.execute(
                    "SELECT * FROM held_events WHERE pipeline=? AND partition_id=? "
                    "ORDER BY source_offset LIMIT 1",
                    (pipeline, partition),
                ).fetchone()
                if held is None:
                    break
                held_record = {
                    "sequence": held["sequence"],
                    "partition_id": partition,
                    "source_offset": held["source_offset"],
                    "document": json.loads(held["document"]),
                }
                try:
                    with self._record_scope(connection, next(scope)):
                        self.process(connection, info, held_record)
                except FAULT_ERRORS as error:
                    connection.execute(
                        "DELETE FROM held_events WHERE pipeline=? AND partition_id=? AND source_offset=?",
                        (pipeline, partition, held["source_offset"]),
                    )
                    self.open_fault(connection, info, held_record, error)
                    break
                connection.execute(
                    "DELETE FROM held_events WHERE pipeline=? AND partition_id=? AND source_offset=?",
                    (pipeline, partition, held["source_offset"]),
                )
                held_processed += 1
                self._advance(connection, info)
            self.catalog.event(
                connection,
                pipeline,
                "partition_fault_repaired",
                {
                    "partition": partition,
                    "offset": offset,
                    "attempt": attempt,
                    "held_processed": held_processed,
                },
            )
            return {
                "pipeline": pipeline,
                "partition": partition,
                "offset": offset,
                "outcome": "resolved",
                "attempt": attempt,
                "processed": disposition,
                "held_processed": held_processed,
                "reason": None,
                "fault": self._fault(connection, pipeline, partition, offset),
            }

    def _fault(self, connection, pipeline, partition, offset):
        row = connection.execute(
            "SELECT * FROM partition_faults WHERE pipeline=? AND partition_id=? AND source_offset=?",
            (pipeline, partition, offset),
        ).fetchone()
        if row is None:
            return None
        value = {**dict(row), "document": json.loads(row["document"])}
        value["correction"] = (
            json.loads(row["correction"]) if row["correction"] else None
        )
        value["resolved"] = bool(row["resolved"])
        return value

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
            faults = connection.execute(
                "SELECT count(*) FROM partition_faults WHERE pipeline=? AND resolved=0",
                (pipeline,),
            ).fetchone()[0]
            held = connection.execute(
                "SELECT count(*) FROM held_events WHERE pipeline=?", (pipeline,)
            ).fetchone()[0]
            return {
                **info,
                "source_events": source_count,
                "processed_events": sum(counts.values()),
                "dispositions": counts,
                "pending": source_count - sum(counts.values()),
                "active_faults": faults,
                "held_events": held,
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
