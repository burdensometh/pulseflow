import json

from .state import bucket_for
from .values import Conflict, Invalid, canonical, integer


DDL = """
CREATE TABLE IF NOT EXISTS join_sides (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 side TEXT NOT NULL CHECK(side IN ('left','right')),
 channel TEXT NOT NULL REFERENCES channels(name),
 watermark INTEGER,
 PRIMARY KEY(pipeline,side)
);
CREATE TABLE IF NOT EXISTS join_progress (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 side TEXT NOT NULL CHECK(side IN ('left','right')),
 partition_id INTEGER NOT NULL,
 next_offset INTEGER NOT NULL DEFAULT 0,
 max_time INTEGER,
 declared_time INTEGER,
 PRIMARY KEY(pipeline,side,partition_id)
);
CREATE TABLE IF NOT EXISTS join_buffers (
 pipeline TEXT NOT NULL REFERENCES pipelines(name),
 side TEXT NOT NULL CHECK(side IN ('left','right')),
 bucket INTEGER NOT NULL,
 group_key TEXT NOT NULL,
 event_id TEXT NOT NULL,
 event_time INTEGER NOT NULL,
 document TEXT NOT NULL,
 matched INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(pipeline,side,event_id)
);
CREATE INDEX IF NOT EXISTS join_buffers_match
ON join_buffers(pipeline,side,group_key,event_time);
"""

SIDES = ("left", "right")


def qualified(side, event_id):
    return side + ":" + event_id


def split_qualified(value):
    side, separator, raw = value.partition(":")
    if not separator or side not in SIDES or not raw:
        raise Invalid("join event ids are side-qualified as left:/right:")
    return side, raw


class Joins:
    def __init__(self, catalog, results):
        self.catalog = catalog
        self.results = results
        with catalog.connect() as connection:
            connection.executescript(DDL)

    def create(self, name, left, right, spec, buckets=4):
        if not isinstance(spec, dict):
            raise Invalid("join spec must be an object")
        plan = {
            "join": {**spec, "left_channel": left, "right_channel": right},
        }
        return self.engine.create(name, left, plan, buckets)

    def initialize(self, connection, name, spec):
        for side in SIDES:
            channel = self.catalog.channel(spec[side + "_channel"], connection)
            connection.execute(
                "INSERT INTO join_sides VALUES (?,?,?,NULL)",
                (name, side, channel["name"]),
            )
            connection.executemany(
                "INSERT INTO join_progress VALUES (?,?,?,0,NULL,NULL)",
                ((name, side, p) for p in range(channel["partitions"])),
            )

    def load(self, connection, pipeline):
        rows = connection.execute(
            "SELECT side,watermark FROM join_sides WHERE pipeline=?",
            (pipeline["name"],),
        ).fetchall()
        marks = {row["side"]: row["watermark"] for row in rows}
        if set(marks) != set(SIDES):
            raise Conflict("join side state is incomplete")
        pipeline["join_watermarks"] = marks
        return marks

    def read_inputs(self, connection, pipeline, limit):
        spec = pipeline["plan"]["join"]
        rows = connection.execute(
            "SELECT * FROM journal WHERE channel IN (?,?) AND sequence>? "
            "ORDER BY sequence LIMIT ?",
            (spec["left_channel"], spec["right_channel"], pipeline["last_sequence"], limit),
        )
        return [{**dict(row), "document": json.loads(row["document"])} for row in rows]

    def side_of(self, spec, channel):
        if channel == spec["left_channel"]:
            return "left"
        if channel == spec["right_channel"]:
            return "right"
        raise Conflict("journal record belongs to neither join channel")

    def process(self, connection, pipeline, record):
        spec = pipeline["plan"]["join"]
        row = record["document"]
        side = self.side_of(spec, record["channel"])
        current = pipeline["join_watermarks"][side]
        if current is not None and row["time"] < current:
            disposition = "late"
            connection.execute(
                "INSERT INTO late_events VALUES (?,?,?,?,?,?)",
                (
                    pipeline["name"],
                    qualified(side, row["id"]),
                    record["sequence"],
                    row["time"],
                    current,
                    canonical(row),
                ),
            )
        else:
            disposition = "accepted"
            self.buffer(connection, pipeline, spec, side, row)
            self.match(connection, pipeline, spec, side, row)
        connection.execute(
            "INSERT INTO processed_events VALUES (?,?,?,?,?,?)",
            (
                pipeline["name"],
                qualified(side, row["id"]),
                record["sequence"],
                disposition,
                canonical(row),
                None,
            ),
        )
        self.observe(connection, pipeline, side, record, row["time"])
        return disposition

    def buffer(self, connection, pipeline, spec, side, row):
        count = connection.execute(
            "SELECT count(*) FROM join_buffers WHERE pipeline=?", (pipeline["name"],)
        ).fetchone()[0]
        if count >= spec["max_buffer_events"]:
            raise Invalid("join buffer capacity exceeded")
        bucket = bucket_for(row["key"], pipeline["buckets"], pipeline["hash_version"])
        connection.execute(
            "INSERT INTO join_buffers VALUES (?,?,?,?,?,?,?,0)",
            (
                pipeline["name"],
                side,
                bucket,
                row["key"],
                row["id"],
                row["time"],
                canonical(row),
            ),
        )

    def match(self, connection, pipeline, spec, side, row):
        if side == "left":
            candidates = connection.execute(
                "SELECT document FROM join_buffers WHERE pipeline=? AND side='right' "
                "AND group_key=? AND event_time>=? AND event_time<? "
                "ORDER BY event_time,event_id",
                (
                    pipeline["name"],
                    row["key"],
                    row["time"] - spec["before_ms"],
                    row["time"] + spec["after_ms"],
                ),
            ).fetchall()
            for candidate in candidates:
                self.emit_pair(connection, pipeline, row, json.loads(candidate[0]))
            if candidates:
                self.mark_matched(connection, pipeline["name"], row["id"])
        else:
            candidates = connection.execute(
                "SELECT event_id,document FROM join_buffers WHERE pipeline=? "
                "AND side='left' AND group_key=? AND event_time>? AND event_time<=? "
                "ORDER BY event_time,event_id",
                (
                    pipeline["name"],
                    row["key"],
                    row["time"] - spec["after_ms"],
                    row["time"] + spec["before_ms"],
                ),
            ).fetchall()
            for candidate in candidates:
                self.emit_pair(
                    connection, pipeline, json.loads(candidate["document"]), row
                )
                self.mark_matched(connection, pipeline["name"], candidate["event_id"])

    def mark_matched(self, connection, pipeline, event_id):
        connection.execute(
            "UPDATE join_buffers SET matched=1 WHERE pipeline=? AND side='left' AND event_id=?",
            (pipeline, event_id),
        )

    def emit_pair(self, connection, pipeline, left, right):
        key = canonical(["join", left["id"], right["id"]])
        document = {
            "kind": "join",
            "key": left["key"],
            "matched": True,
            "left": left,
            "right": right,
            "delta_ms": right["time"] - left["time"],
        }
        self.results.publish(connection, pipeline["name"], key, document)

    def emit_unmatched(self, connection, pipeline, left):
        key = canonical(["join", left["id"], None])
        document = {
            "kind": "join",
            "key": left["key"],
            "matched": False,
            "left": left,
            "right": None,
            "delta_ms": None,
        }
        self.results.publish(connection, pipeline["name"], key, document)

    def observe(self, connection, pipeline, side, record, event_time):
        row = connection.execute(
            "SELECT * FROM join_progress WHERE pipeline=? AND side=? AND partition_id=?",
            (pipeline["name"], side, record["partition_id"]),
        ).fetchone()
        if row is None or row["next_offset"] != record["source_offset"]:
            raise Conflict("processing position is not the next contiguous offset")
        maximum = (
            event_time if row["max_time"] is None else max(row["max_time"], event_time)
        )
        connection.execute(
            "UPDATE join_progress SET next_offset=?,max_time=? "
            "WHERE pipeline=? AND side=? AND partition_id=?",
            (record["source_offset"] + 1, maximum, pipeline["name"], side, record["partition_id"]),
        )

    def advance(self, connection, pipeline):
        delay = pipeline["plan"]["join"]["delay_ms"]
        marks = {}
        for side in SIDES:
            candidates = []
            blocked = False
            for row in connection.execute(
                "SELECT * FROM join_progress WHERE pipeline=? AND side=? "
                "ORDER BY partition_id",
                (pipeline["name"], side),
            ):
                known = [
                    value
                    for value in [row["max_time"], row["declared_time"]]
                    if value is not None
                ]
                if not known:
                    blocked = True
                    break
                candidates.append(max(known) - delay)
            previous = pipeline["join_watermarks"][side]
            if blocked:
                proposed = previous
            else:
                candidate = min(candidates)
                proposed = candidate if previous is None else max(previous, candidate)
            marks[side] = proposed
            connection.execute(
                "UPDATE join_sides SET watermark=? WHERE pipeline=? AND side=?",
                (proposed, pipeline["name"], side),
            )
        pipeline["join_watermarks"] = marks
        overall = None if None in marks.values() else min(marks.values())
        connection.execute(
            "UPDATE pipelines SET watermark=? WHERE name=?",
            (overall, pipeline["name"]),
        )
        pipeline["watermark"] = overall
        return marks

    def finalize(self, connection, pipeline):
        spec = pipeline["plan"]["join"]
        marks = pipeline["join_watermarks"]
        counts = {"unmatched": 0, "expired_left": 0, "expired_right": 0}
        if marks["right"] is not None:
            rows = connection.execute(
                "SELECT event_id,matched,document FROM join_buffers WHERE pipeline=? "
                "AND side='left' AND event_time + ? <= ?",
                (pipeline["name"], spec["after_ms"], marks["right"]),
            ).fetchall()
            for row in rows:
                left = json.loads(row["document"])
                if spec["outer"] and not row["matched"]:
                    self.emit_unmatched(connection, pipeline, left)
                    counts["unmatched"] += 1
                connection.execute(
                    "DELETE FROM join_buffers WHERE pipeline=? AND side='left' AND event_id=?",
                    (pipeline["name"], row["event_id"]),
                )
                counts["expired_left"] += 1
        if marks["left"] is not None:
            cursor = connection.execute(
                "DELETE FROM join_buffers WHERE pipeline=? AND side='right' "
                "AND event_time + ? < ?",
                (pipeline["name"], spec["before_ms"], marks["left"]),
            )
            counts["expired_right"] = cursor.rowcount
        return counts

    def declare(self, pipeline, side, partition, time_ms):
        if side not in SIDES:
            raise Invalid("join side must be left or right")
        integer(time_ms, "declared event time")
        with self.catalog.transaction(True) as connection:
            info = self.catalog.pipeline(pipeline, connection)
            spec = info["plan"].get("join")
            if spec is None:
                raise Invalid("pipeline is not an interval join")
            channel = self.catalog.channel(spec[side + "_channel"], connection)
            integer(partition, "partition", 0, channel["partitions"] - 1)
            row = connection.execute(
                "SELECT declared_time FROM join_progress "
                "WHERE pipeline=? AND side=? AND partition_id=?",
                (pipeline, side, partition),
            ).fetchone()
            if row[0] is not None and time_ms < row[0]:
                raise Conflict("declared partition time cannot move backwards")
            pending = connection.execute(
                "SELECT min(event_time) FROM journal WHERE channel=? AND partition_id=? AND sequence>?",
                (channel["name"], partition, info["last_sequence"]),
            ).fetchone()[0]
            if pending is not None and pending < time_ms:
                raise Conflict("cannot declare progress beyond an unread event")
            connection.execute(
                "UPDATE join_progress SET declared_time=? "
                "WHERE pipeline=? AND side=? AND partition_id=?",
                (time_ms, pipeline, side, partition),
            )
            self.catalog.event(
                connection,
                pipeline,
                "join_partition_time_declared",
                {"side": side, "partition": partition, "time": time_ms},
            )
        return {"pipeline": pipeline, "side": side, "partition": partition, "time": time_ms}

    def status(self, pipeline, connection=None):
        if connection is None:
            with self.catalog.transaction() as connection:
                return self.status(pipeline, connection)
        info = self.catalog.pipeline(pipeline, connection)
        spec = info["plan"].get("join")
        if spec is None:
            raise Invalid("pipeline is not an interval join")
        sides = {}
        for row in connection.execute(
            "SELECT * FROM join_sides WHERE pipeline=? ORDER BY side", (pipeline,)
        ):
            side = row["side"]
            partitions = [
                dict(progress)
                for progress in connection.execute(
                    "SELECT * FROM join_progress WHERE pipeline=? AND side=? "
                    "ORDER BY partition_id",
                    (pipeline, side),
                )
            ]
            buffered = connection.execute(
                "SELECT count(*) FROM join_buffers WHERE pipeline=? AND side=?",
                (pipeline, side),
            ).fetchone()[0]
            sides[side] = {
                "channel": row["channel"],
                "watermark": row["watermark"],
                "partitions": partitions,
                "buffered": buffered,
            }
        pending = connection.execute(
            "SELECT count(*) FROM join_buffers WHERE pipeline=? AND side='left' AND matched=0",
            (pipeline,),
        ).fetchone()[0]
        return {
            "pipeline": pipeline,
            "watermark": info["watermark"],
            "spec": spec,
            "sides": sides,
            "pending_unmatched_left": pending,
        }

    def buffers(self, pipeline, side=None):
        with self.catalog.transaction() as connection:
            info = self.catalog.pipeline(pipeline, connection)
            if "join" not in info["plan"]:
                raise Invalid("pipeline is not an interval join")
            sql = "SELECT * FROM join_buffers WHERE pipeline=?"
            values = [pipeline]
            if side is not None:
                if side not in SIDES:
                    raise Invalid("join side must be left or right")
                sql += " AND side=?"
                values.append(side)
            sql += " ORDER BY side,group_key,event_time,event_id"
            return [
                {**dict(row), "document": json.loads(row["document"])}
                for row in connection.execute(sql, values)
            ]

    def matches(self, pipeline, event_id):
        with self.catalog.transaction() as connection:
            info = self.catalog.pipeline(pipeline, connection)
            if "join" not in info["plan"]:
                raise Invalid("pipeline is not an interval join")
            side, raw = split_qualified(event_id)
            output = []
            for row in self.results.rows(pipeline, connection=connection):
                document = row["document"]
                if document.get("kind") != "join":
                    continue
                involved = document["left"]["id"] == raw
                if side == "right":
                    involved = (
                        document["right"] is not None
                        and document["right"]["id"] == raw
                    )
                if involved:
                    output.append({**row, "document": document})
            return output
