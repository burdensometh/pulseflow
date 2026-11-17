from .values import Conflict, integer


class Watermarks:
    def __init__(self, catalog):
        self.catalog = catalog

    # -- partition activity bookkeeping -----------------------------------

    def _clock(self, connection, pipeline, now_ms, write):
        """Return the effective processing clock for this observation.

        The persisted clock is a high-water mark.  A supplied clock is never
        allowed to move it backwards, so a wall-clock rollback (or a restart
        whose clock is behind the persisted one) cannot rewind activity
        accounting.  Window closure remains purely event-time based; this
        clock only decides whether a silent partition is idle.
        """

        if now_ms is not None:
            integer(now_ms, "processing clock", 0)
        row = connection.execute(
            "SELECT clock_ms FROM pipeline_clocks WHERE pipeline=?",
            (pipeline["name"],),
        ).fetchone()
        stored = row[0] if row is not None else None
        if now_ms is None:
            return stored
        value = now_ms if stored is None else max(stored, now_ms)
        if write and value != stored:
            connection.execute(
                "INSERT INTO pipeline_clocks VALUES (?,?) "
                "ON CONFLICT(pipeline) DO UPDATE SET clock_ms=excluded.clock_ms",
                (pipeline["name"], value),
            )
        return value

    def _activity(self, connection, pipeline, partition):
        row = connection.execute(
            "SELECT * FROM partition_activity WHERE pipeline=? AND partition_id=?",
            (pipeline["name"], partition),
        ).fetchone()
        return dict(row) if row is not None else None

    def _touch(self, connection, pipeline, partition, clock, write):
        """Record that a partition showed life at ``clock``.

        Called when a record is observed, when a backlog exists, or when the
        partition explicitly declares progress.  Clears any idle marking so
        the partition immediately rejoins watermark coordination.
        """

        if clock is None:
            return
        row = self._activity(connection, pipeline, partition)
        if (
            row is not None
            and not row["idle"]
            and row["idle_since_ms"] is None
            and row["last_activity_ms"] >= clock
        ):
            return
        if write:
            connection.execute(
                "INSERT INTO partition_activity"
                "(pipeline,partition_id,last_activity_ms,idle_since_ms,idle) "
                "VALUES (?,?,?,NULL,0) ON CONFLICT(pipeline,partition_id) DO UPDATE SET "
                "last_activity_ms=excluded.last_activity_ms,idle_since_ms=NULL,idle=0",
                (pipeline["name"], partition, clock),
            )

    def _reactivate(self, connection, pipeline, partition):
        """Clear an idle marking, re-baselining silence at the current clock.

        The baseline is the persisted processing clock if one exists, else the
        partition's own last activity.  This never moves the clock forward, so
        declaring progress cannot manufacture silence evidence either.
        """

        row = self._activity(connection, pipeline, partition)
        clock = self._clock(connection, pipeline, None, False)
        if clock is None:
            clock = row["last_activity_ms"] if row is not None else 0
        connection.execute(
            "INSERT INTO partition_activity"
            "(pipeline,partition_id,last_activity_ms,idle_since_ms,idle) "
            "VALUES (?,?,?,NULL,0) ON CONFLICT(pipeline,partition_id) DO UPDATE SET "
            "last_activity_ms=excluded.last_activity_ms,idle_since_ms=NULL,idle=0",
            (pipeline["name"], partition, clock),
        )

    def _pending(self, connection, pipeline, partition):
        return (
            connection.execute(
                "SELECT 1 FROM journal WHERE channel=? AND partition_id=? AND sequence>? LIMIT 1",
                (pipeline["channel"], partition, pipeline["last_sequence"]),
            ).fetchone()
            is not None
        )

    def _is_idle(self, connection, pipeline, partition, clock, timeout, write):
        """Decide whether a silent partition leaves watermark coordination.

        A partition qualifies only when it has no unread source records; a
        partition that is merely slow to process its backlog stays active and
        keeps anchoring the watermark.  Silence is measured on the persisted
        monotonic clock from the partition's last observed activity, and the
        observation that records activity can never also retire the partition
        (``clock > last_activity_ms``), even with a zero timeout.

        A never-seen partition is first given a baseline at the current clock
        and cannot idle until a later observation shows the timeout elapsed;
        this keeps a freshly created pipeline from looking idle before any
        silence has been observed.  Because idling only removes a partition
        from a minimum, a forward clock jump can never invent progress: if
        every partition would fall silent the watermark freezes, it does not
        advance.
        """

        if self._pending(connection, pipeline, partition):
            self._touch(connection, pipeline, partition, clock, write)
            return False
        row = self._activity(connection, pipeline, partition)
        if row is None:
            if write and clock is not None:
                connection.execute(
                    "INSERT INTO partition_activity VALUES (?,?,?,NULL,0)",
                    (pipeline["name"], partition, clock),
                )
            return False
        if clock is None:
            return bool(row["idle"])
        if row["idle"]:
            return True
        if clock > row["last_activity_ms"] and (
            clock - row["last_activity_ms"] >= timeout
        ):
            if write:
                connection.execute(
                    "UPDATE partition_activity SET idle=1,idle_since_ms=? "
                    "WHERE pipeline=? AND partition_id=?",
                    (clock, pipeline["name"], partition),
                )
                self.catalog.event(
                    connection,
                    pipeline["name"],
                    "partition_idle",
                    {
                        "partition": partition,
                        "last_activity": row["last_activity_ms"],
                        "clock": clock,
                        "timeout_ms": timeout,
                    },
                )
            return True
        return False

    # -- watermark computation --------------------------------------------

    def _candidate_for(self, pipeline, row):
        known = [
            value
            for value in [row["max_time"], row["declared_time"]]
            if value is not None
        ]
        if not known:
            return None
        return max(known) - pipeline["plan"]["delay_ms"]

    def calculate(self, connection, pipeline, now_ms=None, write=False):
        """Compute the next watermark without ever moving it backwards.

        With idle handling disabled this matches the original contract: a
        partition that has seen no event and has no declaration blocks the
        watermark.  With idle configured, partitions that have no pending
        records and have been silent for the timeout drop out of the minimum;
        partitions with backlog never do.  If every partition is idle there is
        nothing to anchor the watermark and the previous value is retained.
        """

        rows = list(
            connection.execute(
                "SELECT * FROM progress WHERE pipeline=? ORDER BY partition_id",
                (pipeline["name"],),
            )
        )
        previous = pipeline["watermark"]
        spec = pipeline["plan"].get("idle")
        if spec is None:
            candidates = []
            for row in rows:
                candidate = self._candidate_for(pipeline, row)
                if candidate is None:
                    return previous
                candidates.append(candidate)
            if not candidates:
                return previous
            proposed = min(candidates)
            return proposed if previous is None else max(previous, proposed)
        clock = self._clock(connection, pipeline, now_ms, write)
        candidates = []
        for row in rows:
            partition = row["partition_id"]
            if self._is_idle(
                connection,
                pipeline,
                partition,
                clock,
                spec["timeout_ms"],
                write,
            ):
                continue
            candidate = self._candidate_for(pipeline, row)
            if candidate is None:
                # An active partition with no event-time evidence still
                # blocks progress; idleness is the only way to skip it.
                return previous
            candidates.append(candidate)
        if not candidates:
            return previous
        proposed = min(candidates)
        return proposed if previous is None else max(previous, proposed)

    def advance_in(self, connection, pipeline, now_ms=None):
        value = self.calculate(connection, pipeline, now_ms, write=True)
        connection.execute(
            "UPDATE pipelines SET watermark=? WHERE name=?", (value, pipeline["name"])
        )
        pipeline["watermark"] = value
        return value

    # -- public operations -------------------------------------------------

    def declare(self, pipeline, partition, time_ms):
        integer(time_ms, "declared event time")
        with self.catalog.transaction(True) as connection:
            info = self.catalog.pipeline(pipeline, connection)
            channel = self.catalog.channel(info["channel"], connection)
            integer(partition, "partition", 0, channel["partitions"] - 1)
            row = connection.execute(
                "SELECT declared_time FROM progress WHERE pipeline=? AND partition_id=?",
                (pipeline, partition),
            ).fetchone()
            if row[0] is not None and time_ms < row[0]:
                raise Conflict("declared partition time cannot move backwards")
            pending = connection.execute(
                "SELECT min(event_time) FROM journal WHERE channel=? AND partition_id=? AND sequence>?",
                (info["channel"], partition, info["last_sequence"]),
            ).fetchone()[0]
            if pending is not None and pending < time_ms:
                raise Conflict("cannot declare progress beyond an unread event")
            connection.execute(
                "UPDATE progress SET declared_time=? WHERE pipeline=? AND partition_id=?",
                (time_ms, pipeline, partition),
            )
            # An explicit declaration is evidence of life: the partition
            # rejoins coordination immediately.  It is only given a new
            # activity baseline, not a fabricated wall-clock reading, so a
            # declared partition cannot be retired again in the same breath.
            if info["plan"].get("idle") is not None:
                self._reactivate(connection, info, partition)
            self.catalog.event(
                connection,
                pipeline,
                "partition_time_declared",
                {"partition": partition, "time": time_ms},
            )
        return {"pipeline": pipeline, "partition": partition, "time": time_ms}

    def observe(self, connection, pipeline, partition, offset, event_time, now_ms=None):
        row = connection.execute(
            "SELECT * FROM progress WHERE pipeline=? AND partition_id=?",
            (pipeline["name"], partition),
        ).fetchone()
        if row is None or row["next_offset"] != offset:
            raise Conflict("processing position is not the next contiguous offset")
        maximum = (
            event_time if row["max_time"] is None else max(row["max_time"], event_time)
        )
        connection.execute(
            "UPDATE progress SET next_offset=?,max_time=? WHERE pipeline=? AND partition_id=?",
            (offset + 1, maximum, pipeline["name"], partition),
        )
        if pipeline["plan"].get("idle") is not None:
            clock = self._clock(connection, pipeline, now_ms, True)
            self._touch(connection, pipeline, partition, clock, True)

    def activity(self, connection, pipeline):
        """Return the persisted activity rows plus the current idle decision.

        The stored ``idle`` flag is authoritative for watermark calculation,
        but reporting re-evaluates it too: a partition that was retired and
        then received records is already considered active because unread
        records disqualify idleness, and the report must say so before the
        next processing step persists the change.
        """

        spec = pipeline["plan"].get("idle")
        rows = {
            row["partition_id"]: dict(row)
            for row in connection.execute(
                "SELECT * FROM partition_activity WHERE pipeline=?",
                (pipeline["name"],),
            )
        }
        clock = self._clock(connection, pipeline, None, False)
        output = []
        for progress in connection.execute(
            "SELECT * FROM progress WHERE pipeline=? ORDER BY partition_id",
            (pipeline["name"],),
        ):
            partition = progress["partition_id"]
            state = rows.get(partition, {})
            idle = bool(state.get("idle", 0))
            if spec is not None and state:
                # Re-evaluate even a persisted idle flag: records that arrived
                # since it was set mean the partition is active again, and the
                # report must not lag behind until the next processing step.
                idle = self._is_idle(
                    connection, pipeline, partition, clock, spec["timeout_ms"], False
                )
            output.append(
                {
                    **dict(progress),
                    "idle": idle,
                    "idle_since_ms": state.get("idle_since_ms"),
                    "last_activity_ms": state.get("last_activity_ms"),
                }
            )
        return {"clock_ms": clock, "partitions": output}

    def status(self, pipeline):
        with self.catalog.transaction() as connection:
            info = self.catalog.pipeline(pipeline, connection)
            activity = self.activity(connection, info)
            return {
                "watermark": info["watermark"],
                "partitions": activity["partitions"],
                "candidate": self.calculate(connection, info),
                "idle_partitions": [
                    row["partition_id"] for row in activity["partitions"] if row["idle"]
                ],
                "active_partitions": [
                    row["partition_id"]
                    for row in activity["partitions"]
                    if not row["idle"]
                ],
                "clock_ms": activity["clock_ms"],
            }
