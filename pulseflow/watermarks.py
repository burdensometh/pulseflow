from .values import Conflict, integer


class Watermarks:
    def __init__(self, catalog):
        self.catalog = catalog

    def observe(self, connection, pipeline, partition, offset, event_time):
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

    def calculate(self, connection, pipeline):
        candidates = []
        for row in connection.execute(
            "SELECT * FROM progress WHERE pipeline=? ORDER BY partition_id",
            (pipeline["name"],),
        ):
            known = [
                value
                for value in [row["max_time"], row["declared_time"]]
                if value is not None
            ]
            if not known:
                return pipeline["watermark"]
            candidates.append(max(known) - pipeline["plan"]["delay_ms"])
        proposed = min(candidates)
        previous = pipeline["watermark"]
        return proposed if previous is None else max(previous, proposed)

    def advance_in(self, connection, pipeline):
        value = self.calculate(connection, pipeline)
        connection.execute(
            "UPDATE pipelines SET watermark=? WHERE name=?", (value, pipeline["name"])
        )
        pipeline["watermark"] = value
        return value

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
            self.catalog.event(
                connection,
                pipeline,
                "partition_time_declared",
                {"partition": partition, "time": time_ms},
            )
        return {"pipeline": pipeline, "partition": partition, "time": time_ms}

    def status(self, pipeline):
        with self.catalog.transaction() as connection:
            info = self.catalog.pipeline(pipeline, connection)
            rows = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM progress WHERE pipeline=? ORDER BY partition_id",
                    (pipeline,),
                )
            ]
            return {
                "watermark": info["watermark"],
                "partitions": rows,
                "candidate": self.calculate(connection, info),
            }
