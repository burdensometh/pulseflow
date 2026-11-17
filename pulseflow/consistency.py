import json

from .state import bucket_for
from .values import canonical, fingerprint
from .windows import starts_for


class Consistency:
    def __init__(self, app):
        self.app = app

    def check(self, pipeline):
        problems = []
        with self.app.catalog.transaction() as connection:
            info = self.app.catalog.pipeline(pipeline, connection)
            channel = info["channel"]
            source = self.app.catalog.channel(channel, connection)
            self.check_source(connection, channel, source["partitions"], problems)
            self.check_progress(connection, info, problems)
            self.check_membership(connection, info, problems)
            self.check_results(connection, info, problems)
            for row in connection.execute(
                "SELECT bucket,group_key FROM keyed_state WHERE pipeline=?", (pipeline,)
            ):
                if row["bucket"] != bucket_for(
                    row["group_key"], info["buckets"], info["hash_version"]
                ):
                    problems.append("keyed state bucket mismatch: " + row["group_key"])
        return {"pipeline": pipeline, "ok": not problems, "problems": problems}

    def check_source(self, connection, channel, partitions, problems):
        for partition in range(partitions):
            records = list(
                connection.execute(
                    "SELECT source_offset,document,checksum FROM journal "
                    "WHERE channel=? AND partition_id=? ORDER BY source_offset",
                    (channel, partition),
                )
            )
            if [row["source_offset"] for row in records] != list(range(len(records))):
                problems.append(f"noncontiguous source partition {partition}")
            expected = connection.execute(
                "SELECT next_offset FROM source_positions WHERE channel=? AND partition_id=?",
                (channel, partition),
            ).fetchone()[0]
            if expected != len(records):
                problems.append(f"source position mismatch for {partition}")
            for row in records:
                if fingerprint(json.loads(row["document"])) != row["checksum"]:
                    problems.append(
                        f'source checksum mismatch at {partition}:{row["source_offset"]}'
                    )

    def check_progress(self, connection, info, problems):
        seen = list(
            connection.execute(
                "SELECT p.*,j.partition_id,j.source_offset FROM processed_events p "
                "LEFT JOIN journal j ON p.source_sequence=j.sequence "
                "WHERE p.pipeline=? ORDER BY p.source_sequence",
                (info["name"],),
            )
        )
        maximum = max((row["source_sequence"] for row in seen), default=0)
        if maximum != info["last_sequence"]:
            problems.append("pipeline cursor does not match processed prefix")
        expected_count = connection.execute(
            "SELECT count(*) FROM journal WHERE channel=? AND sequence<=?",
            (info["channel"], info["last_sequence"]),
        ).fetchone()[0]
        if expected_count != len(seen):
            problems.append("processed event prefix is incomplete")
        for progress in connection.execute(
            "SELECT * FROM progress WHERE pipeline=?", (info["name"],)
        ):
            offsets = [
                row["source_offset"]
                for row in seen
                if row["partition_id"] == progress["partition_id"]
            ]
            if offsets != list(range(progress["next_offset"])):
                problems.append("processed partition offsets do not match progress")

    def check_membership(self, connection, info, problems):
        actual = {}
        for row in connection.execute(
            "SELECT * FROM window_events WHERE pipeline=?", (info["name"],)
        ):
            expected_bucket = bucket_for(
                row["group_key"], info["buckets"], info["hash_version"]
            )
            if expected_bucket != row["bucket"]:
                problems.append("window event is in the wrong state bucket")
            actual.setdefault(row["event_id"], []).append(row["window_start"])
        for row in connection.execute(
            "SELECT * FROM processed_events WHERE pipeline=? AND disposition='accepted'",
            (info["name"],),
        ):
            transformed = json.loads(row["transformed"])
            expected = sorted(starts_for(transformed["time"], info["plan"]["window"]))
            if sorted(actual.get(row["event_id"], [])) != expected:
                problems.append("window membership mismatch for " + row["event_id"])
        for row in connection.execute(
            "SELECT * FROM window_status WHERE pipeline=?", (info["name"],)
        ):
            if row["status"] == "closed" and (
                info["watermark"] is None or row["window_end"] > info["watermark"]
            ):
                problems.append("window closed ahead of watermark")

    def check_results(self, connection, info, problems):
        for row in connection.execute(
            "SELECT * FROM results WHERE pipeline=?", (info["name"],)
        ):
            latest = connection.execute(
                "SELECT * FROM changes WHERE pipeline=? AND result_key=? ORDER BY revision DESC LIMIT 1",
                (info["name"], row["result_key"]),
            ).fetchone()
            if latest is None or (latest["revision"], latest["document"]) != (
                row["revision"],
                row["document"],
            ):
                problems.append("visible result and changelog disagree")
            elif bool(row["deleted"]) != (latest["operation"] == "delete"):
                problems.append("result deletion and changelog disagree")
        for window in connection.execute(
            "SELECT * FROM window_status WHERE pipeline=? AND status='closed'",
            (info["name"],),
        ):
            key = canonical(
                [
                    "window",
                    window["group_key"],
                    window["window_start"],
                    window["window_end"],
                ]
            )
            row = connection.execute(
                "SELECT revision FROM results WHERE pipeline=? AND result_key=?",
                (info["name"], key),
            ).fetchone()
            if row is None or row["revision"] < window["result_revision"]:
                problems.append("closed window result revision is missing")
