from .expressions import Mapping, field
from .values import canonical


class Operators:
    def __init__(self, state, dimensions, results):
        self.state = state
        self.dimensions = dimensions
        self.results = results

    def transform(self, connection, pipeline, row):
        version = None
        if "dimension" in pipeline["plan"]:
            row, version = self.dimensions.enrich(
                connection, row, pipeline["plan"]["dimension"]
            )
            if row is None:
                return None, version
        mapped = Mapping(pipeline["plan"]["mapping"]).apply(row)
        return mapped, version

    def duplicate(self, connection, pipeline, row):
        spec = pipeline["plan"].get("dedup")
        if spec is None:
            return False
        value = field(row, spec["field"])
        if value is None:
            return False
        key = canonical(value)
        old = connection.execute(
            "SELECT * FROM seen_keys WHERE pipeline=? AND dedup_key=?",
            (pipeline["name"], key),
        ).fetchone()
        if old and row["time"] < old["expires_at"]:
            return True
        connection.execute(
            "INSERT INTO seen_keys VALUES (?,?,?,?) ON CONFLICT(pipeline,dedup_key) DO UPDATE SET expires_at=excluded.expires_at,event_id=excluded.event_id",
            (pipeline["name"], key, row["time"] + spec["ttl_ms"], row["id"]),
        )
        return False

    def latest(self, connection, pipeline, row):
        previous = self.state.get(connection, pipeline, "latest", row["key"])
        if previous is None or (row["time"], row["id"]) > (
            previous["time"],
            previous["id"],
        ):
            self.state.put(connection, pipeline, "latest", row["key"], row)

    def sequence(self, connection, pipeline, row):
        spec = pipeline["plan"].get("sequence")
        if spec is None:
            return None
        previous = self.state.get(connection, pipeline, "sequence", row["key"])
        if row["kind"] == spec["first_kind"]:
            if previous is None or (row["time"], row["id"]) > (
                previous["time"],
                previous["id"],
            ):
                self.state.put(connection, pipeline, "sequence", row["key"], row)
            return None
        if row["kind"] != spec["then_kind"] or previous is None:
            return None
        if not previous["time"] <= row["time"] <= previous["time"] + spec["within_ms"]:
            return None
        key = canonical(["sequence", previous["id"], row["id"]])
        document = {
            "kind": "sequence",
            "key": row["key"],
            "first": previous,
            "then": row,
            "elapsed_ms": row["time"] - previous["time"],
        }
        self.results.publish(connection, pipeline["name"], key, document)
        self.state.delete(connection, pipeline, "sequence", row["key"])
        return document

    def expire(self, connection, pipeline):
        watermark = pipeline["watermark"]
        if watermark is None:
            return
        connection.execute(
            "DELETE FROM seen_keys WHERE pipeline=? AND expires_at<=?",
            (pipeline["name"], watermark),
        )
        spec = pipeline["plan"].get("sequence")
        if spec:
            rows = connection.execute(
                "SELECT * FROM keyed_state WHERE pipeline=? AND state_kind='sequence'",
                (pipeline["name"],),
            ).fetchall()
            import json

            for row in rows:
                value = json.loads(row["document"])
                if value["time"] + spec["within_ms"] < watermark:
                    self.state.delete(
                        connection, pipeline, "sequence", row["group_key"]
                    )
