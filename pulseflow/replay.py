from collections import Counter

from .spec import call_knowledge_spec, normalize_join_mode
from .values import Invalid, fingerprint


class Replay:
    def __init__(self, app):
        self.app = app

    def fork(
        self,
        source,
        destination,
        plan=None,
        buckets=None,
        knowledge=None,
        mode=None,
        **options,
    ):
        original = self.app.catalog.pipeline(source)
        plan = plan if plan is not None else original["plan"]
        cutoff = call_knowledge_spec(knowledge, options)
        if cutoff is not None or mode is not None:
            if "dimension" not in plan:
                raise Invalid("knowledge cutoff requires a dimension join plan")
            dimension = dict(plan["dimension"])
            if mode is not None:
                dimension = {**dimension, "mode": mode}
                if normalize_join_mode(mode) == "realtime" and cutoff is None:
                    # Explicitly unpinning falls back to the historical
                    # realtime join instead of rejecting the old cutoff.
                    dimension.pop("knowledge", None)
            if cutoff is not None:
                dimension = {**dimension, "knowledge": cutoff}
            plan = {**plan, "dimension": dimension}
        self.app.engine.create(
            destination,
            original["channel"],
            plan,
            original["buckets"] if buckets is None else buckets,
        )
        result = self.app.engine.drain(destination)
        return {
            "pipeline": destination,
            "replayed": result["processed"],
            "status": self.app.engine.status(destination),
        }

    def compare(self, left, right):
        with self.app.catalog.transaction() as connection:
            a = self.app.results.rows(left, connection=connection)
            b = self.app.results.rows(right, connection=connection)
        first = {row["result_key"]: row["document"] for row in a}
        second = {row["result_key"]: row["document"] for row in b}
        changed = []
        for key in sorted(set(first) | set(second)):
            if first.get(key) != second.get(key):
                changed.append(
                    {
                        "result_key": key,
                        "left": first.get(key),
                        "right": second.get(key),
                    }
                )
        return {
            "left": left,
            "right": right,
            "equal": not changed,
            "differences": changed,
        }

    def disposition_diff(self, left, right):
        with self.app.catalog.transaction() as connection:
            output = []
            left_info = self.app.catalog.pipeline(left, connection)
            right_info = self.app.catalog.pipeline(right, connection)
            if left_info["channel"] != right_info["channel"]:
                raise Invalid("disposition comparison requires the same source channel")
            states = []
            for name in [left, right]:
                rows = connection.execute(
                    "SELECT event_id,disposition,transformed FROM processed_events WHERE pipeline=?",
                    (name,),
                )
                states.append({row[0]: [row[1], row[2]] for row in rows})
            for event_id in sorted(set(states[0]) | set(states[1])):
                if states[0].get(event_id) != states[1].get(event_id):
                    output.append(
                        {
                            "event_id": event_id,
                            "left": states[0].get(event_id),
                            "right": states[1].get(event_id),
                        }
                    )
        return output

    def source_multiset(self, pipeline):
        with self.app.catalog.transaction() as connection:
            self.app.catalog.pipeline(pipeline, connection)
            rows = connection.execute(
                "SELECT transformed FROM processed_events WHERE pipeline=? AND disposition=?",
                (pipeline, "accepted"),
            )
            counts = Counter(row[0] for row in rows)
        return {
            "distinct": len(counts),
            "rows": sum(counts.values()),
            "sha256": fingerprint(sorted(counts.items())),
        }
