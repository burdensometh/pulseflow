from .expressions import Mapping
from .values import Invalid, identifier, integer, require_keys


METRICS = {
    "count",
    "count_value",
    "sum",
    "mean",
    "min",
    "max",
    "p50",
    "p95",
    "distinct",
}


def window_spec(value):
    require_keys(value, ["kind", "size_ms"], ["slide_ms", "offset_ms"])
    kind = value["kind"]
    if kind not in {"tumbling", "hopping"}:
        raise Invalid("only tumbling and hopping windows are available")
    size = integer(value["size_ms"], "window size", 1, 86400000)
    slide = integer(value.get("slide_ms", size), "window slide", 1, size)
    if kind == "tumbling" and slide != size:
        raise Invalid("tumbling window slide must equal its size")
    offset = integer(value.get("offset_ms", 0), "window offset", -86400000, 86400000)
    if (size + slide - 1) // slide > 1000:
        raise Invalid("a row may not belong to more than 1000 windows")
    return {"kind": kind, "size_ms": size, "slide_ms": slide, "offset_ms": offset}


def sequence_spec(value):
    require_keys(value, ["first_kind", "then_kind", "within_ms"])
    for key in ["first_kind", "then_kind"]:
        if not isinstance(value[key], str) or not value[key]:
            raise Invalid("sequence event kinds must be nonempty strings")
    if value["first_kind"] == value["then_kind"]:
        raise Invalid("sequence kinds must differ")
    return {
        **value,
        "within_ms": integer(value["within_ms"], "sequence bound", 1, 86400000),
    }


def join_spec(value):
    require_keys(
        value,
        ["left_channel", "right_channel", "before_ms", "after_ms"],
        ["outer", "delay_ms", "max_buffer_events"],
    )
    left = identifier(value["left_channel"], "left join channel")
    right = identifier(value["right_channel"], "right join channel")
    if left == right:
        raise Invalid("join channels must differ")
    before = integer(value["before_ms"], "join lookback", 0, 86400000)
    after = integer(value["after_ms"], "join lookahead", 0, 86400000)
    if before + after < 1:
        raise Invalid("join interval must span at least one millisecond")
    outer = value.get("outer", False)
    if type(outer) is not bool:
        raise Invalid("join outer flag must be boolean")
    return {
        "left_channel": left,
        "right_channel": right,
        "before_ms": before,
        "after_ms": after,
        "outer": outer,
        "delay_ms": integer(value.get("delay_ms", 0), "watermark delay", 0, 86400000),
        "max_buffer_events": integer(
            value.get("max_buffer_events", 100000), "join buffer limit", 1, 1000000
        ),
    }


def plan_spec(document):
    if isinstance(document, dict) and "join" in document:
        require_keys(document, ["join"])
        return {"join": join_spec(document["join"])}
    require_keys(
        document,
        ["window"],
        [
            "mapping",
            "metrics",
            "delay_ms",
            "dedup",
            "dimension",
            "sequence",
            "max_window_events",
        ],
    )
    mapping = document.get("mapping", {})
    Mapping(mapping)
    metrics = document.get("metrics", ["count", "sum", "mean"])
    if (
        not isinstance(metrics, list)
        or not metrics
        or len(set(metrics)) != len(metrics)
        or set(metrics) - METRICS
    ):
        raise Invalid("invalid aggregate metrics")
    delay = integer(document.get("delay_ms", 0), "watermark delay", 0, 86400000)
    maximum = integer(
        document.get("max_window_events", 100000), "window event limit", 1, 1000000
    )
    result = {
        "window": window_spec(document["window"]),
        "mapping": mapping,
        "metrics": metrics,
        "delay_ms": delay,
        "max_window_events": maximum,
    }
    if "dedup" in document:
        dedup = document["dedup"]
        require_keys(dedup, ["field", "ttl_ms"])
        if not isinstance(dedup["field"], str) or not dedup["field"]:
            raise Invalid("dedup field is required")
        result["dedup"] = {
            "field": dedup["field"],
            "ttl_ms": integer(dedup["ttl_ms"], "dedup TTL", 1, 86400000),
        }
    if "dimension" in document:
        dimension = document["dimension"]
        require_keys(dimension, ["name"], ["missing"])
        result["dimension"] = {
            "name": identifier(dimension["name"]),
            "missing": dimension.get("missing", "null"),
        }
        if result["dimension"]["missing"] not in {"null", "drop", "error"}:
            raise Invalid("dimension missing policy must be null, drop or error")
    if "sequence" in document:
        result["sequence"] = sequence_spec(document["sequence"])
    return result
