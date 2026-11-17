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


def plan_spec(document, extensions=None):
    extensions = dict(extensions or {})
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
        ]
        + sorted(extensions),
    )
    if not all(isinstance(name, str) and name for name in extensions):
        raise Invalid("plan extension keys must be nonempty strings")
    for name, validate in extensions.items():
        if name in {
            "mapping",
            "metrics",
            "delay_ms",
            "dedup",
            "dimension",
            "sequence",
            "max_window_events",
          }:
            raise Invalid("plan extension may not shadow a built-in key: " + name)
        if not callable(validate):
            raise Invalid("plan extension requires a validator: " + name)
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
    for name, validate in extensions.items():
        if name in document:
            result[name] = validate(document[name])
    return result
