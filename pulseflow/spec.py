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

IDLE_ALIASES = ("idle", "idle_partitions")
IDLE_TIMEOUT_KEYS = (
    "idle_timeout_ms",
    "idle_partition_timeout_ms",
    "idle_partition_ms",
)
IDLE_INNER_KEYS = ("timeout_ms",) + IDLE_TIMEOUT_KEYS + ("enabled",)


def idle_spec(document):
    """Normalize the accepted idle spellings to one canonical shape.

    ``{"idle": {"timeout_ms": n}}`` is the canonical form; ``idle_partitions``
    may be used in place of ``idle``, an integer in place of the object, and a
    top-level ``idle_timeout_ms`` (or one of its aliases) is accepted too.
    Returns ``None`` when idle handling is disabled (the default), otherwise
    ``{"timeout_ms": n}``.  A timeout of zero is valid: a partition with no
    pending records is retired as soon as its silence is observed on a later
    processing-clock reading.
    """

    timeout = None
    nested = None
    for key in IDLE_ALIASES:
        if key in document:
            if nested is not None:
                raise Invalid("idle is configured more than once")
            nested = document[key]
    if nested is not None:
        if nested is False:
            timeout = False
        elif type(nested) is int:
            timeout = integer(nested, "idle timeout", 0, 86400000)
        elif isinstance(nested, dict):
            require_keys(nested, [], list(IDLE_INNER_KEYS))
            enabled = nested.get("enabled", True)
            if not isinstance(enabled, bool):
                raise Invalid("idle enabled must be boolean")
            values = [nested[key] for key in IDLE_TIMEOUT_KEYS if key in nested]
            if "timeout_ms" in nested:
                values.insert(0, nested["timeout_ms"])
            if len(values) > 1:
                raise Invalid("idle timeout is configured more than once")
            if enabled:
                if not values:
                    raise Invalid("idle timeout is required")
                timeout = integer(values[0], "idle timeout", 0, 86400000)
            else:
                timeout = False
        else:
            raise Invalid("idle must be a timeout, an object or false")
    for key in IDLE_TIMEOUT_KEYS:
        if key in document:
            value = integer(document[key], "idle timeout", 0, 86400000)
            if timeout is False:
                raise Invalid("idle is both disabled and configured")
            if timeout is not None and timeout != value:
                raise Invalid("idle timeout is configured more than once")
            timeout = value
    if timeout is None or timeout is False:
        return None
    return {"timeout_ms": timeout}


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


def plan_spec(document):
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
            "idle",
            "idle_partitions",
            "idle_timeout_ms",
            "idle_partition_timeout_ms",
            "idle_partition_ms",
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
    idle = idle_spec(document)
    if idle is not None:
        result["idle"] = idle
    return result
