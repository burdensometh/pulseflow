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

# Cutoff spellings accepted inside a dimension specification. ``None`` means
# the value may be a bare version id, a batch id, ``{"version": n}``,
# ``{"batch": id}`` or ``{"head": true}``; the other entries force a kind.
CUTOFF_KEYS = {
    "knowledge": None,
    "knowledge_cutoff": None,
    "knowledge_as_of": None,
    "dimension_knowledge": None,
    "dimension_cutoff": None,
    "cutoff": None,
    "as_of": None,
    "known_through": None,
    "version": "version",
    "batch": "batch",
    "head": "head",
    "knowledge_version": "version",
    "knowledge_cutoff_version": "version",
    "cutoff_version": "version",
    "cutoff_version_id": "version",
    "as_of_version": "version",
    "knowledge_batch": "batch",
    "knowledge_cutoff_batch": "batch",
    "cutoff_batch": "batch",
    "as_of_batch": "batch",
    "knowledge_from_batch": "batch",
}

JOIN_MODE_KEYS = ("mode", "join_mode", "join")

REALTIME_JOIN_MODES = {
    "realtime",
    "real_time",
    "processing",
    "processing_time",
    "latest",
    "current",
    "now",
}

KNOWLEDGE_JOIN_MODES = {
    "knowledge",
    "knowledge_time",
    "knowledge_aware",
    "asof",
    "as_of",
    "bitemporal",
    "snapshot",
    "pinned",
    "versioned",
    "cutoff",
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


def normalize_join_mode(mode):
    """Classify a join mode as ``"realtime"`` or ``"knowledge"``."""
    if not isinstance(mode, str):
        raise Invalid("dimension join mode must be text")
    normalized = mode.lower().replace("-", "_").replace(" ", "_")
    if normalized in REALTIME_JOIN_MODES:
        return "realtime"
    if normalized in KNOWLEDGE_JOIN_MODES:
        return "knowledge"
    raise Invalid("unknown dimension join mode")


def knowledge_spec(value):
    """Normalize a knowledge cutoff.

    The normalized form is exactly one of

    * ``{"version": n}`` - inclusive dimension ``version_id`` ceiling;
    * ``{"batch": batch_id}`` - the versions committed by one publish batch;
    * ``{"head": True}`` - the head observed when a pipeline is created.

    Batch and head cutoffs are resolved to a concrete version id when the
    pipeline is created, so every later chunk reuses the same knowledge.
    """
    if type(value) is int:
        return {"version": integer(value, "dimension knowledge cutoff", 0, 2**62)}
    if isinstance(value, str):
        return {"batch": identifier(value, "dimension knowledge batch")}
    if value is True:
        return {"head": True}
    if not isinstance(value, dict) or not value or len(value) > 1 or set(value) - {
        "version",
        "version_id",
        "batch",
        "batch_id",
        "head",
    }:
        raise Invalid(
            "dimension knowledge cutoff must name one version, batch or head"
        )
    if "version" in value or "version_id" in value:
        return {
            "version": integer(
                value.get("version", value.get("version_id")),
                "dimension knowledge cutoff",
                0,
                2**62,
            )
        }
    if "batch" in value or "batch_id" in value:
        return {
            "batch": identifier(
                value.get("batch", value.get("batch_id")),
                "dimension knowledge batch",
            )
        }
    if value["head"] is not True:
        raise Invalid("dimension knowledge cutoff head must be true")
    return {"head": True}


def cutoff_from(mapping, keys=CUTOFF_KEYS):
    """Extract at most one knowledge cutoff from a mapping of aliases.

    A key whose value is ``None`` is treated as absent, so clients may pass
    optional fields without accidentally asking for the null cutoff.
    """
    if not isinstance(mapping, dict):
        raise Invalid("knowledge cutoff must be supplied in an object")
    selected = [
        name for name in keys if name in mapping and mapping[name] is not None
    ]
    if not selected:
        return None
    if len(selected) > 1:
        raise Invalid("dimension knowledge cutoff was specified more than once")
    name = selected[0]
    value = mapping[name]
    kind = keys[name]
    if kind is None:
        return knowledge_spec(value)
    return knowledge_spec({kind: value})


def dimension_knowledge_spec(dimension, extra=None):
    """Extract the dimension join mode and its optional knowledge cutoff.

    ``extra`` carries plan-level cutoff aliases. Naming any cutoff implies
    the knowledge mode; ``mode: realtime`` rejects cutoffs, and a knowledge
    mode requires one.
    """
    if not isinstance(dimension, dict):
        raise Invalid("dimension join specification must be an object")
    modes = [name for name in JOIN_MODE_KEYS if name in dimension]
    if len(modes) > 1:
        raise Invalid("dimension join mode was specified more than once")
    mode = dimension[modes[0]] if modes else None
    knowledge = cutoff_from(dimension)
    extra_knowledge = cutoff_from(extra) if extra else None
    if knowledge is not None and extra_knowledge is not None:
        raise Invalid("dimension knowledge cutoff was specified more than once")
    knowledge = knowledge or extra_knowledge
    if mode is not None:
        if normalize_join_mode(mode) == "realtime":
            if knowledge is not None:
                raise Invalid("realtime dimension join cannot use a knowledge cutoff")
            return None
        if knowledge is None:
            # A knowledge join without an explicit cutoff pins the
            # dimension knowledge head observed at pipeline creation.
            knowledge = {"head": True}
    return knowledge


def plan_join_mode(plan):
    """Return the plan's join mode: ``"realtime"`` or ``"knowledge"``."""
    dimension = plan.get("dimension") if isinstance(plan, dict) else None
    if not isinstance(dimension, dict):
        return None
    modes = [name for name in JOIN_MODE_KEYS if name in dimension]
    if modes:
        return normalize_join_mode(dimension[modes[0]])
    return "knowledge" if "knowledge" in dimension else "realtime"


PLAN_KNOWLEDGE_KEYS = (
    "knowledge",
    "knowledge_cutoff",
    "knowledge_as_of",
    "known_through",
    "dimension_knowledge",
    "dimension_cutoff",
    "as_of",
    "knowledge_version",
    "knowledge_batch",
    "knowledge_cutoff_version",
    "knowledge_cutoff_batch",
    "cutoff",
    "cutoff_version",
    "cutoff_version_id",
    "cutoff_batch",
    "as_of_version",
    "as_of_batch",
)


def call_knowledge_spec(knowledge=None, options=None):
    """Normalize a cutoff passed as call arguments to create/fork.

    ``knowledge`` is the primary argument; ``options`` may carry any of the
    cutoff aliases as keyword arguments. Naming two cutoffs is ambiguous and
    rejected. Returns a normalized cutoff or ``None``.
    """
    options = {} if options is None else options
    unknown = set(options) - set(CUTOFF_KEYS)
    if unknown:
        raise Invalid("unknown plan option: " + sorted(unknown)[0])
    extra = cutoff_from(options) if options else None
    if knowledge is not None and extra is not None:
        raise Invalid("dimension knowledge cutoff was specified more than once")
    if knowledge is None:
        return extra
    return cutoff_from({"knowledge": knowledge})


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
            *PLAN_KNOWLEDGE_KEYS,
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
        require_keys(
            dimension,
            ["name"],
            ["missing", *JOIN_MODE_KEYS, *CUTOFF_KEYS],
        )
        result["dimension"] = {
            "name": identifier(dimension["name"]),
            "missing": dimension.get("missing", "null"),
        }
        if result["dimension"]["missing"] not in {"null", "drop", "error"}:
            raise Invalid("dimension missing policy must be null, drop or error")
        knowledge = dimension_knowledge_spec(
            dimension,
            {key: document[key] for key in PLAN_KNOWLEDGE_KEYS if key in document},
        )
        if knowledge is not None:
            result["dimension"]["mode"] = "knowledge"
            result["dimension"]["knowledge"] = knowledge
    elif any(key in document for key in PLAN_KNOWLEDGE_KEYS):
        raise Invalid("knowledge cutoff requires a dimension join plan")
    if "sequence" in document:
        result["sequence"] = sequence_spec(document["sequence"])
    return result
