from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import re


class Invalid(ValueError):
    pass


class Conflict(RuntimeError):
    pass


class Missing(LookupError):
    pass


def canonical(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def identifier(value, label="identifier"):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z][A-Za-z0-9_.-]{0,79}", value
    ):
        raise Invalid(label + " must be a bounded identifier")
    return value


def integer(value, label, minimum=None, maximum=None):
    if type(value) is not int:
        raise Invalid(label + " must be an integer")
    if minimum is not None and value < minimum:
        raise Invalid(label + " is below its lower bound")
    if maximum is not None and value > maximum:
        raise Invalid(label + " is above its upper bound")
    return value


def decimal(value):
    if type(value) not in {str, int, Decimal}:
        raise Invalid("decimal must be an integer or decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise Invalid("invalid decimal") from error
    if not result.is_finite():
        raise Invalid("decimal must be finite")
    with localcontext() as context:
        context.prec = 76
        result = result.normalize()
    digits = len(result.as_tuple().digits)
    exponent = result.as_tuple().exponent
    if digits > 38 or abs(exponent) > 38 or digits + max(exponent, 0) > 38:
        raise Invalid("decimal exceeds supported precision")
    return result


def decimal_text(value):
    result = decimal(value)
    if result == 0:
        return "0"
    text = format(result, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def arithmetic(op, left, right):
    with localcontext() as context:
        context.prec = 76
        a, b = decimal(left), decimal(right)
        if op == "add":
            value = a + b
        elif op == "sub":
            value = a - b
        elif op == "mul":
            value = a * b
        elif op == "div":
            if b == 0:
                raise Invalid("division by zero")
            value = a / b
            value = value.quantize(Decimal("0.000000000001"))
        else:
            raise Invalid("unsupported arithmetic")
    return decimal_text(value)


def millis(value):
    if type(value) is int:
        return integer(value, "event time", -(2**62), 2**62)
    if not isinstance(value, str):
        raise Invalid("time must be integer milliseconds or timezone-aware ISO text")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise Invalid("invalid ISO time") from error
    if parsed.tzinfo is None:
        raise Invalid("time requires an explicit timezone")
    delta = parsed.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    if delta.microseconds % 1000:
        raise Invalid("sub-millisecond time is not supported")
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def json_value(value, depth=0):
    if depth > 12:
        raise Invalid("attribute nesting is too deep")
    if value is None or type(value) in {bool, int, str}:
        return value
    if isinstance(value, list):
        if len(value) > 1000:
            raise Invalid("attribute array is too large")
        return [json_value(item, depth + 1) for item in value]
    if isinstance(value, dict):
        if len(value) > 200 or not all(isinstance(key, str) for key in value):
            raise Invalid("invalid attribute object")
        return {key: json_value(item, depth + 1) for key, item in value.items()}
    raise Invalid("attributes support exact JSON values without floats")


def event(raw):
    if not isinstance(raw, dict):
        raise Invalid("event must be an object")
    allowed = {"id", "key", "time", "value", "kind", "attrs"}
    if set(raw) - allowed:
        raise Invalid("unknown event fields")
    event_id = raw.get("id")
    key = raw.get("key")
    if not isinstance(event_id, str) or not 1 <= len(event_id) <= 160:
        raise Invalid("event id is required")
    if not isinstance(key, str) or not 1 <= len(key) <= 200:
        raise Invalid("event key is required")
    kind = raw.get("kind", "reading")
    if not isinstance(kind, str) or not 1 <= len(kind) <= 80:
        raise Invalid("event kind is required")
    attrs = raw.get("attrs", {})
    if not isinstance(attrs, dict):
        raise Invalid("event attrs must be an object")
    result = {
        "id": event_id,
        "key": key,
        "time": millis(raw.get("time")),
        "value": None if raw.get("value") is None else decimal_text(raw["value"]),
        "kind": kind,
        "attrs": json_value(attrs),
    }
    if len(canonical(result).encode()) > 65536:
        raise Invalid("event exceeds 64 KiB")
    return result


def require_keys(value, required, optional=()):
    if (
        not isinstance(value, dict)
        or set(required) - set(value)
        or set(value) - set(required) - set(optional)
    ):
        raise Invalid("object has missing or unknown fields")
    return value
