from decimal import Decimal, localcontext

from .values import Invalid, decimal, decimal_text, integer, millis


def ordered_samples(rows):
    by_time = {}
    key = None
    for row in rows:
        if key is None:
            key = row["key"]
        if row["key"] != key:
            raise Invalid("a series must belong to a single key")
        at = millis(row["time"])
        if row.get("value") is None:
            continue
        value = decimal(row["value"])
        old = by_time.get(at)
        if old is None or row["id"] > old["id"]:
            by_time[at] = {"time": at, "value": value, "id": row["id"]}
    return [by_time[at] for at in sorted(by_time)]


def value_at(samples, at, mode="step", max_gap=None):
    if mode not in {"step", "linear"}:
        raise Invalid("series interpolation must be step or linear")
    previous = None
    following = None
    for row in samples:
        if row["time"] <= at:
            previous = row
        if row["time"] >= at:
            following = row
            break
    if previous is None:
        return None
    if max_gap is not None and at - previous["time"] > max_gap:
        return None
    if mode == "step" or previous["time"] == at:
        return previous["value"]
    if following is None:
        return None
    gap = following["time"] - previous["time"]
    if max_gap is not None and gap > max_gap:
        return None
    with localcontext() as context:
        context.prec = 76
        proportion = Decimal(at - previous["time"]) / gap
        return previous["value"] + (following["value"] - previous["value"]) * proportion


def resample(rows, start, end, step_ms, mode="step", max_gap=None):
    start, end = millis(start), millis(end)
    integer(step_ms, "series grid step", 1)
    if end < start or (end - start + step_ms - 1) // step_ms > 100000:
        raise Invalid("series grid range is invalid or too large")
    if max_gap is not None:
        integer(max_gap, "maximum interpolation gap", 0)
    samples = ordered_samples(rows)
    result = []
    for at in range(start, end, step_ms):
        value = value_at(samples, at, mode, max_gap)
        if value is not None:
            with localcontext() as context:
                context.prec = 76
                value = value.quantize(Decimal("0.000000000001"))
        result.append(
            {"time": at, "value": decimal_text(value) if value is not None else None}
        )
    return result


def integral(rows, start, end, mode="step", max_gap=None):
    start, end = millis(start), millis(end)
    if end <= start:
        raise Invalid("integration interval must have positive duration")
    samples = ordered_samples(rows)
    points = sorted(
        {start, end} | {row["time"] for row in samples if start < row["time"] < end}
    )
    covered = 0
    total = Decimal(0)
    with localcontext() as context:
        context.prec = 76
        for left, right in zip(points, points[1:]):
            a = value_at(samples, left, mode, max_gap)
            if a is None:
                continue
            if mode == "step":
                if max_gap is not None:
                    previous = max(
                        row["time"] for row in samples if row["time"] <= left
                    )
                    right = min(right, previous + max_gap)
                    if right <= left:
                        continue
                contribution = a * (right - left)
            else:
                b = value_at(samples, right, mode, max_gap)
                if b is None:
                    continue
                contribution = (a + b) * (right - left) / 2
            total += contribution
            covered += right - left
        mean = (
            (total / covered).quantize(Decimal("0.000000000001")) if covered else None
        )
    return {
        "area_value_ms": decimal_text(total),
        "covered_ms": covered,
        "duration_ms": end - start,
        "time_weighted_mean": decimal_text(mean) if mean is not None else None,
    }
