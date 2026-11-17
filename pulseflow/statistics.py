from collections import Counter
from decimal import Decimal, localcontext

from .values import Invalid, canonical, decimal, decimal_text


def percentile(values, numerator, denominator=100):
    if not values:
        return None
    rank = max(1, (len(values) * numerator + denominator - 1) // denominator)
    return decimal_text(sorted(values)[rank - 1])


def summarize(rows, metrics):
    values = [decimal(row["value"]) for row in rows if row["value"] is not None]
    result = {}
    with localcontext() as context:
        context.prec = 76
        total = sum(values, Decimal(0))
        for metric in metrics:
            if metric == "count":
                result[metric] = len(rows)
            elif metric == "count_value":
                result[metric] = len(values)
            elif metric == "sum":
                result[metric] = decimal_text(total) if values else None
            elif metric == "mean":
                result[metric] = (
                    decimal_text(
                        (total / len(values)).quantize(Decimal("0.000000000001"))
                    )
                    if values
                    else None
                )
            elif metric == "min":
                result[metric] = decimal_text(min(values)) if values else None
            elif metric == "max":
                result[metric] = decimal_text(max(values)) if values else None
            elif metric == "p50":
                result[metric] = percentile(values, 50)
            elif metric == "p95":
                result[metric] = percentile(values, 95)
            elif metric == "distinct":
                result[metric] = len(set(values))
            else:
                raise Invalid("unknown metric")
    return result


def distribution(rows, field_name):
    counter = Counter(canonical(row.get(field_name)) for row in rows)
    return [
        {"value": __import__("json").loads(key), "count": value}
        for key, value in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def topk(rows, k, value_field="value", key_field="key"):
    if type(k) is not int or k < 1:
        raise Invalid("topk size must be positive")
    values = []
    for row in rows:
        if row.get(value_field) is not None:
            values.append(
                (decimal(row[value_field]), canonical(row.get(key_field)), row)
            )
    values.sort(key=lambda value: (-value[0], value[1]))
    return [value[2] for value in values[:k]]
