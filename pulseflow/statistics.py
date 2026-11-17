from collections import Counter
from decimal import Decimal, localcontext

from .values import Invalid, canonical, decimal, decimal_text


def rank_for(count, numerator, denominator=100):
    return max(1, (count * numerator + denominator - 1) // denominator)


def percentile(values, numerator, denominator=100):
    if not values:
        return None
    return decimal_text(sorted(values)[rank_for(len(values), numerator, denominator) - 1])


def render_metrics(
    metrics, count, count_value, total, minimum, maximum, distinct, percentile_at
):
    """Build the metric document shared by in-memory and spilled aggregation."""
    result = {}
    with localcontext() as context:
        context.prec = 76
        for metric in metrics:
            if metric == "count":
                result[metric] = count
            elif metric == "count_value":
                result[metric] = count_value
            elif metric == "sum":
                result[metric] = decimal_text(total) if count_value else None
            elif metric == "mean":
                result[metric] = (
                    decimal_text(
                        (total / count_value).quantize(Decimal("0.000000000001"))
                    )
                    if count_value
                    else None
                )
            elif metric == "min":
                result[metric] = decimal_text(minimum) if count_value else None
            elif metric == "max":
                result[metric] = decimal_text(maximum) if count_value else None
            elif metric == "p50":
                result[metric] = percentile_at(50)
            elif metric == "p95":
                result[metric] = percentile_at(95)
            elif metric == "distinct":
                result[metric] = distinct
            else:
                raise Invalid("unknown metric")
    return result


def summarize(rows, metrics):
    values = [decimal(row["value"]) for row in rows if row["value"] is not None]
    with localcontext() as context:
        context.prec = 76
        total = sum(values, Decimal(0))
    return render_metrics(
        metrics,
        len(rows),
        len(values),
        total,
        min(values) if values else None,
        max(values) if values else None,
        len(set(values)),
        lambda numerator: percentile(values, numerator),
    )


class RunningTotals:
    """Exact incremental aggregates fed one stored row at a time.

    Window rows stream back from catalog storage in budget-bounded chunks, so
    a window of any size is aggregated without holding its rows in a Python
    collection. Additions must run under the same 76-digit decimal context as
    the in-memory path, which keeps the spilled sum bit-identical to it.
    """

    def __init__(self):
        self.count = 0
        self.count_value = 0
        self.total = Decimal(0)
        self.minimum = None
        self.maximum = None

    def add(self, text):
        self.count += 1
        if text is None:
            return
        value = decimal(text)
        self.count_value += 1
        self.total += value
        if self.minimum is None or value < self.minimum:
            self.minimum = value
        if self.maximum is None or value > self.maximum:
            self.maximum = value


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
