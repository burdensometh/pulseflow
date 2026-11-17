from decimal import Decimal
import unittest

from pulseflow.expressions import Expression, Mapping, field, logical, truth
from pulseflow.spec import plan_spec, sequence_spec, window_spec
from pulseflow.statistics import distribution, percentile, summarize, topk
from pulseflow.values import (
    Invalid,
    arithmetic,
    canonical,
    decimal,
    decimal_text,
    event,
    fingerprint,
    identifier,
    integer,
    json_value,
    millis,
    require_keys,
)


class ValuesTest(unittest.TestCase):
    def test_exact_numbers_and_timezone(self):
        self.assertEqual(decimal_text("12.5000"), "12.5")
        self.assertEqual(decimal_text("-0.00"), "0")
        self.assertEqual(arithmetic("add", "0.1", "0.2"), "0.3")
        self.assertEqual(arithmetic("sub", "0.1", "0.2"), "-0.1")
        self.assertEqual(arithmetic("mul", "1.5", "2"), "3")
        self.assertEqual(arithmetic("div", "1", "3"), "0.333333333333")
        self.assertEqual(millis("1970-01-01T08:00:00+08:00"), 0)
        self.assertEqual(millis("1969-12-31T23:59:59.999Z"), -1)
        self.assertEqual(millis(-1), -1)
        for value in [True, 1.2, "NaN", "Infinity", "1e100", "x"]:
            with self.subTest(value=value), self.assertRaises(Invalid):
                decimal(value)
        for value in ["2026-11-01", None, "bad", "1970-01-01T00:00:00.000001Z"]:
            with self.subTest(value=value), self.assertRaises(Invalid):
                millis(value)
        for op, a, b in [("div", "1", "0"), ("unknown", "1", "2")]:
            with self.assertRaises(Invalid):
                arithmetic(op, a, b)

    def test_envelope_and_bounded_json(self):
        row = event({"id": "a", "key": "s", "time": 0, "value": "1.20"})
        self.assertEqual(row["value"], "1.2")
        self.assertEqual(row["attrs"], {})
        self.assertEqual(identifier("abc.1"), "abc.1")
        self.assertEqual(integer(2, "n", 1, 3), 2)
        self.assertEqual(require_keys({"a": 1}, ["a"]), {"a": 1})
        self.assertEqual(json_value({"a": [None, False, 0]}), {"a": [None, False, 0]})
        self.assertEqual(fingerprint({"b": 2, "a": 1}), fingerprint({"a": 1, "b": 2}))
        invalid = [
            None,
            {},
            {**row, "other": 1},
            {**row, "key": ""},
            {**row, "kind": ""},
            {**row, "attrs": []},
        ]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(Invalid):
                event(value)
        for value in [True, 0, 4]:
            with self.assertRaises(Invalid):
                integer(value, "n", 1, 3)
        with self.assertRaises(Invalid):
            identifier("../bad")
        for value in [1.2, list(range(1001)), {str(i): i for i in range(201)}]:
            with self.assertRaises(Invalid):
                json_value(value)
        nested = 0
        for _ in range(15):
            nested = [nested]
        with self.assertRaises(Invalid):
            json_value(nested)
        with self.assertRaises(Invalid):
            require_keys({"b": 2}, ["a"])
        with self.assertRaises(Invalid):
            event({**row, "attrs": {"large": "x" * 65536}})

    def test_three_valued_and_expression_operators(self):
        literal = lambda value: {"literal": value}

        def evaluate(op, *values, **kwargs):
            return Expression(
                {"op": op, "args": [literal(v) for v in values], **kwargs}
            ).evaluate({})

        self.assertIsNone(logical("and", [True, None]))
        self.assertFalse(logical("and", [False, None]))
        self.assertTrue(logical("or", [True, None]))
        self.assertIsNone(logical("or", [False, None]))
        self.assertIsNone(evaluate("not", None))
        self.assertFalse(evaluate("not", True))
        self.assertTrue(evaluate("is_null", None))
        for op, expected in [
            ("eq", False),
            ("ne", True),
            ("lt", True),
            ("le", True),
            ("gt", False),
            ("ge", False),
        ]:
            self.assertEqual(evaluate(op, "2", "10", numeric=True), expected)
        self.assertEqual(evaluate("coalesce", None, 0, 3), 0)
        self.assertIsNone(evaluate("coalesce", None, None))
        self.assertEqual(evaluate("if", None, "yes", "no"), "no")
        self.assertEqual(evaluate("if", True, "yes", "no"), "yes")
        self.assertEqual(evaluate("concat", "a", "b"), "ab")
        self.assertEqual(evaluate("decimal", "2.00"), "2")
        self.assertEqual(evaluate("abs", "-3"), "3")
        self.assertEqual(evaluate("lower", "ABC"), "abc")
        self.assertEqual(evaluate("length", [1, 2]), 2)
        self.assertTrue(evaluate("contains", "abc", "b"))
        self.assertTrue(evaluate("in", 2, [1, 2]))
        self.assertIsNone(evaluate("add", None, 1))
        self.assertEqual(Expression({"field": "a.b"}).evaluate({"a": {"b": 0}}), 0)
        self.assertIsNone(field({"a": 3}, "a.b"))
        for op, values in [
            ("lower", [1]),
            ("length", [1]),
            ("concat", ["a", 1]),
            ("contains", [1, "a"]),
            ("in", [1, 3]),
            ("eq", [1, "1"]),
        ]:
            with self.subTest(op=op), self.assertRaises(Invalid):
                evaluate(op, *values)
        with self.assertRaises(Invalid):
            truth(1)

    def test_expression_validation_and_mapping(self):
        row = event({"id": "e", "key": "A", "time": 0, "value": "2"})
        mapper = Mapping(
            {
                "key": {"op": "lower", "args": [{"field": "key"}]},
                "value": {"op": "mul", "args": [{"field": "value"}, {"literal": "3"}]},
                "attrs": {"zero": {"literal": 0}},
            }
        )
        self.assertEqual(mapper.apply(row)["value"], "6")
        self.assertEqual(mapper.apply(row)["key"], "a")
        self.assertEqual(mapper.apply(row)["attrs"]["zero"], 0)
        self.assertIsNone(Mapping({"keep": {"literal": False}}).apply(row))
        self.assertIsNone(Mapping({"value": {"literal": None}}).apply(row)["value"])
        for expression in [
            None,
            {"field": ""},
            {"op": "unknown", "args": []},
            {"op": "add", "args": []},
            {"op": "not", "args": [{}]},
            {"op": "eq", "args": [{"literal": 1}, {"literal": 1}], "numeric": 1},
        ]:
            with self.subTest(expression=expression), self.assertRaises(Invalid):
                Expression(expression)
        for mapping in [None, {"other": 1}, {"attrs": []}]:
            with self.assertRaises(Invalid):
                Mapping(mapping)
        with self.assertRaises(Invalid):
            Mapping({"key": {"literal": None}}).apply(row)

    def test_plan_and_statistics(self):
        plan = plan_spec({"window": {"kind": "hopping", "size_ms": 10, "slide_ms": 3}})
        self.assertEqual(plan["delay_ms"], 0)
        self.assertEqual(plan["window"]["offset_ms"], 0)
        for bad in [
            {"kind": "session", "size_ms": 10},
            {"kind": "tumbling", "size_ms": 10, "slide_ms": 3},
            {"kind": "hopping", "size_ms": 1001, "slide_ms": 1},
        ]:
            with self.assertRaises(Invalid):
                window_spec(bad)
        with self.assertRaises(Invalid):
            sequence_spec({"first_kind": "a", "then_kind": "a", "within_ms": 10})
        for extra in [
            {"metrics": ["nope"]},
            {"dedup": {"field": "", "ttl_ms": 2}},
            {"dimension": {"name": "d", "missing": "bad"}},
        ]:
            with self.assertRaises(Invalid):
                plan_spec({"window": {"kind": "tumbling", "size_ms": 10}, **extra})
        rows = [
            {"value": "2", "key": "b"},
            {"value": "10", "key": "a"},
            {"value": None, "key": "c"},
            {"value": "2", "key": "a"},
        ]
        result = summarize(
            rows,
            [
                "count",
                "count_value",
                "sum",
                "mean",
                "min",
                "max",
                "p50",
                "p95",
                "distinct",
            ],
        )
        self.assertEqual(
            result,
            {
                "count": 4,
                "count_value": 3,
                "sum": "14",
                "mean": "4.666666666667",
                "min": "2",
                "max": "10",
                "p50": "2",
                "p95": "10",
                "distinct": 2,
            },
        )
        self.assertEqual([row["key"] for row in topk(rows, 3)], ["a", "a", "b"])
        self.assertEqual(distribution(rows, "key")[0], {"value": "a", "count": 2})
        self.assertIsNone(percentile([], 50))
        self.assertEqual(
            summarize([], ["sum", "mean", "min", "max", "p50", "p95"]),
            dict.fromkeys(["sum", "mean", "min", "max", "p50", "p95"]),
        )
        with self.assertRaises(Invalid):
            summarize(rows, ["bad"])
        with self.assertRaises(Invalid):
            topk(rows, 0)
