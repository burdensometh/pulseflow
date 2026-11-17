from .values import Invalid, arithmetic, decimal, json_value


def field(document, path):
    value = document
    for component in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(component)
    return value


def truth(value):
    if value is not None and type(value) is not bool:
        raise Invalid("predicate must produce boolean or null")
    return value


def logical(op, values):
    values = [truth(v) for v in values]
    if op == "not":
        return None if values[0] is None else not values[0]
    if op == "and":
        if False in values:
            return False
        return None if None in values else True
    if True in values:
        return True
    return None if None in values else False


class Expression:
    ARITHMETIC = {"add", "sub", "mul", "div"}
    COMPARISON = {"eq", "ne", "lt", "le", "gt", "ge"}
    UNARY = {"not", "is_null", "decimal", "lower", "length", "abs"}

    def __init__(self, document):
        self.document = self.validate(document)

    def validate(self, node, depth=0):
        if depth > 20 or not isinstance(node, dict):
            raise Invalid("invalid expression tree")
        if set(node) == {"literal"}:
            json_value(node["literal"])
            return node
        if set(node) == {"field"}:
            path = node["field"]
            if not isinstance(path, str) or not path or len(path) > 200:
                raise Invalid("invalid field expression")
            return node
        if (
            set(node) - {"op", "args", "numeric"}
            or "op" not in node
            or "args" not in node
        ):
            raise Invalid("invalid operator expression")
        op, args = node["op"], node["args"]
        if not isinstance(op, str) or not isinstance(args, list):
            raise Invalid("invalid expression arguments")
        if op in self.UNARY:
            expected = len(args) == 1
        elif op in self.ARITHMETIC | self.COMPARISON | {"contains", "in"}:
            expected = len(args) == 2
        elif op == "if":
            expected = len(args) == 3
        elif op in {"and", "or", "coalesce", "concat"}:
            expected = 1 <= len(args) <= 30
        else:
            raise Invalid("unsupported expression operator")
        if not expected:
            raise Invalid("expression argument count mismatch")
        if type(node.get("numeric", False)) is not bool:
            raise Invalid("numeric comparison flag must be boolean")
        for argument in args:
            self.validate(argument, depth + 1)
        return node

    def evaluate(self, row):
        return self.eval(self.document, row)

    def eval(self, node, row):
        if "literal" in node:
            return node["literal"]
        if "field" in node:
            return field(row, node["field"])
        op, args = node["op"], node["args"]
        if op == "if":
            selected = args[1] if truth(self.eval(args[0], row)) is True else args[2]
            return self.eval(selected, row)
        if op == "coalesce":
            for arg in args:
                value = self.eval(arg, row)
                if value is not None:
                    return value
            return None
        values = [self.eval(arg, row) for arg in args]
        if op in {"and", "or", "not"}:
            return logical(op, values)
        if op == "is_null":
            return values[0] is None
        if any(value is None for value in values):
            return None
        if op in self.ARITHMETIC:
            return arithmetic(op, *values)
        if op in self.COMPARISON:
            left, right = values
            if node.get("numeric"):
                left, right = decimal(left), decimal(right)
            elif type(left) is not type(right):
                raise Invalid("comparison requires equal types or numeric=true")
            operations = {
                "eq": lambda: left == right,
                "ne": lambda: left != right,
                "lt": lambda: left < right,
                "le": lambda: left <= right,
                "gt": lambda: left > right,
                "ge": lambda: left >= right,
            }
            try:
                return operations[op]()
            except TypeError as error:
                raise Invalid("values are not comparable") from error
        if op == "decimal":
            return arithmetic("add", values[0], "0")
        if op == "abs":
            return arithmetic("mul", values[0], "-1" if decimal(values[0]) < 0 else "1")
        if op == "lower":
            if not isinstance(values[0], str):
                raise Invalid("lower requires a string")
            return values[0].lower()
        if op == "length":
            if not isinstance(values[0], (str, list, dict)):
                raise Invalid("length requires a string or collection")
            return len(values[0])
        if op == "concat":
            if not all(isinstance(value, str) for value in values):
                raise Invalid("concat requires strings")
            return "".join(values)
        if op == "contains":
            if not isinstance(values[0], str) or not isinstance(values[1], str):
                raise Invalid("contains requires strings")
            return values[1] in values[0]
        if op == "in":
            if not isinstance(values[1], list):
                raise Invalid("in requires an array")
            return values[0] in values[1]
        raise Invalid("unsupported expression")


class Mapping:
    def __init__(self, document):
        if not isinstance(document, dict) or set(document) - {
            "value",
            "key",
            "attrs",
            "keep",
        }:
            raise Invalid("invalid mapping")
        attrs = document.get("attrs", {})
        if not isinstance(attrs, dict) or not all(isinstance(k, str) for k in attrs):
            raise Invalid("mapping attrs must be named expressions")
        self.value = Expression(document["value"]) if "value" in document else None
        self.key = Expression(document["key"]) if "key" in document else None
        self.keep = Expression(document["keep"]) if "keep" in document else None
        self.attrs = {key: Expression(expr) for key, expr in attrs.items()}

    def apply(self, row):
        if self.keep and truth(self.keep.evaluate(row)) is not True:
            return None
        result = dict(row)
        result["attrs"] = dict(row["attrs"])
        if self.value:
            result["value"] = self.value.evaluate(row)
            if result["value"] is not None:
                result["value"] = arithmetic("add", result["value"], "0")
        if self.key:
            result["key"] = self.key.evaluate(row)
            if not isinstance(result["key"], str) or not result["key"]:
                raise Invalid("mapped key must be a nonempty string")
        for key, expression in self.attrs.items():
            result["attrs"][key] = json_value(expression.evaluate(row))
        return result
