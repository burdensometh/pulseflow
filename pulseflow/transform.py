from .expressions import Mapping


class Transformer:
    """Stateless event shaping: dimension enrichment then expression mapping.

    This runs before the stateful operator chain and is not an operator itself;
    it owns no durable state.
    """

    def __init__(self, dimensions):
        self.dimensions = dimensions

    def apply(self, connection, pipeline, row):
        version = None
        if "dimension" in pipeline["plan"]:
            row, version = self.dimensions.enrich(
                connection, row, pipeline["plan"]["dimension"]
            )
            if row is None:
                return None, version
        mapped = Mapping(pipeline["plan"]["mapping"]).apply(row)
        return mapped, version
