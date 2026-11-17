from pathlib import Path

from .artifacts import Artifacts
from .catalog import Catalog
from .consistency import Consistency
from .dimensions import Dimensions
from .engine import Engine
from .exchange import JournalExchange
from .exports import Exports
from .journal import Journal
from .operators import (
    DedupOperator,
    LatestOperator,
    OperatorRegistry,
    SequenceOperator,
    WindowOperator,
)
from .queries import Queries
from .replay import Replay
from .results import Results
from .rollups import Rollups
from .savepoints import Savepoints
from .state import KeyedState, Leases
from .sqlite_source import SQLiteIngress
from .streams import FileIngress
from .transform import Transformer
from .values import Conflict
from .watermarks import Watermarks
from .windows import Windows


class PulseFlow:
    def __init__(self, home, operators=None):
        self.home = Path(home).resolve()
        self.catalog = Catalog(self.home)
        self.journal = Journal(self.catalog)
        self.dimensions = Dimensions(self.catalog)
        self.results = Results(self.catalog)
        self.state = KeyedState(self.catalog)
        self.leases = Leases(self.catalog)
        self.watermarks = Watermarks(self.catalog)
        self.windows = Windows(self.catalog, self.results)
        self.transform = Transformer(self.dimensions)
        self.operators = OperatorRegistry(
            [
                WindowOperator(self.windows),
                DedupOperator(),
                LatestOperator(self.state),
                SequenceOperator(self.state, self.results),
            ]
        )
        for operator in operators or ():
            self.operators.register(operator)
        self.catalog.install(self.operators.schema())
        self.engine = Engine(
            self.catalog,
            self.journal,
            self.watermarks,
            self.transform,
            self.operators,
            self.leases,
        )
        self.artifacts = Artifacts(self.home / "artifacts")
        self.savepoints = Savepoints(
            self.catalog, self.journal, self.artifacts, self.operators
        )
        self.exports = Exports(self.catalog, self.results, self.artifacts)
        self.files = FileIngress(self.catalog, self.journal)
        self.sqlite = SQLiteIngress(self.catalog, self.journal)
        self.exchange = JournalExchange(self.catalog, self.journal, self.artifacts)
        self.rollups = Rollups(self.catalog, self.results)
        self.replay = Replay(self)
        self.queries = Queries(self)
        self.consistency = Consistency(self)

    def register_operator(self, operator):
        """Register an additional stateful operator before creating pipelines.

        Existing state cannot be re-attributed and pipelines already built
        may have published results, so registration is closed once any
        pipeline exists.
        """

        with self.catalog.transaction() as connection:
            if connection.execute("SELECT 1 FROM pipelines LIMIT 1").fetchone():
                raise Conflict(
                    "operators must be registered before pipelines are created"
                )
        self.operators.register(operator)
        self.catalog.install(self.operators.schema())
        return operator
