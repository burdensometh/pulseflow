from pathlib import Path

from .artifacts import Artifacts
from .catalog import Catalog
from .consistency import Consistency
from .dimensions import Dimensions
from .engine import Engine
from .exchange import JournalExchange
from .exports import Exports
from .journal import Journal
from .operators import Operators
from .queries import Queries
from .replay import Replay
from .releases import Releases
from .results import Results
from .rollups import Rollups
from .savepoints import Savepoints
from .state import KeyedState, Leases
from .sqlite_source import SQLiteIngress
from .streams import FileIngress
from .watermarks import Watermarks
from .windows import Windows


class PulseFlow:
    def __init__(self, home):
        self.home = Path(home).resolve()
        self.catalog = Catalog(self.home)
        self.journal = Journal(self.catalog)
        self.dimensions = Dimensions(self.catalog)
        self.results = Results(self.catalog)
        self.state = KeyedState(self.catalog)
        self.leases = Leases(self.catalog)
        self.watermarks = Watermarks(self.catalog)
        self.windows = Windows(self.catalog, self.results)
        self.operators = Operators(self.state, self.dimensions, self.results)
        self.engine = Engine(
            self.catalog,
            self.journal,
            self.watermarks,
            self.windows,
            self.operators,
            self.leases,
        )
        self.artifacts = Artifacts(self.home / "artifacts")
        self.savepoints = Savepoints(self.catalog, self.journal, self.artifacts)
        self.exports = Exports(self.catalog, self.results, self.artifacts)
        self.files = FileIngress(self.catalog, self.journal)
        self.sqlite = SQLiteIngress(self.catalog, self.journal)
        self.exchange = JournalExchange(self.catalog, self.journal, self.artifacts)
        self.rollups = Rollups(self.catalog, self.results)
        self.replay = Replay(self)
        self.queries = Queries(self)
        self.consistency = Consistency(self)
        self.releases = Releases(self)
