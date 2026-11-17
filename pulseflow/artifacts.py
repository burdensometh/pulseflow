from pathlib import Path
import gzip
import hashlib
import json
import os
import tempfile

from .values import Conflict, Invalid, Missing, canonical


class Artifacts:
    def __init__(self, root, max_bytes=64 * 1024 * 1024):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max_bytes

    def path(self, name):
        if (
            not isinstance(name, str)
            or not name
            or Path(name).name != name
            or name in {".", ".."}
        ):
            raise Invalid("artifact name must be a single path component")
        destination = (self.root / name).resolve()
        if not destination.is_relative_to(self.root):
            raise Invalid("artifact path escapes its root")
        return destination

    def put(self, name, raw):
        if not isinstance(raw, bytes) or len(raw) > self.max_bytes:
            raise Invalid("artifact exceeds byte limit")
        destination = self.path(name)
        checksum = hashlib.sha256(raw).hexdigest()
        if destination.exists():
            if self.read(name, checksum) != raw:
                raise Conflict("artifact identity conflict")
            return {"filename": name, "sha256": checksum, "bytes": len(raw)}
        fd, temporary = tempfile.mkstemp(prefix=".artifact-", dir=self.root)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return {"filename": name, "sha256": checksum, "bytes": len(raw)}

    def read(self, name, checksum):
        path = self.path(name)
        if not path.is_file():
            raise Missing("artifact is missing")
        if path.stat().st_size > self.max_bytes:
            raise Invalid("artifact exceeds byte limit")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != checksum:
            raise Conflict("artifact checksum mismatch")
        return raw

    def put_document(self, name, document, compressed=False):
        raw = canonical(document).encode("utf-8")
        if len(raw) > self.max_bytes:
            raise Invalid("document exceeds byte limit")
        return self.put(name, gzip.compress(raw, mtime=0) if compressed else raw)

    def read_document(self, name, checksum, compressed=False):
        raw = self.read(name, checksum)
        if compressed:
            import io

            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
                raw = stream.read(self.max_bytes + 1)
            if len(raw) > self.max_bytes:
                raise Invalid("expanded document exceeds byte limit")
        try:
            result = json.loads(raw)
        except (ValueError, UnicodeError) as error:
            raise Invalid("artifact is not valid JSON") from error
        if not isinstance(result, dict):
            raise Invalid("artifact document must be an object")
        return result

    def inventory(self):
        return [
            {
                "filename": path.name,
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in sorted(self.root.iterdir())
            if path.is_file() and not path.name.startswith(".")
        ]
