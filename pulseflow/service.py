from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit
import json
import threading

from .values import Conflict, Invalid, Missing, canonical, millis


def query_integer(query, name, default=None):
    values = query.get(name)
    if values is None:
        return default
    if len(values) != 1:
        raise Invalid("query parameter may only appear once: " + name)
    try:
        return int(values[0])
    except ValueError as error:
        raise Invalid("query parameter must be integer: " + name) from error


class API:
    def __init__(self, app):
        self.app = app

    def dispatch(self, method, path, query, body):
        parts = [unquote(part) for part in path.strip("/").split("/") if part]
        app = self.app
        if method == "GET":
            return self.get(parts, query)
        if method != "POST":
            raise Missing("operation does not exist")
        if parts == ["channels"]:
            return app.journal.create(body["name"], body["partitions"])
        if len(parts) == 2 and parts[0] == "ingress":
            return app.journal.append(parts[1], body["batch_id"], body["records"])
        if parts == ["pipelines"]:
            return app.engine.create(
                body["name"], body["channel"], body["plan"], body.get("buckets", 4)
            )
        if len(parts) == 3 and parts[0] == "pipelines":
            return self.pipeline_action(parts[1], parts[2], body)
        if parts == ["dimensions"]:
            return app.dimensions.publish(body["batch_id"], body["records"])
        if parts == ["rollups"]:
            return app.rollups.register(
                body["name"], body["pipeline"], body["definition"]
            )
        if len(parts) == 3 and parts[0] == "rollups" and parts[2] == "refresh":
            return app.rollups.catch_up(parts[1], body.get("limit", 1000))
        if len(parts) == 3 and parts[0] == "savepoints" and parts[2] == "restore":
            return app.savepoints.restore_point(
                parts[1], body["destination"], body.get("buckets")
            )
        if len(parts) == 3 and parts[0] == "channels" and parts[2] == "export":
            return app.exchange.export(
                parts[1], body.get("limit", 10000), body.get("compressed", True)
            )
        if parts == ["exchange", "import"]:
            return app.exchange.import_artifact(
                body["filename"], body["sha256"], body["destination"], body["batch_id"]
            )
        if parts == ["replay"]:
            return app.replay.fork(
                body["source"],
                body["destination"],
                body.get("plan"),
                body.get("buckets"),
            )
        raise Missing("operation does not exist")

    def pipeline_action(self, name, action, body):
        app = self.app
        if action == "run":
            return app.engine.drain(name, body.get("limit", 1000))
        if action == "declare":
            return app.watermarks.declare(name, body["partition"], millis(body["time"]))
        if action == "savepoint":
            return app.savepoints.create(name, body.get("compressed", False))
        if action == "export":
            return app.exports.create(name, body.get("format", "jsonl"))
        if action == "replace-results":
            return app.results.replace_visible(
                name, body["expected_head"], body["documents"]
            )
        if action == "repair-windows":
            return app.repair.reconcile(name)
        raise Missing("pipeline operation does not exist")

    def get(self, parts, query):
        app = self.app
        if parts == ["health"]:
            return {"ok": True, "instance_id": app.catalog.identity()}
        if len(parts) == 3 and parts[0] == "channels":
            if parts[2] == "positions":
                return app.journal.positions(parts[1])
            if parts[2] == "events":
                return app.journal.read(
                    parts[1],
                    query_integer(query, "after", 0),
                    query_integer(query, "limit", 1000),
                    query_integer(query, "partition"),
                )
        if len(parts) == 2 and parts[0] == "pipelines":
            return app.engine.status(parts[1])
        if len(parts) == 3 and parts[0] == "pipelines":
            name, action = parts[1:]
            if action == "results":
                return app.results.rows(name)
            if action == "changes":
                return app.results.changes(
                    name,
                    query_integer(query, "after", 0),
                    query_integer(query, "limit", 1000),
                )
            if action == "late":
                return app.engine.late(name)
            if action == "watermark":
                return app.watermarks.status(name)
            if action == "windows":
                return app.windows.inspect(
                    name, query_integer(query, "start"), query.get("key", [None])[0]
                )
            if action == "check":
                return app.consistency.check(name)
            if action == "lag":
                return app.queries.lag(name)
            if action == "statistics":
                return app.queries.statistics(name, query.get("key", [None])[0])
            if action == "top":
                return app.queries.top(name, query_integer(query, "k", 10))
        if len(parts) == 4 and parts[0] == "pipelines" and parts[2] == "events":
            return app.queries.event(parts[1], parts[3])
        if len(parts) == 3 and parts[0] == "dimensions":
            if "at" not in query or len(query["at"]) != 1:
                raise Invalid("dimension lookup requires exactly one at parameter")
            return app.dimensions.lookup(parts[1], parts[2], int(query["at"][0]))
        if len(parts) == 2 and parts[0] == "savepoints":
            return app.savepoints.get(parts[1])
        if len(parts) == 2 and parts[0] == "rollups":
            return {
                "status": app.rollups.status(parts[1]),
                "rows": app.rollups.rows(parts[1]),
            }
        if len(parts) == 2 and parts[0] == "exports":
            return app.exports.get(parts[1])
        raise Missing("operation does not exist")


def handler_for(app):
    api = API(app)

    class Handler(BaseHTTPRequestHandler):
        server_version = "PulseFlow/1"

        def do_GET(self):
            self.handle_operation("GET")

        def do_POST(self):
            self.handle_operation("POST")

        def read_body(self):
            raw_length = self.headers.get("Content-Length", "0")
            try:
                length = int(raw_length)
            except ValueError as error:
                raise Invalid("invalid Content-Length") from error
            if not 0 <= length <= 2 * 1024 * 1024:
                raise Invalid("request exceeds 2 MiB")
            if length == 0:
                return {}
            if self.headers.get("Content-Type", "").split(";")[0] != "application/json":
                raise Invalid("request Content-Type must be application/json")
            try:
                result = json.loads(self.rfile.read(length))
            except (ValueError, UnicodeError) as error:
                raise Invalid("request is not valid UTF-8 JSON") from error
            if not isinstance(result, dict):
                raise Invalid("request body must be an object")
            return result

        def handle_operation(self, method):
            try:
                url = urlsplit(self.path)
                result = api.dispatch(
                    method, url.path, parse_qs(url.query), self.read_body()
                )
                self.respond(200, result)
            except (Invalid, ValueError, KeyError, TypeError) as error:
                self.respond(400, {"error": str(error)})
            except Missing as error:
                self.respond(404, {"error": str(error)})
            except Conflict as error:
                self.respond(409, {"error": str(error)})

        def respond(self, status, document):
            raw = canonical(document).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, format, *args):
            return

    return Handler


@contextmanager
def serving(app, host="127.0.0.1", port=0):
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise Invalid("operational API must bind loopback")
    server = ThreadingHTTPServer((host, port), handler_for(app))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://{server.server_address[0]}:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
