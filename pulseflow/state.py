import json
import zlib

from .values import Conflict, Invalid, canonical, integer


def bucket_for(key, buckets, version=1):
    if version != 1:
        raise Invalid("unsupported state hash version")
    return zlib.adler32(key.encode("utf-8")) % buckets


class KeyedState:
    def __init__(self, catalog):
        self.catalog = catalog

    def get(self, connection, pipeline, kind, key):
        bucket = bucket_for(key, pipeline["buckets"], pipeline["hash_version"])
        row = connection.execute(
            "SELECT document FROM keyed_state WHERE pipeline=? AND bucket=? AND state_kind=? AND group_key=?",
            (pipeline["name"], bucket, kind, key),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, connection, pipeline, kind, key, value):
        bucket = bucket_for(key, pipeline["buckets"], pipeline["hash_version"])
        connection.execute(
            "INSERT INTO keyed_state VALUES (?,?,?,?,?) ON CONFLICT(pipeline,bucket,state_kind,group_key) DO UPDATE SET document=excluded.document",
            (pipeline["name"], bucket, kind, key, canonical(value)),
        )

    def delete(self, connection, pipeline, kind, key):
        bucket = bucket_for(key, pipeline["buckets"], pipeline["hash_version"])
        connection.execute(
            "DELETE FROM keyed_state WHERE pipeline=? AND bucket=? AND state_kind=? AND group_key=?",
            (pipeline["name"], bucket, kind, key),
        )

    def inspect(self, pipeline, kind=None):
        with self.catalog.transaction() as connection:
            self.catalog.pipeline(pipeline, connection)
            sql = "SELECT * FROM keyed_state WHERE pipeline=?"
            values = [pipeline]
            if kind is not None:
                sql += " AND state_kind=?"
                values.append(kind)
            sql += " ORDER BY bucket,state_kind,group_key"
            return [
                {**dict(row), "document": json.loads(row["document"])}
                for row in connection.execute(sql, values)
            ]


class Leases:
    def __init__(self, catalog):
        self.catalog = catalog

    def acquire(self, pipeline, owner, now_ms, ttl_ms=30000):
        if not isinstance(owner, str) or not owner:
            raise Invalid("worker owner is required")
        integer(now_ms, "lease clock", 0)
        integer(ttl_ms, "lease TTL", 1, 3600000)
        with self.catalog.transaction(True) as connection:
            self.catalog.pipeline(pipeline, connection)
            old = connection.execute(
                "SELECT * FROM worker_leases WHERE pipeline=?", (pipeline,)
            ).fetchone()
            if old and old["owner"] and old["expires_at"] > now_ms:
                if old["owner"] != owner:
                    raise Conflict("pipeline has a live worker lease")
                return dict(old)
            fence = old["fence"] + 1 if old else 1
            connection.execute(
                "INSERT INTO worker_leases VALUES (?,?,?,?) ON CONFLICT(pipeline) DO UPDATE SET owner=excluded.owner,fence=excluded.fence,expires_at=excluded.expires_at",
                (pipeline, owner, fence, now_ms + ttl_ms),
            )
            return {
                "pipeline": pipeline,
                "owner": owner,
                "fence": fence,
                "expires_at": now_ms + ttl_ms,
            }

    def check(self, connection, lease, now_ms):
        old = connection.execute(
            "SELECT * FROM worker_leases WHERE pipeline=?", (lease["pipeline"],)
        ).fetchone()
        if (
            not old
            or (old["owner"], old["fence"]) != (lease["owner"], lease["fence"])
            or old["expires_at"] <= now_ms
        ):
            raise Conflict("worker lease has expired or was fenced")

    def renew(self, lease, now_ms, ttl_ms=30000):
        integer(now_ms, "lease clock", 0)
        integer(ttl_ms, "lease TTL", 1, 3600000)
        with self.catalog.transaction(True) as connection:
            self.check(connection, lease, now_ms)
            expires = now_ms + ttl_ms
            connection.execute(
                "UPDATE worker_leases SET expires_at=? WHERE pipeline=?",
                (expires, lease["pipeline"]),
            )
        return {**lease, "expires_at": expires}

    def release(self, lease, now_ms):
        with self.catalog.transaction(True) as connection:
            self.check(connection, lease, now_ms)
            connection.execute(
                "UPDATE worker_leases SET owner=NULL,expires_at=0 WHERE pipeline=?",
                (lease["pipeline"],),
            )
        return {"released": True, "fence": lease["fence"]}
