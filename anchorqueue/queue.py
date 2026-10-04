"""SQLite storage. Every mutation is a short, isolated transaction."""

from contextlib import contextmanager
from dataclasses import dataclass
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable, Iterator
from uuid import uuid4


class LeaseLost(RuntimeError):
    """The caller no longer owns a live lease."""


@dataclass(frozen=True)
class Job:
    id: str
    kind: str
    payload: Any
    token: str
    attempt: int
    lease_until: float


def _duration(value: float, *, positive: bool = False) -> None:
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError("duration must be finite and " + ("positive" if positive else "nonnegative"))


class Queue:
    """One local database, multiple processes; at-least-once execution.

    Connections are operation-local, so a Queue can be shared by threads.
    The injected clock is for deterministic tests; production uses epoch time.
    """

    def __init__(self, path: str | Path, *, clock: Callable[[], float] = time.time):
        if str(path) == ":memory:":
            raise ValueError("a persistent database path is required")
        self.path = str(Path(path).resolve())
        self.clock = clock
        with self._db() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('queued','running','done','dead')),
                    priority INTEGER NOT NULL, available_at REAL NOT NULL,
                    created_at REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
                    token TEXT, lease_until REAL, error TEXT,
                    dedupe_key TEXT UNIQUE
                );
                CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(state, available_at, priority);
                CREATE INDEX IF NOT EXISTS jobs_leases ON jobs(state, lease_until);
            """)

    @contextmanager
    def _db(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous=FULL")
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.commit()
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise
        finally:
            db.close()

    def enqueue(self, kind: str, payload: Any, *, delay: float = 0,
                priority: int = 0, max_attempts: int = 3,
                dedupe_key: str | None = None) -> str:
        _duration(delay)
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError("kind must be a nonempty string")
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        if type(priority) is not int:
            raise ValueError("priority must be an integer")
        if dedupe_key is not None and (not isinstance(dedupe_key, str) or not dedupe_key):
            raise ValueError("dedupe_key must be a nonempty string")
        encoded = json.dumps(payload, allow_nan=False)
        with self._db(write=True) as db:
            if dedupe_key is not None:
                existing = db.execute("SELECT id FROM jobs WHERE dedupe_key=?", (dedupe_key,)).fetchone()
                if existing:
                    return existing["id"]
            now = self.clock()
            job_id = uuid4().hex
            db.execute("""INSERT INTO jobs
                (id,kind,payload,state,priority,available_at,created_at,max_attempts,dedupe_key)
                VALUES (?,?,?,'queued',?,?,?,?,?)""",
                (job_id, kind, encoded, priority, now + delay, now, max_attempts, dedupe_key))
            return job_id

    def claim(self, *, lease_seconds: float = 60) -> Job | None:
        _duration(lease_seconds, positive=True)
        with self._db(write=True) as db:
            now = self.clock()
            db.execute("""UPDATE jobs SET
                state=CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
                token=NULL, lease_until=NULL, available_at=?, error='lease expired'
                WHERE state='running' AND lease_until<=?""", (now, now))
            row = db.execute("""SELECT * FROM jobs WHERE state='queued' AND available_at<=?
                ORDER BY priority DESC, created_at, rowid LIMIT 1""", (now,)).fetchone()
            if row is None:
                return None
            token = uuid4().hex
            until = now + lease_seconds
            db.execute("""UPDATE jobs SET state='running', attempts=attempts+1,
                token=?, lease_until=? WHERE id=?""", (token, until, row["id"]))
            return Job(row["id"], row["kind"], json.loads(row["payload"]),
                       token, row["attempts"] + 1, until)

    def _owned(self, db: sqlite3.Connection, job: Job, now: float) -> sqlite3.Row:
        row = db.execute("""SELECT * FROM jobs WHERE id=? AND token=?
            AND state='running' AND lease_until>?""", (job.id, job.token, now)).fetchone()
        if row is None:
            raise LeaseLost(job.id)
        return row

    def ack(self, job: Job) -> None:
        with self._db(write=True) as db:
            self._owned(db, job, self.clock())
            db.execute("UPDATE jobs SET state='done',token=NULL,lease_until=NULL,error=NULL WHERE id=?", (job.id,))

    def heartbeat(self, job: Job, *, lease_seconds: float = 60) -> float:
        _duration(lease_seconds, positive=True)
        with self._db(write=True) as db:
            now = self.clock()
            row = self._owned(db, job, now)
            until = max(row["lease_until"], now + lease_seconds)
            db.execute("UPDATE jobs SET lease_until=? WHERE id=?", (until, job.id))
            return until

    def fail(self, job: Job, error: str, *, base_delay: float = 1, max_delay: float = 300) -> None:
        _duration(base_delay)
        _duration(max_delay)
        with self._db(write=True) as db:
            now = self.clock()
            row = self._owned(db, job, now)
            state = "dead" if row["attempts"] >= row["max_attempts"] else "queued"
            delay = min(max_delay, base_delay * (2 ** min(row["attempts"] - 1, 30)))
            db.execute("""UPDATE jobs SET state=?,available_at=?,token=NULL,
                lease_until=NULL,error=? WHERE id=?""", (state, now + delay, str(error)[:2000], job.id))

    def retry(self, job_id: str) -> bool:
        """Explicitly reset a dead job, retaining its identity and dedupe key."""
        with self._db(write=True) as db:
            result = db.execute("""UPDATE jobs SET state='queued',attempts=0,
                available_at=?,error=NULL WHERE id=? AND state='dead'""", (self.clock(), job_id))
            return result.rowcount == 1

    def inspect(self, job_id: str) -> dict[str, Any] | None:
        with self._db() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            result = dict(row)
            result.pop("token")
            result["payload"] = json.loads(result["payload"])
            return result

    def stats(self) -> dict[str, int]:
        with self._db() as db:
            result = dict.fromkeys(("queued", "running", "done", "dead"), 0)
            result.update({row[0]: row[1] for row in db.execute("SELECT state,count(*) FROM jobs GROUP BY state")})
            return result
