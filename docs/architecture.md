# Design decisions

## Why SQLite?

For a single-host tool, removing a separate broker simplifies setup and recovery.
SQLite transactions provide a clear serialization boundary. Each API operation
opens and closes its own connection; no connection crosses a thread boundary.
WAL allows readers alongside a writer, but there is still one writer at a time.
The busy timeout is ten seconds; sustained contention can raise OperationalError.

## Claim transaction

1. Acquire the write transaction before sampling the clock.
2. Requeue expired jobs, or mark them dead when their attempt budget is exhausted.
3. Select the next eligible job by priority, creation time and insertion order.
4. Increment attempts and assign a random lease token and expiry.
5. Commit before handing work to application code.

User handlers execute outside the transaction. A slow handler cannot hold a
database write lock through the queue API.

## Ownership versus external effects

Acknowledgement, failure and heartbeat require the current token and an unexpired
lease. This prevents an abandoned worker from changing state after reclamation.
It cannot stop that worker from sending a request or writing another database.
External effects need their own idempotency protocol. This distinction is why the
project does not claim exactly-once processing.

## Retry policy

Every claim consumes an attempt, including claims followed by process termination.
Reported failures use min(max_delay, base_delay * 2^(attempt-1)), with exponent
clamped at 30 to avoid unbounded arithmetic. Expired leases become immediately
eligible on the next claim. Manual retry resets attempts but keeps the dedupe key.
No random jitter is currently applied.

## Read operations

Stats and inspection are snapshots. They do not reap leases, so running can include
expired jobs until a worker calls claim. Inspection omits the ownership token.
Deduplication keys are permanent while their rows exist; repeated keys return the
original ID without updating kind or payload.

## Next design boundaries

Retention needs an explicit decision about how long deduplication must last.
Schema evolution needs migrations before incompatible database changes.
Multi-host coordination requires a different storage/locking model, not a shared
SQLite file. Throughput claims require published, reproducible benchmarks first.

Reference: [Python sqlite3 transaction control](https://docs.python.org/3/library/sqlite3.html#transaction-control).
