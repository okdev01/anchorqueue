"""JSON-oriented CLI; handlers are explicit Python code, never shell input."""

import argparse
import json
import sqlite3
import sys

from .queue import Queue


def main() -> int:
    parser = argparse.ArgumentParser(description="AnchorQueue: durable local jobs")
    parser.add_argument("--db", default="anchorqueue.db")
    commands = parser.add_subparsers(dest="command", required=True)
    enqueue = commands.add_parser("enqueue")
    enqueue.add_argument("kind")
    enqueue.add_argument("payload", help="JSON value")
    enqueue.add_argument("--key")
    enqueue.add_argument("--priority", type=int, default=0)
    enqueue.add_argument("--delay", type=float, default=0)
    commands.add_parser("stats")
    commands.add_parser("demo", help="enqueue and process a safe demonstration job")
    for name in ("inspect", "retry"):
        commands.add_parser(name).add_argument("id")
    args = parser.parse_args()
    try:
        queue = Queue(args.db)
        if args.command == "enqueue":
            output = {"id": queue.enqueue(args.kind, json.loads(args.payload),
                      dedupe_key=args.key, priority=args.priority, delay=args.delay)}
        elif args.command == "stats":
            output = queue.stats()
        elif args.command == "inspect":
            output = queue.inspect(args.id)
            if output is None:
                print(json.dumps({"error": "job not found"}), file=sys.stderr)
                return 1
        elif args.command == "retry":
            output = {"retried": queue.retry(args.id)}
        else:
            if sum(queue.stats().values()):
                raise ValueError("demo requires an empty database; choose a new --db path")
            queue.enqueue("uppercase", {"text": "durable jobs, explicit guarantees"})
            job = queue.claim()
            assert job is not None
            result = job.payload["text"].upper()
            queue.ack(job)
            output = {"result": result, "stats": queue.stats()}
        print(json.dumps(output, indent=2))
        return 0
    except (ValueError, TypeError, sqlite3.Error) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
