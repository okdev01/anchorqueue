"""Run after `python -m pip install .`; Ctrl+C stops polling.

Only this explicit handler is executed. It must finish within the 60-second lease.
Applications with longer work should renew the lease and stop on LeaseLost.
"""

import logging
import time

from anchorqueue import LeaseLost, Queue


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    queue = Queue("jobs.db")
    handlers = {"report": lambda payload: logging.info("report account=%s", payload["account"])}
    try:
        while True:
            job = queue.claim()
            if job is None:
                time.sleep(0.5)
                continue
            try:
                handlers[job.kind](job.payload)
            except Exception as exc:
                try:
                    queue.fail(job, str(exc))
                except LeaseLost:
                    logging.warning("lease lost while reporting failure: %s", job.id)
            else:
                try:
                    queue.ack(job)
                except LeaseLost:
                    logging.warning("handler finished after lease expiry: %s", job.id)
    except KeyboardInterrupt:
        logging.info("worker stopped")


if __name__ == "__main__":
    main()
