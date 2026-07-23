"""Demonstrates crash + durable resume.

First process attempt: `prep` succeeds (its result is journaled), then the
process hard-exits with no terminal event — a genuine crash. The worker
re-dispatches the workflow (up to `retries`). On resume, `prep` is in the journal
so it is *skipped* (returns its recorded output), and the workflow completes.

Because the journal is durable in SQLite, this resumes even if the whole daemon
restarts between attempts.
"""

import os
import time

from shift_clock import workflow, step


@step()
def prep():
    time.sleep(0.2)
    return {"ready": True}


@workflow
def main():
    prep()  # first attempt: executes & journals; resume: skipped from journal

    os.makedirs("out", exist_ok=True)
    marker = "out/crashy.marker"
    if not os.path.exists(marker):
        open(marker, "w").close()
        print("simulating a hard crash (no terminal event)…")
        os._exit(1)  # dies without emitting workflow_success/failure

    print("resume: prep was skipped from the journal, completing cleanly")
    os.remove(marker)


if __name__ == "__main__":
    main()
