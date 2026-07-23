"""Demonstrates durable resume across a full daemon restart.

`stage_one` journals its result, then the workflow sleeps long enough for you to
kill the daemon. On restart, the recovery scan re-dispatches this workflow;
`stage_one` is skipped from the journal and only the remainder re-runs.
"""

import time

from shift_clock import workflow, step, log, sleep


@step()
def stage_one():
    time.sleep(0.2)
    return {"stage": 1}


@step()
def stage_two():
    time.sleep(0.2)
    return {"stage": 2}


@workflow
def main():
    stage_one()
    log("durable sleep 6s — the process will PARK (unload) and be re-dispatched")
    sleep(6)  # > park threshold → unloads the process, zero resources while waiting
    stage_two()


if __name__ == "__main__":
    main()
