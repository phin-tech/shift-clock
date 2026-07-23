"""A tiny every-minute flow to watch the scheduler fire on its own."""

import time

from shift_clock import workflow, step, log


@step()
def beat():
    stamp = time.strftime("%H:%M:%S")
    log(f"heartbeat at {stamp}")
    return {"at": stamp}


@workflow
def main():
    beat()


if __name__ == "__main__":
    main()
