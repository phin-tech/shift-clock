"""A workflow that stays running for a bit — used to test concurrency limits."""

import time

from shift_clock import workflow, step


@step()
def work():
    time.sleep(1.5)
    return {"ok": True}


@workflow
def main():
    work()


if __name__ == "__main__":
    main()
