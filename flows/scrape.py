"""A leaf child workflow: 'scrape' one URL and return a small result.

Spawned by `fanout.py` via `spawn("scrape", {"url": ...})`. It's an ordinary
deployment — nothing about it knows it's a child; its return value is routed
back to the parent as a `child:<id>` signal by the daemon.
"""

import time

from shift_clock import workflow, step, get_param


@step(retries=1)
def fetch(url, delay):
    time.sleep(delay)
    return {"url": url, "bytes": len(url) * 100}


@workflow
def main():
    url = get_param("url", "http://example.com")
    delay = float(get_param("delay", 0.2))  # widen for demos / crash-resume tests
    return fetch(url, delay)


if __name__ == "__main__":
    main()
