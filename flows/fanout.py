"""Fan-out / fan-in with child workflows (Phase 5).

Spawns one `scrape` child per URL — each runs as its own supervised process,
concurrently — then joins on all of them. While waiting, the PARENT parks
(unloads): zero resources held during the fan-out. On a crash it resumes from
the journal; already-spawned children attach idempotently and finished ones
return their journaled results.

    shift-clock trigger fanout

Swap `wait_all` for `as_completed` to react to children in completion order
(that ordering is itself journaled, so replay reproduces it exactly).
"""

from shift_clock import workflow, step, log, spawn, wait_all, get_param


@step()
def summarize(results):
    total = sum(r["bytes"] for r in results)
    log(f"scraped {len(results)} urls, {total} bytes total")
    return {"count": len(results), "total_bytes": total}


@workflow
def main():
    urls = get_param("urls", [
        "http://a.example.com",
        "http://bb.example.com",
        "http://ccc.example.com",
    ])
    delay = get_param("delay", 0.2)  # forwarded to children (widen for crash tests)
    # fan-out: N children, each a separate process running concurrently
    kids = [spawn("scrape", {"url": u, "delay": delay}) for u in urls]
    # fan-in: parent parks (0 procs) until every child finishes
    results = wait_all(kids)
    return summarize(results)


if __name__ == "__main__":
    main()
