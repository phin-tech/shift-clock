"""Demonstrates signals + durable park (Phase 4).

The workflow does some work, then blocks on an external `approve` signal. While
waiting it *parks* — the process exits and holds zero resources. When someone
runs `shift-clock signal <id> approve '{"by":"sam"}'`, the daemon re-dispatches
the workflow, `wait_signal` returns the payload, and it finishes.
"""

from shift_clock import workflow, step, log, wait_signal


@step()
def prepare():
    return {"prepared": True}


@step()
def finalize(decision):
    log(f"finalizing with decision: {decision}")
    return {"done": True, "decision": decision}


@workflow
def main():
    prepare()
    log("waiting for human approval — parking until an 'approve' signal arrives")
    decision = wait_signal("approve")
    finalize(decision)


if __name__ == "__main__":
    main()
