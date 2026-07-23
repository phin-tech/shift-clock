"""Demonstrates exactly-once transactional steps + queryable state.

`charge` records the charge via set_state — which commits *atomically* with the
step's journal entry. The workflow then hard-crashes. On resume, `charge` is
skipped from the journal, so the charge is NOT re-applied: charged_total stays
100, not 200. The state is readable any time with `shift-clock query <id>`.
"""

import os

from shift_clock import workflow, step, set_state, get_state, log


@step()
def charge(amount):
    total = get_state("charged_total", 0) + amount
    set_state("charged_total", total)   # exactly-once: atomic with the journal
    set_state("status", "charged")
    log(f"charged {amount}; total now {total}")
    return {"total": total}


@step()
def finish():
    set_state("status", "done")
    return {"ok": True}


@workflow
def main():
    charge(100)

    # Crash once AFTER the charge is durably journaled — resume must NOT re-charge.
    os.makedirs("out", exist_ok=True)
    marker = "out/payment.marker"
    if not os.path.exists(marker):
        open(marker, "w").close()
        log("crashing after charge — resume must not double-charge")
        os._exit(1)
    os.remove(marker)

    finish()


if __name__ == "__main__":
    main()
