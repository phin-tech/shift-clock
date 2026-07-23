"""A small SDK-instrumented ETL workflow (Python).

Steps with retries, a Luigi-style Target (the `load` step is skipped when its
output file already exists), and input params from the deployment.
"""

import os
import time

from shift_clock import workflow, step, get_param


@step(retries=2, retry_delay=0.3)
def extract():
    source = get_param("source", "dev")
    time.sleep(0.2)
    return {"rows": 100, "source": source}


@step()
def transform(data):
    time.sleep(0.2)
    return {"rows": data["rows"], "clean": True}


@step(is_complete=lambda: os.path.exists("out/report.txt"))
def load(data):
    os.makedirs("out", exist_ok=True)
    with open("out/report.txt", "w") as f:
        f.write(f"loaded {data['rows']} clean rows\n")
    return {"written": True}


@workflow
def main():
    data = extract()
    clean = transform(data)
    load(clean)


if __name__ == "__main__":
    main()
