#!/usr/bin/env bash
# A bare, SDK-less flow. No task events — the worker judges it purely by exit
# code and captures stdout/stderr as logs. This is exactly how `claude -p "..."`
# or any shell script becomes a first-class scheduled flow.
set -euo pipefail
echo "hello from a bare command"
echo "param source = ${SC_PARAM_SOURCE:-<unset>}"
sleep 0.2
echo "done"
