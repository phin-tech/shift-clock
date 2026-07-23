#!/usr/bin/env bash
# Install shift-clock as a macOS launchd background service (starts at login,
# restarts on crash). Run from the repo root:  ./deploy/install.sh
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="tech.phin.shift-clock"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

echo "▶ building release binary…"
( cd "$REPO" && cargo build --release )

echo "▶ writing $PLIST"
mkdir -p "$HOME/Library/LaunchAgents"
sed "s#@WORKDIR@#$REPO#g" "$REPO/deploy/$LABEL.plist" > "$PLIST"

echo "▶ (re)loading the service…"
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"

echo "✓ installed. Control plane on http://127.0.0.1:8080"
echo "  status:  launchctl print gui/$(id -u)/$LABEL | grep state"
echo "  restart: launchctl kickstart -k gui/$(id -u)/$LABEL"
echo "  logs:    tail -f \"$REPO/shift-clock.service.log\""
echo "  stop:    launchctl bootout gui/$(id -u)/$LABEL"
echo "  watch:   shift-clock dashboard        # TUI client of the running service"
