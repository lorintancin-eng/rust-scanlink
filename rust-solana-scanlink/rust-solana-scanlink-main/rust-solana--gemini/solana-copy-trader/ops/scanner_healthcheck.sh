#!/usr/bin/env bash
set -euo pipefail

WORKDIR="${WORKDIR:-/home/ubuntu/rust-scanlink}"
SERVICE_NAME="${SERVICE_NAME:-rust-scanlink.service}"
SCANNER_LIVE_FILE="${SCANNER_LIVE_FILE:-$WORKDIR/data/scanner_live_tokens.jsonl}"
MAX_IDLE_SECS="${MAX_IDLE_SECS:-300}"
DISK_MOUNT="${DISK_MOUNT:-/}"

cd "$WORKDIR"

used_pct="$(df -P "$DISK_MOUNT" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')"
if [ "${used_pct:-0}" -ge 95 ]; then
  echo "HEALTHCHECK disk critical: ${used_pct}%"
  python3 "$WORKDIR/ops/analytics_janitor.py" --db "${ANALYTICS_DB_PATH:-$WORKDIR/data/analytics.sqlite3}" || true
fi

if ! systemctl is-active --quiet "$SERVICE_NAME"; then
  echo "HEALTHCHECK service inactive, restarting"
  systemctl restart "$SERVICE_NAME"
  exit 0
fi

now="$(date +%s)"
mtime=0
if [ -f "$SCANNER_LIVE_FILE" ]; then
  mtime="$(stat -c %Y "$SCANNER_LIVE_FILE" 2>/dev/null || echo 0)"
fi

if [ "$mtime" -gt 0 ] && [ $(( now - mtime )) -le "$MAX_IDLE_SECS" ]; then
  echo "HEALTHCHECK ok: recent scanner output"
  exit 0
fi

echo "HEALTHCHECK stale scanner output, restarting"
python3 "$WORKDIR/ops/analytics_janitor.py" --db "${ANALYTICS_DB_PATH:-$WORKDIR/data/analytics.sqlite3}" || true
systemctl restart "$SERVICE_NAME"
