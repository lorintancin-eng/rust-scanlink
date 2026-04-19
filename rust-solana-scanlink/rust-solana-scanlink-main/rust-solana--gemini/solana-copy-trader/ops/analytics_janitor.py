#!/usr/bin/env python3
import argparse
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path


RAW_EVENT_TABLES = {
    "raw_events": "recorded_at_ms",
}

METRIC_TABLES = {
    "feed_health": "ts_ms",
    "feed_first_hits": "first_seen_ms",
    "gate3_snapshots": "recorded_at_ms",
    "gate3_sequences": "detected_at_ms",
    "scoring_breakdowns": "recorded_at_ms",
    "token_risk_signals": "detected_at_ms",
    "label_suggestions": "created_at_ms",
}

EXECUTION_TABLES = {
    "execution_receipts": "recorded_at_ms",
    "post_trade_outcomes": "recorded_at_ms",
}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def effective_retentions(mount_path: str, raw_secs: int, metrics_secs: int, exec_secs: int):
    usage = shutil.disk_usage(mount_path)
    used_pct = int((usage.used / usage.total) * 100)
    if used_pct >= 95:
        return used_pct, min(raw_secs, 900), min(metrics_secs, 3600), min(exec_secs, 86400)
    if used_pct >= 90:
        return used_pct, min(raw_secs, 1800), min(metrics_secs, 21600), min(exec_secs, 172800)
    return used_pct, raw_secs, metrics_secs, exec_secs


def delete_older_than(conn: sqlite3.Connection, table_map, cutoff_ms: int):
    deleted = {}
    for table, column in table_map.items():
        cur = conn.execute(f"DELETE FROM {table} WHERE {column} < ?", (cutoff_ms,))
        deleted[table] = cur.rowcount
    return deleted


def main() -> int:
    parser = argparse.ArgumentParser(description="Prune analytics SQLite tables")
    parser.add_argument(
        "--db",
        default=os.getenv("ANALYTICS_DB_PATH", "data/analytics.sqlite3"),
        help="Path to analytics sqlite db",
    )
    parser.add_argument("--mount-path", default="/", help="Filesystem path for disk pressure checks")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"JANITOR skip missing db: {db_path}")
        return 0

    now_ms = int(time.time() * 1000)
    raw_secs = env_int("ANALYTICS_RAW_EVENT_RETENTION_SECS", 3600)
    metrics_secs = env_int("ANALYTICS_METRICS_RETENTION_SECS", 86400)
    exec_secs = env_int("ANALYTICS_EXECUTION_RETENTION_SECS", 604800)
    used_pct, raw_secs, metrics_secs, exec_secs = effective_retentions(
        args.mount_path, raw_secs, metrics_secs, exec_secs
    )

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA busy_timeout=15000")
        conn.execute("PRAGMA journal_mode=WAL")
        raw_deleted = delete_older_than(conn, RAW_EVENT_TABLES, now_ms - raw_secs * 1000)
        metric_deleted = delete_older_than(conn, METRIC_TABLES, now_ms - metrics_secs * 1000)
        execution_deleted = delete_older_than(conn, EXECUTION_TABLES, now_ms - exec_secs * 1000)
        conn.commit()
        checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        print(
            "JANITOR done",
            {
                "db": str(db_path),
                "disk_used_pct": used_pct,
                "raw_retention_secs": raw_secs,
                "metrics_retention_secs": metrics_secs,
                "execution_retention_secs": exec_secs,
                "raw_deleted": raw_deleted,
                "metric_deleted": metric_deleted,
                "execution_deleted": execution_deleted,
                "wal_checkpoint": checkpoint,
            },
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
