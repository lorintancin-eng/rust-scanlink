#!/usr/bin/env python3
import argparse
import sqlite3
import time
from pathlib import Path

RUNTIME_TABLES = [
    "creator_profiles",
    "buyer_profiles",
    "funder_profiles",
    "address_snapshots",
    "smart_money",
    "blacklist",
    "filter_results",
    "filter_timelines",
    "dynamic_keywords",
    "uri_patterns",
    "creator_templates",
    "funder_blacklist",
    "address_clusters",
    "cluster_edges",
]

ANALYTICS_TABLES = [
    "raw_events",
    "feed_health",
    "feed_first_hits",
    "gate3_snapshots",
    "gate3_sequences",
    "scoring_breakdowns",
    "token_risk_signals",
    "label_suggestions",
    "post_trade_outcomes",
    "execution_receipts",
]

RAW_EVENT_RETENTION_MS = 3600 * 1000


def copy_schema(src: sqlite3.Connection, dst: sqlite3.Connection, tables):
    for table in tables:
        sql = src.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if sql and sql[0]:
            dst.execute(sql[0])
    for table in tables:
        rows = src.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
            (table,),
        ).fetchall()
        for (sql,) in rows:
            dst.execute(sql)
    dst.commit()


def copy_full(src: sqlite3.Connection, dst: sqlite3.Connection, table: str):
    cols = [r[1] for r in src.execute(f"PRAGMA table_info({table})")]
    if not cols:
        return
    column_list = ", ".join(cols)
    placeholders = ", ".join(["?"] * len(cols))
    rows = src.execute(f"SELECT {column_list} FROM {table}")
    dst.executemany(
        f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})",
        rows,
    )
    dst.commit()


def copy_filtered_raw_events(src: sqlite3.Connection, dst: sqlite3.Connection, cutoff_ms: int):
    cols = [r[1] for r in src.execute("PRAGMA table_info(raw_events)")]
    if not cols:
        return 0
    column_list = ", ".join(cols)
    placeholders = ", ".join(["?"] * len(cols))
    rows = list(
        src.execute(
            f"SELECT {column_list} FROM raw_events WHERE recorded_at_ms >= ?",
            (cutoff_ms,),
        )
    )
    dst.executemany(
        f"INSERT INTO raw_events ({column_list}) VALUES ({placeholders})",
        rows,
    )
    dst.commit()
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Split old filter.sqlite3 into runtime + analytics")
    parser.add_argument("--source", default="data/filter.sqlite3")
    parser.add_argument("--runtime", default="data/runtime.sqlite3")
    parser.add_argument("--analytics", default="data/analytics.sqlite3")
    args = parser.parse_args()

    source = Path(args.source)
    runtime = Path(args.runtime)
    analytics = Path(args.analytics)
    if not source.exists():
        raise SystemExit(f"missing source db: {source}")

    runtime.parent.mkdir(parents=True, exist_ok=True)
    analytics.parent.mkdir(parents=True, exist_ok=True)

    src = sqlite3.connect(str(source))
    runtime_db = sqlite3.connect(str(runtime))
    analytics_db = sqlite3.connect(str(analytics))
    try:
        copy_schema(src, runtime_db, RUNTIME_TABLES)
        copy_schema(src, analytics_db, ANALYTICS_TABLES)
        for table in RUNTIME_TABLES:
            copy_full(src, runtime_db, table)
        cutoff_ms = int(time.time() * 1000) - RAW_EVENT_RETENTION_MS
        raw_count = copy_filtered_raw_events(src, analytics_db, cutoff_ms)
        for table in [t for t in ANALYTICS_TABLES if t != "raw_events"]:
            copy_full(src, analytics_db, table)
        runtime_db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        analytics_db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        runtime_db.commit()
        analytics_db.commit()
        print(
            {
                "source": str(source),
                "runtime": str(runtime),
                "analytics": str(analytics),
                "raw_events_kept": raw_count,
            }
        )
    finally:
        src.close()
        runtime_db.close()
        analytics_db.close()


if __name__ == "__main__":
    main()
