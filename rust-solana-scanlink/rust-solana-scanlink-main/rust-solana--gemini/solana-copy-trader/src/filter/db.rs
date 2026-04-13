use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Default)]
pub struct CreatorProfile {
    pub address: String,
    pub total_tokens: u32,
    pub graduated: u32,
    pub rug_count: u32,
    pub oldest_tx_ms: u64,
    pub wallet_age_days: u32,
    pub first_funder: Option<String>,
    pub fetched_at_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct BuyerProfile {
    pub address: String,
    pub oldest_tx_ms: u64,
    pub wallet_age_days: u32,
    pub first_funder: Option<String>,
    pub fetched_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct FilterResultRecord {
    pub mint: String,
    pub creator: String,
    pub symbol: String,
    pub passed: bool,
    pub reject_gate: Option<String>,
    pub score: Option<u32>,
    pub reason: String,
    pub ts: u64,
}

#[derive(Debug, Clone)]
pub struct FilterTimingRecord {
    pub mint: String,
    pub decision: String,
    pub mode: String,
    pub path: String,
    pub detected_at_ms: u64,
    pub gate1_at_ms: Option<u64>,
    pub gate2_at_ms: Option<u64>,
    pub gate3_open_at_ms: Option<u64>,
    pub gate3_trigger_at_ms: Option<u64>,
    pub gate4_at_ms: Option<u64>,
    pub final_at_ms: u64,
    pub latency_ms: u64,
    pub early_buy_count: usize,
    pub matched_buyers: usize,
}

#[derive(Clone)]
pub struct FilterDb {
    path: Arc<PathBuf>,
    write_lock: Arc<Mutex<()>>,
}

impl FilterDb {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("鍒涘缓杩囨护灞傜洰褰曞け璐? {}", parent.display()))?;
        }

        let init_path = path.clone();
        tokio::task::spawn_blocking(move || init_db(&init_path))
            .await
            .context("鍒濆鍖栬繃婊ゅ眰 SQLite 浠诲姟澶辫触")??;

        Ok(Self {
            path: Arc::new(path),
            write_lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn get_creator_profile(&self, address: &str) -> Result<Option<CreatorProfile>> {
        let path = self.path.clone();
        let address = address.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            conn.query_row(
                "SELECT address, total_tokens, graduated, rug_count, oldest_tx_ms, wallet_age_days, first_funder, fetched_at_ms
                 FROM creator_profiles WHERE address = ?1",
                params![address],
                |row| {
                    Ok(CreatorProfile {
                        address: row.get(0)?,
                        total_tokens: row.get(1)?,
                        graduated: row.get(2)?,
                        rug_count: row.get(3)?,
                        oldest_tx_ms: row.get(4)?,
                        wallet_age_days: row.get(5)?,
                        first_funder: row.get(6)?,
                        fetched_at_ms: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
        .await
        .context("璇诲彇 creator_profiles 浠诲姟澶辫触")?
    }

    pub async fn upsert_creator_profile(&self, profile: &CreatorProfile) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let path = self.path.clone();
        let profile = profile.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            conn.execute(
                "INSERT INTO creator_profiles(address, total_tokens, graduated, rug_count, oldest_tx_ms, wallet_age_days, first_funder, fetched_at_ms)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(address) DO UPDATE SET
                    total_tokens = excluded.total_tokens,
                    graduated = excluded.graduated,
                    rug_count = excluded.rug_count,
                    oldest_tx_ms = excluded.oldest_tx_ms,
                    wallet_age_days = excluded.wallet_age_days,
                    first_funder = excluded.first_funder,
                    fetched_at_ms = excluded.fetched_at_ms",
                params![
                    profile.address,
                    profile.total_tokens,
                    profile.graduated,
                    profile.rug_count,
                    profile.oldest_tx_ms,
                    profile.wallet_age_days,
                    profile.first_funder,
                    profile.fetched_at_ms,
                ],
            )?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("鍐欏叆 creator_profiles 浠诲姟澶辫触")??;
        Ok(())
    }

    pub async fn get_buyer_profile(&self, address: &str) -> Result<Option<BuyerProfile>> {
        let path = self.path.clone();
        let address = address.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            conn.query_row(
                "SELECT address, oldest_tx_ms, wallet_age_days, first_funder, fetched_at_ms
                 FROM buyer_profiles WHERE address = ?1",
                params![address],
                |row| {
                    Ok(BuyerProfile {
                        address: row.get(0)?,
                        oldest_tx_ms: row.get(1)?,
                        wallet_age_days: row.get(2)?,
                        first_funder: row.get(3)?,
                        fetched_at_ms: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
        .await
        .context("璇诲彇 buyer_profiles 浠诲姟澶辫触")?
    }

    pub async fn upsert_buyer_profile(&self, profile: &BuyerProfile) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let path = self.path.clone();
        let profile = profile.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            conn.execute(
                "INSERT INTO buyer_profiles(address, oldest_tx_ms, wallet_age_days, first_funder, fetched_at_ms)
                 VALUES(?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(address) DO UPDATE SET
                    oldest_tx_ms = excluded.oldest_tx_ms,
                    wallet_age_days = excluded.wallet_age_days,
                    first_funder = excluded.first_funder,
                    fetched_at_ms = excluded.fetched_at_ms",
                params![
                    profile.address,
                    profile.oldest_tx_ms,
                    profile.wallet_age_days,
                    profile.first_funder,
                    profile.fetched_at_ms,
                ],
            )?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("鍐欏叆 buyer_profiles 浠诲姟澶辫触")??;
        Ok(())
    }

    pub async fn insert_filter_result(&self, record: &FilterResultRecord) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let path = self.path.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            conn.execute(
                "INSERT OR REPLACE INTO filter_results(
                    mint, creator, symbol, passed, reject_gate, score, reason, ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    record.mint,
                    record.creator,
                    record.symbol,
                    if record.passed { 1 } else { 0 },
                    record.reject_gate,
                    record.score,
                    record.reason,
                    record.ts
                ],
            )?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("鍐欏叆 filter_results 浠诲姟澶辫触")??;
        Ok(())
    }

    pub async fn insert_filter_timing(&self, record: &FilterTimingRecord) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let path = self.path.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            conn.execute(
                "INSERT OR REPLACE INTO filter_timelines(
                    mint, decision, mode, path, detected_at_ms, gate1_at_ms, gate2_at_ms, gate3_open_at_ms,
                    gate3_trigger_at_ms, gate4_at_ms, final_at_ms, latency_ms, early_buy_count, matched_buyers
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    record.mint,
                    record.decision,
                    record.mode,
                    record.path,
                    record.detected_at_ms,
                    record.gate1_at_ms,
                    record.gate2_at_ms,
                    record.gate3_open_at_ms,
                    record.gate3_trigger_at_ms,
                    record.gate4_at_ms,
                    record.final_at_ms,
                    record.latency_ms,
                    record.early_buy_count as u64,
                    record.matched_buyers as u64,
                ],
            )?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("鍐欏叆 filter_timelines 浠诲姟澶辫触")??;
        Ok(())
    }

    pub async fn sync_blacklist(&self, addresses: &[String]) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let path = self.path.clone();
        let entries = addresses.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let tx = conn.unchecked_transaction()?;
            for address in entries {
                tx.execute(
                    "INSERT INTO blacklist(address, reason, added_at)
                     VALUES(?1, ?2, strftime('%s','now') * 1000)
                     ON CONFLICT(address) DO UPDATE SET reason = excluded.reason",
                    params![address, "file_hot_reload"],
                )?;
            }
            tx.commit()?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("鍚屾 blacklist 浠诲姟澶辫触")??;
        Ok(())
    }

    pub async fn sync_smart_money(&self, addresses: &[String]) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let path = self.path.clone();
        let entries = addresses.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let tx = conn.unchecked_transaction()?;
            for address in entries {
                tx.execute(
                    "INSERT INTO smart_money(address, win_count, total_signals, last_updated)
                     VALUES(?1, 0, 0, strftime('%s','now') * 1000)
                     ON CONFLICT(address) DO NOTHING",
                    params![address],
                )?;
            }
            tx.commit()?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("鍚屾 smart_money 浠诲姟澶辫触")??;
        Ok(())
    }
}

fn init_db(path: &Path) -> Result<()> {
    let conn = open_conn(path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS creator_profiles (
            address TEXT PRIMARY KEY,
            total_tokens INTEGER NOT NULL DEFAULT 0,
            graduated INTEGER NOT NULL DEFAULT 0,
            rug_count INTEGER NOT NULL DEFAULT 0,
            oldest_tx_ms INTEGER NOT NULL DEFAULT 0,
            wallet_age_days INTEGER NOT NULL DEFAULT 0,
            first_funder TEXT,
            fetched_at_ms INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS buyer_profiles (
            address TEXT PRIMARY KEY,
            oldest_tx_ms INTEGER NOT NULL DEFAULT 0,
            wallet_age_days INTEGER NOT NULL DEFAULT 0,
            first_funder TEXT,
            fetched_at_ms INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS smart_money (
            address TEXT PRIMARY KEY,
            win_count INTEGER NOT NULL DEFAULT 0,
            total_signals INTEGER NOT NULL DEFAULT 0,
            last_updated INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS blacklist (
            address TEXT PRIMARY KEY,
            reason TEXT NOT NULL,
            added_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS filter_results (
            mint TEXT PRIMARY KEY,
            creator TEXT NOT NULL,
            symbol TEXT NOT NULL,
            passed INTEGER NOT NULL,
            reject_gate TEXT,
            score INTEGER,
            reason TEXT NOT NULL,
            ts INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS filter_timelines (
            mint TEXT PRIMARY KEY,
            decision TEXT NOT NULL,
            mode TEXT NOT NULL,
            path TEXT NOT NULL,
            detected_at_ms INTEGER NOT NULL,
            gate1_at_ms INTEGER,
            gate2_at_ms INTEGER,
            gate3_open_at_ms INTEGER,
            gate3_trigger_at_ms INTEGER,
            gate4_at_ms INTEGER,
            final_at_ms INTEGER NOT NULL,
            latency_ms INTEGER NOT NULL,
            early_buy_count INTEGER NOT NULL DEFAULT 0,
            matched_buyers INTEGER NOT NULL DEFAULT 0
        );",
    )?;
    ensure_column(
        &conn,
        "creator_profiles",
        "oldest_tx_ms",
        "ALTER TABLE creator_profiles ADD COLUMN oldest_tx_ms INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        &conn,
        "creator_profiles",
        "wallet_age_days",
        "ALTER TABLE creator_profiles ADD COLUMN wallet_age_days INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        &conn,
        "creator_profiles",
        "first_funder",
        "ALTER TABLE creator_profiles ADD COLUMN first_funder TEXT",
    )?;
    Ok(())
}

fn ensure_column(conn: &Connection, table: &str, column: &str, statement: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .flatten()
        .any(|name| name == column);
    if !exists {
        conn.execute(statement, [])?;
    }
    Ok(())
}

fn open_conn(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("鎵撳紑 SQLite 澶辫触: {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}
