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
                .with_context(|| format!("创建过滤层目录失败: {}", parent.display()))?;
        }

        let init_path = path.clone();
        tokio::task::spawn_blocking(move || init_db(&init_path))
            .await
            .context("初始化过滤层 SQLite 任务失败")??;

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
                "SELECT address, total_tokens, graduated, rug_count, fetched_at_ms
                 FROM creator_profiles WHERE address = ?1",
                params![address],
                |row| {
                    Ok(CreatorProfile {
                        address: row.get(0)?,
                        total_tokens: row.get(1)?,
                        graduated: row.get(2)?,
                        rug_count: row.get(3)?,
                        fetched_at_ms: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
        .await
        .context("读取 creator_profiles 任务失败")?
    }

    pub async fn upsert_creator_profile(&self, profile: &CreatorProfile) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let path = self.path.clone();
        let profile = profile.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            conn.execute(
                "INSERT INTO creator_profiles(address, total_tokens, graduated, rug_count, fetched_at_ms)
                 VALUES(?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(address) DO UPDATE SET
                    total_tokens = excluded.total_tokens,
                    graduated = excluded.graduated,
                    rug_count = excluded.rug_count,
                    fetched_at_ms = excluded.fetched_at_ms",
                params![
                    profile.address,
                    profile.total_tokens,
                    profile.graduated,
                    profile.rug_count,
                    profile.fetched_at_ms
                ],
            )?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("写入 creator_profiles 任务失败")??;
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
        .context("写入 filter_results 任务失败")??;
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
        .context("同步 blacklist 任务失败")??;
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
        .context("同步 smart_money 任务失败")??;
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
        );",
    )?;
    Ok(())
}

fn open_conn(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("打开 SQLite 失败: {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}
