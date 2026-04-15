use crate::config::AppConfig;
use crate::filter;
use crate::filter::FilterDb;
use crate::scanner::raw_event::raw_event_to_scanner_event;
use anyhow::{Context, Result};
use rusqlite::{Connection, DatabaseName, OpenFlags};
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::info;

const REPLAY_PROGRESS_EVERY: usize = 1_000;

#[derive(Debug, Clone)]
pub struct ReplayPipelineSummary {
    pub source_event_count: usize,
    pub replayed_event_count: usize,
    pub emitted_buy_signals: usize,
    pub elapsed_ms: u64,
    pub speedup: f64,
}

pub async fn run_pipeline(
    config: &AppConfig,
    from_ms: u64,
    to_ms: u64,
) -> Result<ReplayPipelineSummary> {
    prepare_replay_db(config).await?;

    let source_db_path = if config.filter_db_path == config.replay_db_path {
        &config.filter_db_path
    } else {
        &config.replay_db_path
    };
    let source_db = FilterDb::new(source_db_path).await?;
    let raw_events = source_db.list_raw_events_window(from_ms, to_ms).await?;
    let source_event_count = raw_events.len();
    info!(
        "Replay pipeline source prepared | raw_events={} | source_db={} | from_ms={} | to_ms={}",
        source_event_count,
        source_db_path,
        from_ms,
        to_ms,
    );

    let mut replayed_events = Vec::with_capacity(raw_events.len());
    for record in raw_events {
        if let Some(event) = raw_event_to_scanner_event(&record)? {
            replayed_events.push((record.recorded_at_ms, event));
        }
    }
    replayed_events.sort_by_key(|(recorded_at_ms, _)| *recorded_at_ms);

    let mut replay_config = config.clone();
    replay_config.filter_db_path = config.replay_db_path.clone();
    replay_config.execution_enabled = false;
    replay_config.auto_sell_enabled = false;
    replay_config.replay_mode_enabled = false;
    replay_config.replay_pipeline_enabled = false;

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        replay_config.rpc_url.clone(),
        CommitmentConfig::confirmed(),
    ));

    let (scanner_tx, scanner_rx) = mpsc::channel(4096);
    let (buy_tx, mut buy_rx) = mpsc::channel(512);
    let replay_config = Arc::new(replay_config);
    let filter_task = tokio::spawn(filter::run(replay_config, rpc_client, scanner_rx, buy_tx));
    let buy_counter = tokio::spawn(async move {
        let mut count = 0usize;
        while buy_rx.recv().await.is_some() {
            count = count.saturating_add(1);
        }
        count
    });

    let start = Instant::now();
    let mut previous_ts = replayed_events.first().map(|(ts, _)| *ts).unwrap_or(to_ms);
    for (idx, (recorded_at_ms, event)) in replayed_events.iter().enumerate() {
        let delta_ms = recorded_at_ms.saturating_sub(previous_ts);
        let scaled_ms = ((delta_ms as f64) / config.replay_speedup.max(1.0)).round() as u64;
        if scaled_ms > 0 {
            tokio::time::sleep(Duration::from_millis(scaled_ms.min(250))).await;
        }
        if scanner_tx.send(event.clone()).await.is_err() {
            break;
        }
        if (idx + 1) % REPLAY_PROGRESS_EVERY == 0 || idx + 1 == replayed_events.len() {
            info!(
                "Replay pipeline progress | replayed={}/{} | elapsed_ms={} | latest_recorded_at_ms={}",
                idx + 1,
                replayed_events.len(),
                start.elapsed().as_millis(),
                recorded_at_ms,
            );
        }
        previous_ts = *recorded_at_ms;
    }
    drop(scanner_tx);

    filter_task
        .await
        .context("replay filter task join failed")??;
    let emitted_buy_signals = buy_counter
        .await
        .context("replay buy signal counter join failed")?;

    Ok(ReplayPipelineSummary {
        source_event_count,
        replayed_event_count: replayed_events.len(),
        emitted_buy_signals,
        elapsed_ms: start.elapsed().as_millis() as u64,
        speedup: config.replay_speedup.max(1.0),
    })
}

async fn prepare_replay_db(config: &AppConfig) -> Result<()> {
    let source = Path::new(&config.filter_db_path);
    let target = Path::new(&config.replay_db_path);
    if source == target {
        return Ok(());
    }
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if !tokio::fs::try_exists(source).await? {
        return Ok(());
    }

    let source = source.to_path_buf();
    let target = target.to_path_buf();
    tokio::task::spawn_blocking(move || snapshot_sqlite_db(&source, &target))
        .await
        .context("replay db snapshot join failed")??;

    Ok(())
}

fn snapshot_sqlite_db(source: &Path, target: &Path) -> Result<()> {
    let temp_target = replay_temp_path(target);
    if temp_target.exists() {
        std::fs::remove_file(&temp_target).with_context(|| {
            format!("remove stale replay temp db failed: {}", temp_target.display())
        })?;
    }

    let source_conn = Connection::open_with_flags(
        source,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open replay source db failed: {}", source.display()))?;

    source_conn
        .backup(
            DatabaseName::Main,
            &temp_target,
            None::<fn(rusqlite::backup::Progress)>,
        )
        .with_context(|| {
            format!(
                "sqlite backup replay db failed: {} -> {}",
                source.display(),
                temp_target.display()
            )
        })?;

    if target.exists() {
        std::fs::remove_file(target)
            .with_context(|| format!("remove stale replay target failed: {}", target.display()))?;
    }

    std::fs::rename(&temp_target, target).with_context(|| {
        format!(
            "promote replay temp db failed: {} -> {}",
            temp_target.display(),
            target.display()
        )
    })?;

    Ok(())
}

fn replay_temp_path(target: &Path) -> PathBuf {
    let mut temp = target.to_path_buf();
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| format!("{name}.tmp"))
        .unwrap_or_else(|| "replay.tmp".to_string());
    temp.set_file_name(file_name);
    temp
}
