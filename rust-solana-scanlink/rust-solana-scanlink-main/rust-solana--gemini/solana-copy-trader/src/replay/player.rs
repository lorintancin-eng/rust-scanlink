use crate::config::AppConfig;
use crate::filter;
use crate::filter::FilterDb;
use crate::scanner::raw_event::raw_event_to_scanner_event;
use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

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

    let source_db = FilterDb::new(&config.filter_db_path).await?;
    let raw_events = source_db.list_raw_events_window(from_ms, to_ms).await?;
    let source_event_count = raw_events.len();

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
    for (recorded_at_ms, event) in &replayed_events {
        let delta_ms = recorded_at_ms.saturating_sub(previous_ts);
        let scaled_ms = ((delta_ms as f64) / config.replay_speedup.max(1.0)).round() as u64;
        if scaled_ms > 0 {
            tokio::time::sleep(Duration::from_millis(scaled_ms.min(250))).await;
        }
        if scanner_tx.send(event.clone()).await.is_err() {
            break;
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
    if tokio::fs::try_exists(source).await? {
        tokio::fs::copy(source, target).await.with_context(|| {
            format!(
                "copy replay db failed: {} -> {}",
                source.display(),
                target.display()
            )
        })?;
    }
    Ok(())
}
