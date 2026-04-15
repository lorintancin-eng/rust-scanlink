mod player;

use crate::analytics::attribution::{build_replay_report, ReplayReport};
use crate::config::AppConfig;
use crate::filter::FilterDb;
use anyhow::{Context, Result};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

pub async fn run(config: &AppConfig) -> Result<ReplayReport> {
    let to_ms = config.replay_to_ms.unwrap_or_else(now_ms);
    let from_ms = config
        .replay_from_ms
        .unwrap_or_else(|| to_ms.saturating_sub(config.replay_window_minutes * 60_000));
    let report_db_path = if config.replay_pipeline_enabled {
        let pipeline = player::run_pipeline(config, from_ms, to_ms).await?;
        info!(
            "Replay pipeline complete | source_events={} replayed={} buy_signals={} duration_ms={} speedup={:.1} replay_db={}",
            pipeline.source_event_count,
            pipeline.replayed_event_count,
            pipeline.emitted_buy_signals,
            pipeline.elapsed_ms,
            pipeline.speedup,
            config.replay_db_path,
        );
        &config.replay_db_path
    } else {
        &config.filter_db_path
    };
    let db = FilterDb::new(report_db_path).await?;
    let report = build_replay_report(&db, from_ms, to_ms).await?;
    write_report(&config.replay_report_file, &report).await?;
    info!(
        "Replay report ready | from_ms={} | to_ms={} | decisions={} | pass={} | source_db={} | output={}",
        report.from_ms,
        report.to_ms,
        report.decision_count,
        report.pass_count,
        report_db_path,
        config.replay_report_file,
    );
    Ok(report)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

async fn write_report(path: &str, report: &ReplayReport) -> Result<()> {
    let path_ref = Path::new(path);
    if let Some(parent) = path_ref.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let content = serde_json::to_vec_pretty(report)?;
    tokio::fs::write(path_ref, content)
        .await
        .with_context(|| format!("write replay report failed: {}", path_ref.display()))?;
    Ok(())
}
