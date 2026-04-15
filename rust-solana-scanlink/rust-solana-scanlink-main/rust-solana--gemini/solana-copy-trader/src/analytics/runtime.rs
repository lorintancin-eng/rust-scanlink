use crate::analytics::attribution::{
    FeedLatencySummary, NamedCount, PercentileSummary, RateSummary, RawEventSourceStat,
};
use crate::config::AppConfig;
use crate::filter::{
    FeedFirstHitRecord, FeedHealthRecord, FeedLatencyStatRecord, FilterDb, FilterResultRecord,
    FilterTimingRecord, RawEventRecord, RawEventSourceStatRecord,
};
use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tracing::info;

#[derive(Debug, Serialize)]
pub struct RuntimeReport {
    pub generated_at_ms: u64,
    pub from_ms: u64,
    pub to_ms: u64,
    pub window_secs: u64,
    pub scanner_mode: String,
    pub execution_enabled: bool,
    pub raw_event_count: usize,
    pub new_token_count: usize,
    pub buy_event_count: usize,
    pub decision_count: usize,
    pub pass_count: usize,
    pub reject_count: usize,
    pub overall_latency_ms: PercentileSummary,
    pub pass_latency_ms: PercentileSummary,
    pub first_hit_lag_ms: PercentileSummary,
    pub feed_breakdown: Vec<NamedCount>,
    pub raw_event_source_stats: Vec<RawEventSourceStat>,
    pub first_hit_breakdown: Vec<NamedCount>,
    pub feed_latency_stats: Vec<FeedLatencySummary>,
    pub deshred_first_hit_rate: RateSummary,
    pub feed_health_breakdown: Vec<NamedCount>,
    pub latest_feed_statuses: Vec<FeedStatusSummary>,
    pub gate_breakdown: Vec<NamedCount>,
    pub path_breakdown: Vec<NamedCount>,
    pub execution_status_breakdown: Vec<NamedCount>,
    pub execution_route_breakdown: Vec<NamedCount>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct FeedStatusSummary {
    pub feed_label: String,
    pub status: String,
    pub ts_ms: u64,
    pub preferred: Option<bool>,
    pub score: Option<i64>,
    pub first_hits: Option<usize>,
    pub stale_ms: Option<u64>,
}

pub async fn build_runtime_report(
    db: &FilterDb,
    config: &AppConfig,
    from_ms: u64,
    to_ms: u64,
) -> Result<RuntimeReport> {
    let raw_events = db.list_raw_events_window(from_ms, to_ms).await?;
    let raw_event_source_stats = db.list_raw_event_source_stats_window(from_ms, to_ms).await?;
    let filter_results = db.list_filter_results_window(from_ms, to_ms).await?;
    let filter_timings = db.list_filter_timings_window(from_ms, to_ms).await?;
    let feed_health = db.list_feed_health_window(from_ms, to_ms).await?;
    let feed_first_hits = db.list_feed_first_hits_window(from_ms, to_ms).await?;
    let feed_latency_stats = db.list_feed_latency_stats_window(from_ms, to_ms).await?;
    let execution_receipts = db.list_execution_receipts_window(from_ms, to_ms).await?;

    let feed_breakdown = count_by(raw_events.iter(), |row| row.feed_source.clone());
    let raw_event_source_stats = to_raw_event_source_stats(raw_event_source_stats);
    let first_hit_breakdown = count_by(feed_first_hits.iter(), |row| row.first_feed_source.clone());
    let feed_latency_stats = summarize_feed_latency(&feed_latency_stats);
    let deshred_first_hit_rate =
        summarize_deshred_first_hit_rate(&feed_first_hits, &config.scanner_deshred_feed_label);
    let feed_health_breakdown = count_by(feed_health.iter(), |row| feed_health_key(row));
    let latest_feed_statuses = summarize_latest_feed_statuses(&feed_health);
    let gate_breakdown = count_by(filter_results.iter(), |row| {
        if row.passed {
            "pass".to_string()
        } else {
            row.reject_gate
                .clone()
                .unwrap_or_else(|| "reject".to_string())
        }
    });
    let path_breakdown = count_by(filter_timings.iter(), |row| row.path.clone());
    let execution_status_breakdown = count_by(execution_receipts.iter(), |row| row.status.clone());
    let execution_route_breakdown =
        count_by(execution_receipts.iter(), |row| row.route_label.clone());

    let new_token_count = unique_event_mints(&raw_events, "new_token");
    let buy_event_count = raw_events.iter().filter(|row| row.event_type == "buy").count();
    let pass_count = filter_results.iter().filter(|row| row.passed).count();
    let reject_count = filter_results.len().saturating_sub(pass_count);
    let overall_latency_ms = summarize_latency(filter_timings.iter().map(|row| row.latency_ms));
    let pass_latency_ms = summarize_latency(
        filter_timings
            .iter()
            .filter(|row| row.decision == "pass")
            .map(|row| row.latency_ms),
    );
    let first_hit_lag_ms =
        summarize_latency(feed_first_hits.iter().map(|row| row.lag_to_latest_ms));
    let warnings = build_runtime_warnings(
        config,
        &raw_events,
        &latest_feed_statuses,
        &deshred_first_hit_rate,
        &execution_status_breakdown,
    );

    Ok(RuntimeReport {
        generated_at_ms: to_ms,
        from_ms,
        to_ms,
        window_secs: to_ms.saturating_sub(from_ms) / 1000,
        scanner_mode: config.scanner_mode.to_string(),
        execution_enabled: config.execution_enabled,
        raw_event_count: raw_events.len(),
        new_token_count,
        buy_event_count,
        decision_count: filter_results.len(),
        pass_count,
        reject_count,
        overall_latency_ms,
        pass_latency_ms,
        first_hit_lag_ms,
        feed_breakdown,
        raw_event_source_stats,
        first_hit_breakdown,
        feed_latency_stats,
        deshred_first_hit_rate,
        feed_health_breakdown,
        latest_feed_statuses,
        gate_breakdown,
        path_breakdown,
        execution_status_breakdown,
        execution_route_breakdown,
        warnings,
    })
}

pub fn log_runtime_report(report: &RuntimeReport) {
    let feed_summary = top_named_counts(&report.feed_breakdown, 4);
    let gate_summary = top_named_counts(&report.gate_breakdown, 5);
    let path_summary = top_named_counts(&report.path_breakdown, 4);
    let exec_summary = top_named_counts(&report.execution_status_breakdown, 4);
    let route_summary = top_named_counts(&report.execution_route_breakdown, 4);
    let health_summary = top_feed_statuses(&report.latest_feed_statuses, 4);
    let warnings = if report.warnings.is_empty() {
        "-".to_string()
    } else {
        report.warnings.join("|")
    };
    info!(
        "Runtime summary | mode={} window={}s raw_events={} new_tokens={} buys={} decisions={} pass={} reject={} overall_p50={}ms pass_p50={}ms first_hit_p50={}ms deshred_first_hit_rate={:.2}% feeds={} gates={} paths={} exec={} routes={} health={} warnings={}",
        report.scanner_mode,
        report.window_secs,
        report.raw_event_count,
        report.new_token_count,
        report.buy_event_count,
        report.decision_count,
        report.pass_count,
        report.reject_count,
        report.overall_latency_ms.p50,
        report.pass_latency_ms.p50,
        report.first_hit_lag_ms.p50,
        report.deshred_first_hit_rate.rate * 100.0,
        feed_summary,
        gate_summary,
        path_summary,
        exec_summary,
        route_summary,
        health_summary,
        warnings,
    );
}

pub async fn persist_runtime_report(report: &RuntimeReport, path: &str) -> Result<()> {
    let target = Path::new(path);
    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create runtime report dir failed: {}", parent.display()))?;
        }
    }
    let bytes = serde_json::to_vec_pretty(report).context("serialize runtime report failed")?;
    tokio::fs::write(target, bytes)
        .await
        .with_context(|| format!("write runtime report failed: {}", target.display()))
}

fn unique_event_mints(rows: &[RawEventRecord], event_type: &str) -> usize {
    rows.iter()
        .filter(|row| row.event_type == event_type)
        .map(|row| row.mint.as_str())
        .collect::<HashSet<_>>()
        .len()
}

fn feed_health_key(row: &FeedHealthRecord) -> String {
    format!("{}:{}", row.feed_label, row.status)
}

fn summarize_latest_feed_statuses(rows: &[FeedHealthRecord]) -> Vec<FeedStatusSummary> {
    let mut latest = HashMap::<&str, &FeedHealthRecord>::new();
    for row in rows {
        match latest.get(row.feed_label.as_str()) {
            Some(existing) if existing.ts_ms >= row.ts_ms => {}
            _ => {
                latest.insert(row.feed_label.as_str(), row);
            }
        }
    }

    let mut statuses: Vec<FeedStatusSummary> = latest
        .into_values()
        .map(|row| {
            let detail = parse_detail_fields(&row.detail);
            FeedStatusSummary {
                feed_label: row.feed_label.clone(),
                status: row.status.clone(),
                ts_ms: row.ts_ms,
                preferred: detail.get("preferred").and_then(|value| parse_bool(value)),
                score: detail.get("score").and_then(|value| value.parse::<i64>().ok()),
                first_hits: detail
                    .get("first_hits")
                    .and_then(|value| value.parse::<usize>().ok()),
                stale_ms: detail.get("stale_ms").and_then(|value| value.parse::<u64>().ok()),
            }
        })
        .collect();
    statuses.sort_by(|left, right| left.feed_label.cmp(&right.feed_label));
    statuses
}

fn parse_detail_fields(detail: &str) -> HashMap<&str, &str> {
    let mut fields = HashMap::new();
    for token in detail.split_whitespace() {
        let Some((key, value)) = token.split_once('=') else {
            continue;
        };
        if key == "detail" {
            break;
        }
        fields.insert(key, value);
    }
    fields
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn build_runtime_warnings(
    config: &AppConfig,
    raw_events: &[RawEventRecord],
    latest_feed_statuses: &[FeedStatusSummary],
    deshred_first_hit_rate: &RateSummary,
    execution_status_breakdown: &[NamedCount],
) -> Vec<String> {
    let mut warnings = Vec::new();

    if raw_events.is_empty() {
        warnings.push("scanner_no_events".to_string());
    }

    if config.scanner_mode.allows_deshred()
        && !latest_feed_statuses
            .iter()
            .any(|row| row.feed_label == config.scanner_deshred_feed_label)
    {
        warnings.push("deshred_feed_missing".to_string());
    }

    if config.scanner_mode.allows_deshred()
        && deshred_first_hit_rate.denominator > 0
        && deshred_first_hit_rate.numerator == 0
    {
        warnings.push("deshred_first_hit_rate_zero".to_string());
    }

    if config.execution_enabled && execution_status_breakdown.is_empty() {
        warnings.push("execution_feedback_empty".to_string());
    }

    warnings
}

fn top_named_counts(rows: &[NamedCount], limit: usize) -> String {
    if rows.is_empty() {
        return "-".to_string();
    }
    rows.iter()
        .take(limit)
        .map(|row| format!("{}:{}", row.name, row.count))
        .collect::<Vec<_>>()
        .join("|")
}

fn top_feed_statuses(rows: &[FeedStatusSummary], limit: usize) -> String {
    if rows.is_empty() {
        return "-".to_string();
    }
    rows.iter()
        .take(limit)
        .map(|row| match (row.preferred, row.stale_ms) {
            (Some(preferred), Some(stale_ms)) => format!(
                "{}:{}:preferred={}:stale={}ms",
                row.feed_label, row.status, preferred, stale_ms
            ),
            _ => format!("{}:{}", row.feed_label, row.status),
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn count_by<T, F>(rows: impl IntoIterator<Item = T>, key_fn: F) -> Vec<NamedCount>
where
    F: Fn(&T) -> String,
{
    let mut counts = HashMap::<String, usize>::new();
    for row in rows {
        *counts.entry(key_fn(&row)).or_default() += 1;
    }
    let mut counts: Vec<NamedCount> = counts
        .into_iter()
        .map(|(name, count)| NamedCount { name, count })
        .collect();
    counts.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.name.cmp(&right.name))
    });
    counts
}

fn to_raw_event_source_stats(rows: Vec<RawEventSourceStatRecord>) -> Vec<RawEventSourceStat> {
    rows.into_iter()
        .map(|row| RawEventSourceStat {
            feed_source: row.feed_source,
            event_type: row.event_type,
            event_count: row.event_count,
            first_seen_ms: row.first_seen_ms,
            last_seen_ms: row.last_seen_ms,
        })
        .collect()
}

fn summarize_feed_latency(rows: &[FeedLatencyStatRecord]) -> Vec<FeedLatencySummary> {
    let total_first_hits: usize = rows.iter().map(|row| row.first_hit_count).sum();
    rows.iter()
        .map(|row| FeedLatencySummary {
            feed_source: row.feed_source.clone(),
            event_type: row.event_type.clone(),
            first_hit_count: row.first_hit_count,
            cross_feed_match_count: row.cross_feed_match_count,
            first_hit_rate: if total_first_hits == 0 {
                0.0
            } else {
                row.first_hit_count as f64 / total_first_hits as f64
            },
            avg_lag_ms: row.avg_lag_ms,
            avg_cross_feed_lag_ms: row.avg_cross_feed_lag_ms,
            max_lag_ms: row.max_lag_ms,
        })
        .collect()
}

fn summarize_deshred_first_hit_rate(
    rows: &[FeedFirstHitRecord],
    deshred_feed_label: &str,
) -> RateSummary {
    let denominator = rows.len();
    let numerator = rows
        .iter()
        .filter(|row| {
            row.first_feed_source == deshred_feed_label
                || row.first_feed_source.contains("deshred")
        })
        .count();
    RateSummary {
        name: "deshred_first_hit_rate".to_string(),
        numerator,
        denominator,
        rate: if denominator == 0 {
            0.0
        } else {
            numerator as f64 / denominator as f64
        },
    }
}

fn summarize_latency(iter: impl Iterator<Item = u64>) -> PercentileSummary {
    let mut values: Vec<u64> = iter.collect();
    if values.is_empty() {
        return PercentileSummary::default();
    }
    values.sort_unstable();
    let count = values.len();
    let sum: u128 = values.iter().map(|value| *value as u128).sum();
    PercentileSummary {
        count,
        min: values[0],
        p50: percentile(&values, 0.50),
        p90: percentile(&values, 0.90),
        p95: percentile(&values, 0.95),
        max: *values.last().unwrap_or(&values[0]),
        avg: sum as f64 / count as f64,
    }
}

fn percentile(values: &[u64], percentile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len().saturating_sub(1)) as f64 * percentile).round() as usize;
    values[index.min(values.len().saturating_sub(1))]
}
