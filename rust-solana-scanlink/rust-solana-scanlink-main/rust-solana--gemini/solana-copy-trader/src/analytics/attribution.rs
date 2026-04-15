use crate::filter::{
    FilterDb, FilterResultRecord, FilterTimingRecord, Gate3SnapshotRecord, LabelSuggestionRecord,
    PostTradeOutcomeRecord, RawEventRecord, ScoringBreakdownRecord,
};
use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub struct ReplayReport {
    pub from_ms: u64,
    pub to_ms: u64,
    pub raw_event_count: usize,
    pub decision_count: usize,
    pub pass_count: usize,
    pub reject_count: usize,
    pub overall_latency_ms: PercentileSummary,
    pub pass_latency_ms: PercentileSummary,
    pub feed_breakdown: Vec<NamedCount>,
    pub gate_breakdown: Vec<NamedCount>,
    pub path_breakdown: Vec<NamedCount>,
    pub label_breakdown: Vec<NamedCount>,
    pub top_passes: Vec<PassSummary>,
    pub outcome_summary: OutcomeSummary,
}

#[derive(Debug, Serialize, Default)]
pub struct PercentileSummary {
    pub count: usize,
    pub min: u64,
    pub p50: u64,
    pub p90: u64,
    pub p95: u64,
    pub max: u64,
    pub avg: f64,
}

#[derive(Debug, Serialize)]
pub struct NamedCount {
    pub name: String,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct PassSummary {
    pub mint: String,
    pub symbol: String,
    pub score: Option<u32>,
    pub latency_ms: u64,
    pub path: String,
}

#[derive(Debug, Serialize, Default)]
pub struct OutcomeSummary {
    pub count: usize,
    pub metric_type: String,
    pub avg_metric_10s: f64,
    pub avg_metric_30s: f64,
    pub avg_metric_60s: f64,
    pub avg_peak_metric: f64,
    pub avg_drawdown_metric: f64,
}

pub async fn build_replay_report(db: &FilterDb, from_ms: u64, to_ms: u64) -> Result<ReplayReport> {
    let raw_events = db.list_raw_events_window(from_ms, to_ms).await?;
    let filter_results = db.list_filter_results_window(from_ms, to_ms).await?;
    let filter_timings = db.list_filter_timings_window(from_ms, to_ms).await?;
    let _gate3_snapshots: Vec<Gate3SnapshotRecord> =
        db.list_gate3_snapshots_window(from_ms, to_ms).await?;
    let scoring_breakdowns = db.list_scoring_breakdowns_window(from_ms, to_ms).await?;
    let label_suggestions = db.list_label_suggestions_window(from_ms, to_ms).await?;
    let outcomes = db.list_post_trade_outcomes_window(from_ms, to_ms).await?;

    let feed_breakdown = count_by(raw_events.iter(), |row| row.feed_source.clone());
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
    let label_breakdown = count_by(label_suggestions.iter(), |row| row.label_type.clone());

    let pass_count = filter_results.iter().filter(|row| row.passed).count();
    let reject_count = filter_results.len().saturating_sub(pass_count);
    let overall_latency_ms = summarize_latency(filter_timings.iter().map(|row| row.latency_ms));
    let pass_latency_ms = summarize_latency(
        filter_timings
            .iter()
            .filter(|row| row.decision == "pass")
            .map(|row| row.latency_ms),
    );
    let top_passes = build_top_passes(&filter_results, &filter_timings, &scoring_breakdowns);
    let outcome_summary = summarize_outcomes(&outcomes);

    Ok(ReplayReport {
        from_ms,
        to_ms,
        raw_event_count: raw_events.len(),
        decision_count: filter_results.len(),
        pass_count,
        reject_count,
        overall_latency_ms,
        pass_latency_ms,
        feed_breakdown,
        gate_breakdown,
        path_breakdown,
        label_breakdown,
        top_passes,
        outcome_summary,
    })
}

fn build_top_passes(
    filter_results: &[FilterResultRecord],
    filter_timings: &[FilterTimingRecord],
    scoring_breakdowns: &[ScoringBreakdownRecord],
) -> Vec<PassSummary> {
    let timing_by_mint: HashMap<&str, &FilterTimingRecord> = filter_timings
        .iter()
        .map(|row| (row.mint.as_str(), row))
        .collect();
    let score_by_mint: HashMap<&str, &ScoringBreakdownRecord> = scoring_breakdowns
        .iter()
        .map(|row| (row.mint.as_str(), row))
        .collect();
    let mut passes: Vec<PassSummary> = filter_results
        .iter()
        .filter(|row| row.passed)
        .filter_map(|row| {
            let timing = timing_by_mint.get(row.mint.as_str())?;
            Some(PassSummary {
                mint: row.mint.clone(),
                symbol: row.symbol.clone(),
                score: score_by_mint
                    .get(row.mint.as_str())
                    .map(|record| record.total_score)
                    .or(row.score),
                latency_ms: timing.latency_ms,
                path: timing.path.clone(),
            })
        })
        .collect();
    passes.sort_by_key(|row| row.latency_ms);
    passes.truncate(20);
    passes
}

fn summarize_outcomes(outcomes: &[PostTradeOutcomeRecord]) -> OutcomeSummary {
    if outcomes.is_empty() {
        return OutcomeSummary::default();
    }
    let count = outcomes.len() as f64;
    let metric_type = outcomes
        .first()
        .map(|row| row.metric_type.clone())
        .unwrap_or_else(|| "unknown".to_string());
    OutcomeSummary {
        count: outcomes.len(),
        metric_type,
        avg_metric_10s: outcomes
            .iter()
            .filter_map(|row| row.metric_10s)
            .sum::<f64>()
            / count,
        avg_metric_30s: outcomes
            .iter()
            .filter_map(|row| row.metric_30s)
            .sum::<f64>()
            / count,
        avg_metric_60s: outcomes
            .iter()
            .filter_map(|row| row.metric_60s)
            .sum::<f64>()
            / count,
        avg_peak_metric: outcomes
            .iter()
            .filter_map(|row| row.peak_metric)
            .sum::<f64>()
            / count,
        avg_drawdown_metric: outcomes
            .iter()
            .filter_map(|row| row.drawdown_metric)
            .sum::<f64>()
            / count,
    }
}

fn count_by<T, F>(items: impl Iterator<Item = T>, mut key_fn: F) -> Vec<NamedCount>
where
    F: FnMut(T) -> String,
{
    let mut counts: HashMap<String, usize> = HashMap::new();
    for item in items {
        *counts.entry(key_fn(item)).or_default() += 1;
    }
    let mut rows: Vec<NamedCount> = counts
        .into_iter()
        .map(|(name, count)| NamedCount { name, count })
        .collect();
    rows.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.name.cmp(&right.name))
    });
    rows
}

fn summarize_latency(iter: impl Iterator<Item = u64>) -> PercentileSummary {
    let mut values: Vec<u64> = iter.collect();
    if values.is_empty() {
        return PercentileSummary::default();
    }
    values.sort_unstable();
    let count = values.len();
    let sum = values.iter().copied().sum::<u64>() as f64;
    PercentileSummary {
        count,
        min: values[0],
        p50: percentile(&values, 0.50),
        p90: percentile(&values, 0.90),
        p95: percentile(&values, 0.95),
        max: *values.last().unwrap_or(&values[0]),
        avg: sum / count as f64,
    }
}

fn percentile(values: &[u64], pct: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let idx = ((values.len() - 1) as f64 * pct).round() as usize;
    values[idx.min(values.len() - 1)]
}
