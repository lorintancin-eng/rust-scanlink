use crate::filter::{
    FeedFirstHitRecord, FeedHealthRecord, FeedLatencyStatRecord, FilterDb, FilterResultRecord,
    FilterTimingRecord, Gate3SnapshotRecord, PostTradeOutcomeRecord, RawEventSourceStatRecord,
    ScoringBreakdownRecord,
};
use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
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
    pub raw_event_source_stats: Vec<RawEventSourceStat>,
    pub first_hit_breakdown: Vec<NamedCount>,
    pub first_hit_event_breakdown: Vec<NamedCount>,
    pub feed_latency_stats: Vec<FeedLatencySummary>,
    pub deshred_first_hit_rate: RateSummary,
    pub feed_health_breakdown: Vec<NamedCount>,
    pub gate_breakdown: Vec<NamedCount>,
    pub path_breakdown: Vec<NamedCount>,
    pub label_breakdown: Vec<NamedCount>,
    pub execution_status_breakdown: Vec<NamedCount>,
    pub execution_route_breakdown: Vec<NamedCount>,
    pub first_hit_lag_ms: PercentileSummary,
    pub first_hit_lag_by_source: Vec<NamedPercentileSummary>,
    pub top_passes: Vec<PassSummary>,
    pub dynamic_keyword_contribution: Vec<ContributionSummary>,
    pub risk_signal_contribution: Vec<ContributionSummary>,
    pub cluster_contribution: Vec<ContributionSummary>,
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
pub struct NamedPercentileSummary {
    pub name: String,
    pub stats: PercentileSummary,
}

#[derive(Debug, Serialize)]
pub struct RawEventSourceStat {
    pub feed_source: String,
    pub event_type: String,
    pub event_count: usize,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct FeedLatencySummary {
    pub feed_source: String,
    pub event_type: String,
    pub first_hit_count: usize,
    pub cross_feed_match_count: usize,
    pub first_hit_rate: f64,
    pub avg_lag_ms: f64,
    pub avg_cross_feed_lag_ms: f64,
    pub max_lag_ms: u64,
}

#[derive(Debug, Serialize, Default)]
pub struct RateSummary {
    pub name: String,
    pub numerator: usize,
    pub denominator: usize,
    pub rate: f64,
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

#[derive(Debug, Serialize)]
pub struct ContributionSummary {
    pub name: String,
    pub sample_count: usize,
    pub pass_count: usize,
    pub avg_total_score: f64,
    pub avg_metric_60s: f64,
}

pub async fn build_replay_report(db: &FilterDb, from_ms: u64, to_ms: u64) -> Result<ReplayReport> {
    let raw_events = db.list_raw_events_window(from_ms, to_ms).await?;
    let raw_event_source_stats = db.list_raw_event_source_stats_window(from_ms, to_ms).await?;
    let filter_results = db.list_filter_results_window(from_ms, to_ms).await?;
    let filter_timings = db.list_filter_timings_window(from_ms, to_ms).await?;
    let feed_health = db.list_feed_health_window(from_ms, to_ms).await?;
    let feed_first_hits = db.list_feed_first_hits_window(from_ms, to_ms).await?;
    let feed_latency_stats = db.list_feed_latency_stats_window(from_ms, to_ms).await?;
    let _gate3_snapshots: Vec<Gate3SnapshotRecord> =
        db.list_gate3_snapshots_window(from_ms, to_ms).await?;
    let scoring_breakdowns = db.list_scoring_breakdowns_window(from_ms, to_ms).await?;
    let label_suggestions = db.list_label_suggestions_window(from_ms, to_ms).await?;
    let outcomes = db.list_post_trade_outcomes_window(from_ms, to_ms).await?;
    let execution_receipts = db.list_execution_receipts_window(from_ms, to_ms).await?;

    let feed_breakdown = count_by(raw_events.iter(), |row| row.feed_source.clone());
    let raw_event_source_stats = to_raw_event_source_stats(raw_event_source_stats);
    let first_hit_breakdown = count_by(feed_first_hits.iter(), |row| row.first_feed_source.clone());
    let first_hit_event_breakdown = count_by(feed_first_hits.iter(), |row| {
        format!("{}:{}", row.event_type, row.first_feed_source)
    });
    let feed_latency_stats = summarize_feed_latency(&feed_latency_stats);
    let deshred_first_hit_rate = summarize_deshred_first_hit_rate(&feed_first_hits);
    let feed_health_breakdown = count_by(feed_health.iter(), feed_health_key);
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
    let execution_status_breakdown = count_by(execution_receipts.iter(), |row| row.status.clone());
    let execution_route_breakdown =
        count_by(execution_receipts.iter(), |row| row.route_label.clone());

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
    let first_hit_lag_by_source = summarize_grouped_first_hit_latency(
        &feed_first_hits,
        |row| row.first_feed_source.clone(),
    );
    let top_passes = build_top_passes(&filter_results, &filter_timings, &scoring_breakdowns);
    let (dynamic_keyword_contribution, risk_signal_contribution, cluster_contribution) =
        summarize_contributions(&filter_results, &scoring_breakdowns, &outcomes);
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
        raw_event_source_stats,
        first_hit_breakdown,
        first_hit_event_breakdown,
        feed_latency_stats,
        deshred_first_hit_rate,
        feed_health_breakdown,
        gate_breakdown,
        path_breakdown,
        label_breakdown,
        execution_status_breakdown,
        execution_route_breakdown,
        first_hit_lag_ms,
        first_hit_lag_by_source,
        top_passes,
        dynamic_keyword_contribution,
        risk_signal_contribution,
        cluster_contribution,
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

fn feed_health_key(row: &FeedHealthRecord) -> String {
    format!("{}:{}", row.feed_label, row.status)
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

fn summarize_feed_latency(rows: &[FeedLatencyStatRecord]) -> Vec<FeedLatencySummary> {
    let mut totals: HashMap<String, usize> = HashMap::new();
    for row in rows {
        *totals.entry(row.event_type.clone()).or_default() += row.first_hit_count;
    }

    let mut out: Vec<_> = rows
        .iter()
        .map(|row| FeedLatencySummary {
            feed_source: row.feed_source.clone(),
            event_type: row.event_type.clone(),
            first_hit_count: row.first_hit_count,
            cross_feed_match_count: row.cross_feed_match_count,
            first_hit_rate: if let Some(total) = totals.get(&row.event_type) {
                if *total == 0 {
                    0.0
                } else {
                    row.first_hit_count as f64 / *total as f64
                }
            } else {
                0.0
            },
            avg_lag_ms: row.avg_lag_ms,
            avg_cross_feed_lag_ms: row.avg_cross_feed_lag_ms,
            max_lag_ms: row.max_lag_ms,
        })
        .collect();
    out.sort_by(|left, right| {
        right
            .first_hit_count
            .cmp(&left.first_hit_count)
            .then_with(|| left.feed_source.cmp(&right.feed_source))
            .then_with(|| left.event_type.cmp(&right.event_type))
    });
    out
}

fn summarize_deshred_first_hit_rate(rows: &[FeedFirstHitRecord]) -> RateSummary {
    let numerator = rows
        .iter()
        .filter(|row| row.first_feed_source.to_ascii_lowercase().contains("deshred"))
        .count();
    let denominator = rows.len();
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

fn summarize_grouped_first_hit_latency<F>(
    items: &[FeedFirstHitRecord],
    mut key_fn: F,
) -> Vec<NamedPercentileSummary>
where
    F: FnMut(&FeedFirstHitRecord) -> String,
{
    let mut buckets: HashMap<String, Vec<u64>> = HashMap::new();
    for item in items {
        let key = key_fn(item);
        buckets.entry(key).or_default().push(item.lag_to_latest_ms);
    }

    let mut rows: Vec<_> = buckets
        .into_iter()
        .map(|(name, values)| NamedPercentileSummary {
            name,
            stats: summarize_latency(values.into_iter()),
        })
        .collect();
    rows.sort_by(|left, right| {
        right
            .stats
            .count
            .cmp(&left.stats.count)
            .then_with(|| left.name.cmp(&right.name))
    });
    rows
}

fn percentile(values: &[u64], pct: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let idx = ((values.len() - 1) as f64 * pct).round() as usize;
    values[idx.min(values.len() - 1)]
}

fn summarize_contributions(
    filter_results: &[FilterResultRecord],
    scoring_breakdowns: &[ScoringBreakdownRecord],
    outcomes: &[PostTradeOutcomeRecord],
) -> (
    Vec<ContributionSummary>,
    Vec<ContributionSummary>,
    Vec<ContributionSummary>,
) {
    let passes_by_mint: HashMap<&str, bool> = filter_results
        .iter()
        .map(|row| (row.mint.as_str(), row.passed))
        .collect();
    let outcome_60s_by_mint: HashMap<&str, f64> = outcomes
        .iter()
        .filter_map(|row| row.metric_60s.map(|metric| (row.mint.as_str(), metric)))
        .collect();

    let mut dynamic = ContributionAccumulator::default();
    let mut risk = ContributionAccumulator::default();
    let mut cluster = ContributionAccumulator::default();

    for row in scoring_breakdowns {
        let parsed: Value = serde_json::from_str(&row.details_json).unwrap_or_default();
        let passed = passes_by_mint.get(row.mint.as_str()).copied().unwrap_or(false);
        let metric_60s = outcome_60s_by_mint.get(row.mint.as_str()).copied();

        let dynamic_keywords = parsed
            .get("dynamic_narrative_keywords")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for keyword in dynamic_keywords.iter().filter_map(Value::as_str) {
            dynamic.add(keyword, passed, row.total_score as f64, metric_60s);
        }

        let risk_signals = parsed
            .get("risk_signals")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for signal in risk_signals
            .iter()
            .filter_map(|value| value.get("type").and_then(Value::as_str))
        {
            risk.add(signal, passed, row.total_score as f64, metric_60s);
        }

        if parsed
            .get("suspicious_funder_penalty")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            > 0
        {
            cluster.add(
                "suspicious_funder_cluster",
                passed,
                row.total_score as f64,
                metric_60s,
            );
        }
        if parsed
            .get("same_cluster_first_buy_penalty")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            > 0
        {
            cluster.add(
                "same_funder_cluster_pressure",
                passed,
                row.total_score as f64,
                metric_60s,
            );
        }
        if parsed
            .get("quality_cluster")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            cluster.add(
                "quality_cluster",
                passed,
                row.total_score as f64,
                metric_60s,
            );
        }
        if parsed
            .get("fast_required_score_relief")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            > 0
        {
            cluster.add(
                "quality_cluster_fast_relief",
                passed,
                row.total_score as f64,
                metric_60s,
            );
        }
    }

    (dynamic.finish(), risk.finish(), cluster.finish())
}

#[derive(Default)]
struct ContributionAccumulator {
    rows: HashMap<String, ContributionAccumulatorRow>,
}

#[derive(Default)]
struct ContributionAccumulatorRow {
    sample_count: usize,
    pass_count: usize,
    total_score_sum: f64,
    metric_60s_sum: f64,
    metric_60s_count: usize,
}

impl ContributionAccumulator {
    fn add(&mut self, name: &str, passed: bool, total_score: f64, metric_60s: Option<f64>) {
        let row = self.rows.entry(name.to_string()).or_default();
        row.sample_count += 1;
        if passed {
            row.pass_count += 1;
        }
        row.total_score_sum += total_score;
        if let Some(metric) = metric_60s {
            row.metric_60s_sum += metric;
            row.metric_60s_count += 1;
        }
    }

    fn finish(self) -> Vec<ContributionSummary> {
        let mut rows: Vec<_> = self
            .rows
            .into_iter()
            .map(|(name, row)| ContributionSummary {
                name,
                sample_count: row.sample_count,
                pass_count: row.pass_count,
                avg_total_score: if row.sample_count == 0 {
                    0.0
                } else {
                    row.total_score_sum / row.sample_count as f64
                },
                avg_metric_60s: if row.metric_60s_count == 0 {
                    0.0
                } else {
                    row.metric_60s_sum / row.metric_60s_count as f64
                },
            })
            .collect();
        rows.sort_by(|left, right| {
            right
                .sample_count
                .cmp(&left.sample_count)
                .then_with(|| left.name.cmp(&right.name))
        });
        rows
    }
}
