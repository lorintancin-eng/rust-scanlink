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

#[derive(Debug, Clone, Default)]
pub struct FunderProfile {
    pub address: String,
    pub wallet_count: u32,
    pub rug_exposure: u32,
    pub last_seen_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct AddressSnapshotRecord {
    pub address: String,
    pub oldest_tx_ms: u64,
    pub wallet_age_days: u32,
    pub first_funder: Option<String>,
    pub source: String,
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

#[derive(Debug, Clone)]
pub struct RawEventRecord {
    pub feed_source: String,
    pub event_type: String,
    pub slot: u64,
    pub signature: String,
    pub mint: String,
    pub actor: Option<String>,
    pub recorded_at_ms: u64,
    pub payload_json: String,
}

#[derive(Debug, Clone)]
pub struct FeedHealthRecord {
    pub feed_label: String,
    pub feed_url: String,
    pub status: String,
    pub detail: String,
    pub ts_ms: u64,
}

#[derive(Debug, Clone)]
pub struct FeedFirstHitRecord {
    pub event_key: String,
    pub event_type: String,
    pub mint: String,
    pub signature: String,
    pub slot: u64,
    pub first_feed_source: String,
    pub first_seen_ms: u64,
    pub last_feed_source: String,
    pub last_seen_ms: u64,
    pub distinct_source_count: usize,
    pub lag_to_latest_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RawEventSourceStatRecord {
    pub feed_source: String,
    pub event_type: String,
    pub event_count: usize,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
}

#[derive(Debug, Clone)]
pub struct FeedLatencyStatRecord {
    pub feed_source: String,
    pub event_type: String,
    pub first_hit_count: usize,
    pub cross_feed_match_count: usize,
    pub avg_lag_ms: f64,
    pub avg_cross_feed_lag_ms: f64,
    pub max_lag_ms: u64,
}

#[derive(Debug, Clone)]
pub struct Gate3SnapshotRecord {
    pub mint: String,
    pub mode: String,
    pub path: String,
    pub buy_count: usize,
    pub unique_buyers: usize,
    pub unique_funders: usize,
    pub matched_buyers: usize,
    pub total_sol: f64,
    pub matched_sol: f64,
    pub creator_buy_sol: f64,
    pub max_single_buyer_share: f64,
    pub first_buy_ms: Option<u64>,
    pub threshold_hit_ms: Option<u64>,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct Gate3SequenceRecord {
    pub mint: String,
    pub seq_no: usize,
    pub buyer: String,
    pub funder: Option<String>,
    pub cluster_id: Option<String>,
    pub sol_amount: f64,
    pub detected_at_ms: u64,
    pub is_creator: bool,
    pub feed_source: String,
}

#[derive(Debug, Clone)]
pub struct ScoringBreakdownRecord {
    pub mint: String,
    pub path: String,
    pub quality_score: u32,
    pub urgency_score: u32,
    pub execution_confidence: u32,
    pub total_score: u32,
    pub required_score: u32,
    pub details_json: String,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RiskSignalRecord {
    pub mint: String,
    pub signal_type: String,
    pub signal_value: String,
    pub score: i32,
    pub detected_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct DynamicKeywordRecord {
    pub keyword: String,
    pub source: String,
    pub score: u32,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct UriPatternRecord {
    pub pattern: String,
    pub label: String,
    pub risk_score: i32,
    pub mint_count: u32,
    pub last_seen_ms: u64,
}

#[derive(Debug, Clone)]
pub struct CreatorTemplateRecord {
    pub creator: String,
    pub template_hash: String,
    pub repeat_count: u32,
    pub last_mint: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct LabelSuggestionRecord {
    pub label_type: String,
    pub subject: String,
    pub reason: String,
    pub score: i32,
    pub mint: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct PostTradeOutcomeRecord {
    pub mint: String,
    pub path: String,
    pub score: u32,
    pub metric_type: String,
    pub metric_10s: Option<f64>,
    pub metric_30s: Option<f64>,
    pub metric_60s: Option<f64>,
    pub peak_metric: Option<f64>,
    pub drawdown_metric: Option<f64>,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutionReceiptRecord {
    pub mint: String,
    pub signature: Option<String>,
    pub route_label: String,
    pub status: String,
    pub detail: String,
    pub path: String,
    pub quality_score: u32,
    pub urgency_score: u32,
    pub execution_confidence: u32,
    pub priority_fee_micro_lamport: u64,
    pub jito_tip_lamports: u64,
    pub zero_slot_tip_lamports: u64,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ClusterMemberRecord {
    pub cluster_id: String,
    pub address: String,
    pub cluster_type: String,
    pub score: i32,
}

#[derive(Debug, Clone)]
pub struct ClusterEdgeRecord {
    pub src: String,
    pub dst: String,
    pub edge_type: String,
    pub weight: i32,
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
                .with_context(|| format!("create sqlite parent failed: {}", parent.display()))?;
        }

        let init_path = path.clone();
        tokio::task::spawn_blocking(move || init_db(&init_path))
            .await
            .context("initialize sqlite schema failed")??;

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
        .context("query creator_profiles failed")?
    }

    pub async fn upsert_creator_profile(&self, profile: &CreatorProfile) -> Result<()> {
        let profile = profile.clone();
        self.with_conn(move |conn| {
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
            Ok(())
        })
        .await
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
        .context("query buyer_profiles failed")?
    }

    pub async fn upsert_buyer_profile(&self, profile: &BuyerProfile) -> Result<()> {
        let profile = profile.clone();
        self.with_conn(move |conn| {
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
            Ok(())
        })
        .await
    }

    pub async fn get_funder_profile(&self, address: &str) -> Result<Option<FunderProfile>> {
        let path = self.path.clone();
        let address = address.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            conn.query_row(
                "SELECT address, wallet_count, rug_exposure, last_seen_ms
                 FROM funder_profiles WHERE address = ?1",
                params![address],
                |row| {
                    Ok(FunderProfile {
                        address: row.get(0)?,
                        wallet_count: row.get(1)?,
                        rug_exposure: row.get(2)?,
                        last_seen_ms: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
        .await
        .context("query funder_profiles failed")?
    }

    pub async fn get_address_snapshot(
        &self,
        address: &str,
    ) -> Result<Option<AddressSnapshotRecord>> {
        let path = self.path.clone();
        let address = address.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            conn.query_row(
                "SELECT address, oldest_tx_ms, wallet_age_days, first_funder, source, fetched_at_ms
                 FROM address_snapshots WHERE address = ?1",
                params![address],
                |row| {
                    Ok(AddressSnapshotRecord {
                        address: row.get(0)?,
                        oldest_tx_ms: row.get(1)?,
                        wallet_age_days: row.get(2)?,
                        first_funder: row.get(3)?,
                        source: row.get(4)?,
                        fetched_at_ms: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
        .await
        .context("query address_snapshots failed")?
    }

    pub async fn upsert_funder_profile(&self, profile: &FunderProfile) -> Result<()> {
        let profile = profile.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO funder_profiles(address, wallet_count, rug_exposure, last_seen_ms)
                 VALUES(?1, ?2, ?3, ?4)
                 ON CONFLICT(address) DO UPDATE SET
                    wallet_count = MAX(wallet_count, excluded.wallet_count),
                    rug_exposure = MAX(rug_exposure, excluded.rug_exposure),
                    last_seen_ms = MAX(last_seen_ms, excluded.last_seen_ms)",
                params![
                    profile.address,
                    profile.wallet_count,
                    profile.rug_exposure,
                    profile.last_seen_ms,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn upsert_address_snapshot(&self, snapshot: &AddressSnapshotRecord) -> Result<()> {
        let snapshot = snapshot.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO address_snapshots(address, oldest_tx_ms, wallet_age_days, first_funder, source, fetched_at_ms)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(address) DO UPDATE SET
                    oldest_tx_ms = CASE
                        WHEN address_snapshots.oldest_tx_ms = 0 THEN excluded.oldest_tx_ms
                        WHEN excluded.oldest_tx_ms = 0 THEN address_snapshots.oldest_tx_ms
                        ELSE MIN(address_snapshots.oldest_tx_ms, excluded.oldest_tx_ms)
                    END,
                    wallet_age_days = MAX(address_snapshots.wallet_age_days, excluded.wallet_age_days),
                    first_funder = COALESCE(address_snapshots.first_funder, excluded.first_funder),
                    source = excluded.source,
                    fetched_at_ms = MAX(address_snapshots.fetched_at_ms, excluded.fetched_at_ms)",
                params![
                    snapshot.address,
                    snapshot.oldest_tx_ms,
                    snapshot.wallet_age_days,
                    snapshot.first_funder,
                    snapshot.source,
                    snapshot.fetched_at_ms,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn insert_filter_result(&self, record: &FilterResultRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
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
                    record.ts,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn insert_filter_timing(&self, record: &FilterTimingRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
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
            Ok(())
        })
        .await
    }

    pub async fn insert_raw_event(&self, record: &RawEventRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO raw_events(
                    feed_source, event_type, slot, signature, mint, actor, recorded_at_ms, payload_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    record.feed_source,
                    record.event_type,
                    record.slot,
                    record.signature,
                    record.mint,
                    record.actor,
                    record.recorded_at_ms,
                    record.payload_json,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn insert_feed_health(&self, record: &FeedHealthRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO feed_health(feed_label, feed_url, status, detail, ts_ms)
                 VALUES(?1, ?2, ?3, ?4, ?5)",
                params![
                    record.feed_label,
                    record.feed_url,
                    record.status,
                    record.detail,
                    record.ts_ms,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn upsert_feed_first_hit(&self, record: &FeedFirstHitRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO feed_first_hits(
                    event_key, event_type, mint, signature, slot, first_feed_source, first_seen_ms,
                    last_feed_source, last_seen_ms, distinct_source_count, lag_to_latest_ms
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT(event_key) DO UPDATE SET
                    last_feed_source = excluded.last_feed_source,
                    last_seen_ms = excluded.last_seen_ms,
                    distinct_source_count = excluded.distinct_source_count,
                    lag_to_latest_ms = excluded.lag_to_latest_ms",
                params![
                    record.event_key,
                    record.event_type,
                    record.mint,
                    record.signature,
                    record.slot,
                    record.first_feed_source,
                    record.first_seen_ms,
                    record.last_feed_source,
                    record.last_seen_ms,
                    record.distinct_source_count as u64,
                    record.lag_to_latest_ms,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn insert_gate3_snapshot(&self, record: &Gate3SnapshotRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO gate3_snapshots(
                    mint, mode, path, buy_count, unique_buyers, unique_funders, matched_buyers, total_sol,
                    matched_sol, creator_buy_sol, max_single_buyer_share, first_buy_ms, threshold_hit_ms, recorded_at_ms
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    record.mint,
                    record.mode,
                    record.path,
                    record.buy_count as u64,
                    record.unique_buyers as u64,
                    record.unique_funders as u64,
                    record.matched_buyers as u64,
                    record.total_sol,
                    record.matched_sol,
                    record.creator_buy_sol,
                    record.max_single_buyer_share,
                    record.first_buy_ms,
                    record.threshold_hit_ms,
                    record.recorded_at_ms,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn replace_gate3_sequences(
        &self,
        mint: &str,
        records: &[Gate3SequenceRecord],
    ) -> Result<()> {
        let mint = mint.to_string();
        let records = records.to_vec();
        self.with_conn(move |conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute("DELETE FROM gate3_sequences WHERE mint = ?1", params![mint])?;
            for record in records {
                tx.execute(
                    "INSERT INTO gate3_sequences(
                        mint, seq_no, buyer, funder, cluster_id, sol_amount, detected_at_ms, is_creator, feed_source
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        record.mint,
                        record.seq_no as u64,
                        record.buyer,
                        record.funder,
                        record.cluster_id,
                        record.sol_amount,
                        record.detected_at_ms,
                        if record.is_creator { 1 } else { 0 },
                        record.feed_source,
                    ],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn insert_scoring_breakdown(&self, record: &ScoringBreakdownRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO scoring_breakdowns(
                    mint, path, quality_score, urgency_score, execution_confidence, total_score,
                    required_score, details_json, recorded_at_ms
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    record.mint,
                    record.path,
                    record.quality_score,
                    record.urgency_score,
                    record.execution_confidence,
                    record.total_score,
                    record.required_score,
                    record.details_json,
                    record.recorded_at_ms,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn replace_risk_signals(
        &self,
        mint: &str,
        signals: &[RiskSignalRecord],
    ) -> Result<()> {
        let mint = mint.to_string();
        let signals = signals.to_vec();
        self.with_conn(move |conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute("DELETE FROM token_risk_signals WHERE mint = ?1", params![mint])?;
            for signal in signals {
                tx.execute(
                    "INSERT INTO token_risk_signals(mint, signal_type, signal_value, score, detected_at_ms)
                     VALUES(?1, ?2, ?3, ?4, ?5)",
                    params![
                        signal.mint,
                        signal.signal_type,
                        signal.signal_value,
                        signal.score,
                        signal.detected_at_ms,
                    ],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn replace_dynamic_keywords(
        &self,
        source: &str,
        keywords: &[DynamicKeywordRecord],
    ) -> Result<()> {
        let source = source.to_string();
        let keywords = keywords.to_vec();
        self.with_conn(move |conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute(
                "DELETE FROM dynamic_keywords WHERE source = ?1",
                params![source],
            )?;
            for keyword in keywords {
                tx.execute(
                    "INSERT INTO dynamic_keywords(keyword, source, score, expires_at_ms)
                     VALUES(?1, ?2, ?3, ?4)",
                    params![
                        keyword.keyword,
                        keyword.source,
                        keyword.score,
                        keyword.expires_at_ms,
                    ],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn upsert_uri_pattern(&self, record: &UriPatternRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO uri_patterns(pattern, label, risk_score, mint_count, last_seen_ms)
                 VALUES(?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(pattern) DO UPDATE SET
                    label = excluded.label,
                    risk_score = CASE
                        WHEN excluded.risk_score > uri_patterns.risk_score THEN excluded.risk_score
                        ELSE uri_patterns.risk_score
                    END,
                    mint_count = uri_patterns.mint_count + excluded.mint_count,
                    last_seen_ms = excluded.last_seen_ms",
                params![
                    record.pattern,
                    record.label,
                    record.risk_score,
                    record.mint_count,
                    record.last_seen_ms,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn record_creator_template(
        &self,
        creator: &str,
        template_hash: &str,
        last_mint: Option<&str>,
        updated_at_ms: u64,
    ) -> Result<u32> {
        let creator = creator.to_string();
        let template_hash = template_hash.to_string();
        let last_mint = last_mint.map(str::to_string);
        self.with_conn(move |conn| {
            let existing: Option<u32> = conn
                .query_row(
                    "SELECT repeat_count
                     FROM creator_templates
                     WHERE creator = ?1 AND template_hash = ?2",
                    params![&creator, &template_hash],
                    |row| row.get(0),
                )
                .optional()?;
            let next_count = existing.unwrap_or(0).saturating_add(1);
            conn.execute(
                "INSERT INTO creator_templates(creator, template_hash, repeat_count, last_mint, updated_at_ms)
                 VALUES(?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(creator, template_hash) DO UPDATE SET
                    repeat_count = excluded.repeat_count,
                    last_mint = excluded.last_mint,
                    updated_at_ms = excluded.updated_at_ms",
                params![&creator, &template_hash, next_count, last_mint, updated_at_ms],
            )?;
            Ok(next_count)
        })
        .await
    }

    pub async fn get_funder_blacklist_reason(&self, address: &str) -> Result<Option<String>> {
        let address = address.to_string();
        self.with_conn(move |conn| {
            Ok(conn
                .query_row(
                    "SELECT reason FROM funder_blacklist WHERE address = ?1",
                    params![address],
                    |row| row.get(0),
                )
                .optional()?)
        })
        .await
    }

    pub async fn upsert_funder_blacklist(
        &self,
        address: &str,
        reason: &str,
        updated_at_ms: u64,
    ) -> Result<()> {
        let address = address.to_string();
        let reason = reason.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO funder_blacklist(address, reason, updated_at_ms)
                 VALUES(?1, ?2, ?3)
                 ON CONFLICT(address) DO UPDATE SET
                    reason = excluded.reason,
                    updated_at_ms = excluded.updated_at_ms",
                params![address, reason, updated_at_ms],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn insert_label_suggestion(&self, record: &LabelSuggestionRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO label_suggestions(label_type, subject, reason, score, mint, created_at_ms)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    record.label_type,
                    record.subject,
                    record.reason,
                    record.score,
                    record.mint,
                    record.created_at_ms,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn upsert_post_trade_outcome(&self, record: &PostTradeOutcomeRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO post_trade_outcomes(
                    mint, path, score, metric_type, metric_10s, metric_30s, metric_60s,
                    peak_metric, drawdown_metric, recorded_at_ms
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(mint) DO UPDATE SET
                    path = excluded.path,
                    score = excluded.score,
                    metric_type = excluded.metric_type,
                    metric_10s = excluded.metric_10s,
                    metric_30s = excluded.metric_30s,
                    metric_60s = excluded.metric_60s,
                    peak_metric = excluded.peak_metric,
                    drawdown_metric = excluded.drawdown_metric,
                    recorded_at_ms = excluded.recorded_at_ms",
                params![
                    record.mint,
                    record.path,
                    record.score,
                    record.metric_type,
                    record.metric_10s,
                    record.metric_30s,
                    record.metric_60s,
                    record.peak_metric,
                    record.drawdown_metric,
                    record.recorded_at_ms,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn insert_execution_receipt(&self, record: &ExecutionReceiptRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO execution_receipts(
                    mint, signature, route_label, status, detail, path, quality_score,
                    urgency_score, execution_confidence, priority_fee_micro_lamport,
                    jito_tip_lamports, zero_slot_tip_lamports, recorded_at_ms
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    record.mint,
                    record.signature,
                    record.route_label,
                    record.status,
                    record.detail,
                    record.path,
                    record.quality_score,
                    record.urgency_score,
                    record.execution_confidence,
                    record.priority_fee_micro_lamport,
                    record.jito_tip_lamports,
                    record.zero_slot_tip_lamports,
                    record.recorded_at_ms,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn list_filter_results_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<FilterResultRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT mint, creator, symbol, passed, reject_gate, score, reason, ts
                 FROM filter_results
                 WHERE ts BETWEEN ?1 AND ?2
                 ORDER BY ts ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(FilterResultRecord {
                    mint: row.get(0)?,
                    creator: row.get(1)?,
                    symbol: row.get(2)?,
                    passed: row.get::<_, i64>(3)? != 0,
                    reject_gate: row.get(4)?,
                    score: row.get(5)?,
                    reason: row.get(6)?,
                    ts: row.get(7)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query filter_results window failed")?
    }

    pub async fn list_filter_timings_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<FilterTimingRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT mint, decision, mode, path, detected_at_ms, gate1_at_ms, gate2_at_ms,
                        gate3_open_at_ms, gate3_trigger_at_ms, gate4_at_ms, final_at_ms,
                        latency_ms, early_buy_count, matched_buyers
                 FROM filter_timelines
                 WHERE final_at_ms BETWEEN ?1 AND ?2
                 ORDER BY final_at_ms ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(FilterTimingRecord {
                    mint: row.get(0)?,
                    decision: row.get(1)?,
                    mode: row.get(2)?,
                    path: row.get(3)?,
                    detected_at_ms: row.get(4)?,
                    gate1_at_ms: row.get(5)?,
                    gate2_at_ms: row.get(6)?,
                    gate3_open_at_ms: row.get(7)?,
                    gate3_trigger_at_ms: row.get(8)?,
                    gate4_at_ms: row.get(9)?,
                    final_at_ms: row.get(10)?,
                    latency_ms: row.get(11)?,
                    early_buy_count: row.get::<_, u64>(12)? as usize,
                    matched_buyers: row.get::<_, u64>(13)? as usize,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query filter_timelines window failed")?
    }

    pub async fn list_raw_events_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<RawEventRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT feed_source, event_type, slot, signature, mint, actor, recorded_at_ms, payload_json
                 FROM raw_events
                 WHERE recorded_at_ms BETWEEN ?1 AND ?2
                 ORDER BY recorded_at_ms ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(RawEventRecord {
                    feed_source: row.get(0)?,
                    event_type: row.get(1)?,
                    slot: row.get(2)?,
                    signature: row.get(3)?,
                    mint: row.get(4)?,
                    actor: row.get(5)?,
                    recorded_at_ms: row.get(6)?,
                    payload_json: row.get(7)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query raw_events window failed")?
    }

    pub async fn list_raw_event_source_stats_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<RawEventSourceStatRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT feed_source, event_type, COUNT(*), MIN(recorded_at_ms), MAX(recorded_at_ms)
                 FROM raw_events
                 WHERE recorded_at_ms BETWEEN ?1 AND ?2
                 GROUP BY feed_source, event_type
                 ORDER BY COUNT(*) DESC, feed_source ASC, event_type ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(RawEventSourceStatRecord {
                    feed_source: row.get(0)?,
                    event_type: row.get(1)?,
                    event_count: row.get::<_, u64>(2)? as usize,
                    first_seen_ms: row.get(3)?,
                    last_seen_ms: row.get(4)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query raw_events source stats window failed")?
    }

    pub async fn list_feed_first_hits_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<FeedFirstHitRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT event_key, event_type, mint, signature, slot, first_feed_source, first_seen_ms,
                        last_feed_source, last_seen_ms, distinct_source_count, lag_to_latest_ms
                 FROM feed_first_hits
                 WHERE first_seen_ms BETWEEN ?1 AND ?2
                 ORDER BY first_seen_ms ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(FeedFirstHitRecord {
                    event_key: row.get(0)?,
                    event_type: row.get(1)?,
                    mint: row.get(2)?,
                    signature: row.get(3)?,
                    slot: row.get(4)?,
                    first_feed_source: row.get(5)?,
                    first_seen_ms: row.get(6)?,
                    last_feed_source: row.get(7)?,
                    last_seen_ms: row.get(8)?,
                    distinct_source_count: row.get::<_, u64>(9)? as usize,
                    lag_to_latest_ms: row.get(10)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query feed_first_hits window failed")?
    }

    pub async fn list_feed_latency_stats_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<FeedLatencyStatRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT first_feed_source, event_type, COUNT(*),
                        SUM(CASE WHEN distinct_source_count > 1 THEN 1 ELSE 0 END),
                        AVG(CAST(lag_to_latest_ms AS REAL)),
                        AVG(CASE WHEN distinct_source_count > 1 THEN CAST(lag_to_latest_ms AS REAL) END),
                        MAX(lag_to_latest_ms)
                 FROM feed_first_hits
                 WHERE first_seen_ms BETWEEN ?1 AND ?2
                 GROUP BY first_feed_source, event_type
                 ORDER BY COUNT(*) DESC, first_feed_source ASC, event_type ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(FeedLatencyStatRecord {
                    feed_source: row.get(0)?,
                    event_type: row.get(1)?,
                    first_hit_count: row.get::<_, u64>(2)? as usize,
                    cross_feed_match_count: row.get::<_, u64>(3)? as usize,
                    avg_lag_ms: row.get::<_, Option<f64>>(4)?.unwrap_or_default(),
                    avg_cross_feed_lag_ms: row.get::<_, Option<f64>>(5)?.unwrap_or_default(),
                    max_lag_ms: row.get::<_, Option<u64>>(6)?.unwrap_or_default(),
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query feed_latency_stats window failed")?
    }

    pub async fn list_feed_health_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<FeedHealthRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT feed_label, feed_url, status, detail, ts_ms
                 FROM feed_health
                 WHERE ts_ms BETWEEN ?1 AND ?2
                 ORDER BY ts_ms ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(FeedHealthRecord {
                    feed_label: row.get(0)?,
                    feed_url: row.get(1)?,
                    status: row.get(2)?,
                    detail: row.get(3)?,
                    ts_ms: row.get(4)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query feed_health window failed")?
    }

    pub async fn list_gate3_snapshots_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<Gate3SnapshotRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT mint, mode, path, buy_count, unique_buyers, unique_funders, matched_buyers,
                        total_sol, matched_sol, creator_buy_sol, max_single_buyer_share,
                        first_buy_ms, threshold_hit_ms, recorded_at_ms
                 FROM gate3_snapshots
                 WHERE recorded_at_ms BETWEEN ?1 AND ?2
                 ORDER BY recorded_at_ms ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(Gate3SnapshotRecord {
                    mint: row.get(0)?,
                    mode: row.get(1)?,
                    path: row.get(2)?,
                    buy_count: row.get::<_, u64>(3)? as usize,
                    unique_buyers: row.get::<_, u64>(4)? as usize,
                    unique_funders: row.get::<_, u64>(5)? as usize,
                    matched_buyers: row.get::<_, u64>(6)? as usize,
                    total_sol: row.get(7)?,
                    matched_sol: row.get(8)?,
                    creator_buy_sol: row.get(9)?,
                    max_single_buyer_share: row.get(10)?,
                    first_buy_ms: row.get(11)?,
                    threshold_hit_ms: row.get(12)?,
                    recorded_at_ms: row.get(13)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query gate3_snapshots window failed")?
    }

    pub async fn list_scoring_breakdowns_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<ScoringBreakdownRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT mint, path, quality_score, urgency_score, execution_confidence, total_score,
                        required_score, details_json, recorded_at_ms
                 FROM scoring_breakdowns
                 WHERE recorded_at_ms BETWEEN ?1 AND ?2
                 ORDER BY recorded_at_ms ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(ScoringBreakdownRecord {
                    mint: row.get(0)?,
                    path: row.get(1)?,
                    quality_score: row.get::<_, u64>(2)? as u32,
                    urgency_score: row.get::<_, u64>(3)? as u32,
                    execution_confidence: row.get::<_, u64>(4)? as u32,
                    total_score: row.get::<_, u64>(5)? as u32,
                    required_score: row.get::<_, u64>(6)? as u32,
                    details_json: row.get(7)?,
                    recorded_at_ms: row.get(8)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query scoring_breakdowns window failed")?
    }

    pub async fn list_label_suggestions_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<LabelSuggestionRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT label_type, subject, reason, score, mint, created_at_ms
                 FROM label_suggestions
                 WHERE created_at_ms BETWEEN ?1 AND ?2
                 ORDER BY created_at_ms ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(LabelSuggestionRecord {
                    label_type: row.get(0)?,
                    subject: row.get(1)?,
                    reason: row.get(2)?,
                    score: row.get(3)?,
                    mint: row.get(4)?,
                    created_at_ms: row.get(5)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query label_suggestions window failed")?
    }

    pub async fn list_post_trade_outcomes_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<PostTradeOutcomeRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT mint, path, score, metric_type, metric_10s, metric_30s, metric_60s,
                        peak_metric, drawdown_metric, recorded_at_ms
                 FROM post_trade_outcomes
                 WHERE recorded_at_ms BETWEEN ?1 AND ?2
                 ORDER BY recorded_at_ms ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(PostTradeOutcomeRecord {
                    mint: row.get(0)?,
                    path: row.get(1)?,
                    score: row.get::<_, u64>(2)? as u32,
                    metric_type: row.get(3)?,
                    metric_10s: row.get(4)?,
                    metric_30s: row.get(5)?,
                    metric_60s: row.get(6)?,
                    peak_metric: row.get(7)?,
                    drawdown_metric: row.get(8)?,
                    recorded_at_ms: row.get(9)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query post_trade_outcomes window failed")?
    }

    pub async fn list_execution_receipts_window(
        &self,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<ExecutionReceiptRecord>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_conn(path.as_path())?;
            let mut stmt = conn.prepare(
                "SELECT mint, signature, route_label, status, detail, path, quality_score,
                        urgency_score, execution_confidence, priority_fee_micro_lamport,
                        jito_tip_lamports, zero_slot_tip_lamports, recorded_at_ms
                 FROM execution_receipts
                 WHERE recorded_at_ms BETWEEN ?1 AND ?2
                 ORDER BY recorded_at_ms ASC",
            )?;
            let rows = stmt.query_map(params![from_ms, to_ms], |row| {
                Ok(ExecutionReceiptRecord {
                    mint: row.get(0)?,
                    signature: row.get(1)?,
                    route_label: row.get(2)?,
                    status: row.get(3)?,
                    detail: row.get(4)?,
                    path: row.get(5)?,
                    quality_score: row.get::<_, u64>(6)? as u32,
                    urgency_score: row.get::<_, u64>(7)? as u32,
                    execution_confidence: row.get::<_, u64>(8)? as u32,
                    priority_fee_micro_lamport: row.get(9)?,
                    jito_tip_lamports: row.get(10)?,
                    zero_slot_tip_lamports: row.get(11)?,
                    recorded_at_ms: row.get(12)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
        .context("query execution_receipts window failed")?
    }

    pub async fn upsert_cluster_member(&self, record: &ClusterMemberRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO address_clusters(cluster_id, address, cluster_type, score)
                 VALUES(?1, ?2, ?3, ?4)
                 ON CONFLICT(cluster_id, address) DO UPDATE SET
                    cluster_type = excluded.cluster_type,
                    score = MAX(score, excluded.score)",
                params![
                    record.cluster_id,
                    record.address,
                    record.cluster_type,
                    record.score,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn upsert_cluster_edge(&self, record: &ClusterEdgeRecord) -> Result<()> {
        let record = record.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO cluster_edges(src, dst, edge_type, weight)
                 VALUES(?1, ?2, ?3, ?4)
                 ON CONFLICT(src, dst, edge_type) DO UPDATE SET
                    weight = MAX(weight, excluded.weight)",
                params![record.src, record.dst, record.edge_type, record.weight],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn sync_blacklist(&self, addresses: &[String]) -> Result<()> {
        let entries = addresses.to_vec();
        self.with_conn(move |conn| {
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
            Ok(())
        })
        .await
    }

    pub async fn sync_smart_money(&self, addresses: &[String]) -> Result<()> {
        let entries = addresses.to_vec();
        self.with_conn(move |conn| {
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
            Ok(())
        })
        .await
    }

    pub async fn backfill_entity_graph(&self) -> Result<()> {
        self.with_conn(move |conn| {
            let tx = conn.unchecked_transaction()?;

            tx.execute(
                "INSERT INTO funder_profiles(address, wallet_count, rug_exposure, last_seen_ms)
                 SELECT first_funder, COUNT(*), 0, MAX(fetched_at_ms)
                 FROM buyer_profiles
                 WHERE first_funder IS NOT NULL AND TRIM(first_funder) != ''
                 GROUP BY first_funder
                 ON CONFLICT(address) DO UPDATE SET
                    wallet_count = MAX(wallet_count, excluded.wallet_count),
                    last_seen_ms = MAX(last_seen_ms, excluded.last_seen_ms)",
                [],
            )?;

            tx.execute(
                "INSERT INTO funder_profiles(address, wallet_count, rug_exposure, last_seen_ms)
                 SELECT first_funder, COUNT(*), MAX(rug_count), MAX(fetched_at_ms)
                 FROM creator_profiles
                 WHERE first_funder IS NOT NULL AND TRIM(first_funder) != ''
                 GROUP BY first_funder
                 ON CONFLICT(address) DO UPDATE SET
                    wallet_count = MAX(wallet_count, excluded.wallet_count),
                    rug_exposure = MAX(rug_exposure, excluded.rug_exposure),
                    last_seen_ms = MAX(last_seen_ms, excluded.last_seen_ms)",
                [],
            )?;

            tx.execute(
                "INSERT INTO address_clusters(cluster_id, address, cluster_type, score)
                 SELECT 'funder:' || first_funder, address, 'creator_funder', 100
                 FROM creator_profiles
                 WHERE first_funder IS NOT NULL AND TRIM(first_funder) != ''
                 ON CONFLICT(cluster_id, address) DO UPDATE SET
                    cluster_type = excluded.cluster_type,
                    score = MAX(score, excluded.score)",
                [],
            )?;

            tx.execute(
                "INSERT INTO address_clusters(cluster_id, address, cluster_type, score)
                 SELECT 'funder:' || first_funder, address, 'buyer_funder', 50
                 FROM buyer_profiles
                 WHERE first_funder IS NOT NULL AND TRIM(first_funder) != ''
                 ON CONFLICT(cluster_id, address) DO UPDATE SET
                    cluster_type = excluded.cluster_type,
                    score = MAX(score, excluded.score)",
                [],
            )?;

            tx.execute(
                "INSERT INTO cluster_edges(src, dst, edge_type, weight)
                 SELECT first_funder, address, 'funds_creator_cached', MAX(total_tokens, 1)
                 FROM creator_profiles
                 WHERE first_funder IS NOT NULL AND TRIM(first_funder) != ''
                 ON CONFLICT(src, dst, edge_type) DO UPDATE SET
                    weight = MAX(weight, excluded.weight)",
                [],
            )?;

            tx.execute(
                "INSERT INTO cluster_edges(src, dst, edge_type, weight)
                 SELECT first_funder, address, 'funds_buyer_cached', 1
                 FROM buyer_profiles
                 WHERE first_funder IS NOT NULL AND TRIM(first_funder) != ''
                 ON CONFLICT(src, dst, edge_type) DO UPDATE SET
                    weight = MAX(weight, excluded.weight)",
                [],
            )?;

            tx.commit()?;
            Ok(())
        })
        .await
    }

    async fn with_conn<F, T>(&self, func: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let _guard = self.write_lock.lock().await;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = open_conn(path.as_path())?;
            func(&mut conn)
        })
        .await
        .context("sqlite write task failed")?
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
        CREATE TABLE IF NOT EXISTS funder_profiles (
            address TEXT PRIMARY KEY,
            wallet_count INTEGER NOT NULL DEFAULT 0,
            rug_exposure INTEGER NOT NULL DEFAULT 0,
            last_seen_ms INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS address_snapshots (
            address TEXT PRIMARY KEY,
            oldest_tx_ms INTEGER NOT NULL DEFAULT 0,
            wallet_age_days INTEGER NOT NULL DEFAULT 0,
            first_funder TEXT,
            source TEXT NOT NULL DEFAULT '',
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
        );
        CREATE TABLE IF NOT EXISTS raw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_source TEXT NOT NULL,
            event_type TEXT NOT NULL,
            slot INTEGER NOT NULL,
            signature TEXT NOT NULL,
            mint TEXT NOT NULL,
            actor TEXT,
            recorded_at_ms INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS feed_health (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_label TEXT NOT NULL,
            feed_url TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT NOT NULL,
            ts_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS feed_first_hits (
            event_key TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            mint TEXT NOT NULL,
            signature TEXT NOT NULL,
            slot INTEGER NOT NULL,
            first_feed_source TEXT NOT NULL,
            first_seen_ms INTEGER NOT NULL,
            last_feed_source TEXT NOT NULL,
            last_seen_ms INTEGER NOT NULL,
            distinct_source_count INTEGER NOT NULL DEFAULT 1,
            lag_to_latest_ms INTEGER NOT NULL DEFAULT 0
        );
        CREATE VIEW IF NOT EXISTS feed_latency_stats AS
            SELECT
                first_feed_source AS feed_source,
                event_type,
                COUNT(*) AS first_hit_count,
                SUM(CASE WHEN distinct_source_count > 1 THEN 1 ELSE 0 END) AS cross_feed_match_count,
                AVG(CAST(lag_to_latest_ms AS REAL)) AS avg_lag_ms,
                AVG(CASE WHEN distinct_source_count > 1 THEN CAST(lag_to_latest_ms AS REAL) END) AS avg_cross_feed_lag_ms,
                MAX(lag_to_latest_ms) AS max_lag_ms
            FROM feed_first_hits
            GROUP BY first_feed_source, event_type;
        CREATE TABLE IF NOT EXISTS gate3_snapshots (
            mint TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            path TEXT NOT NULL,
            buy_count INTEGER NOT NULL,
            unique_buyers INTEGER NOT NULL,
            unique_funders INTEGER NOT NULL DEFAULT 0,
            matched_buyers INTEGER NOT NULL DEFAULT 0,
            total_sol REAL NOT NULL DEFAULT 0,
            matched_sol REAL NOT NULL DEFAULT 0,
            creator_buy_sol REAL NOT NULL DEFAULT 0,
            max_single_buyer_share REAL NOT NULL DEFAULT 0,
            first_buy_ms INTEGER,
            threshold_hit_ms INTEGER,
            recorded_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS gate3_sequences (
            mint TEXT NOT NULL,
            seq_no INTEGER NOT NULL,
            buyer TEXT NOT NULL,
            funder TEXT,
            cluster_id TEXT,
            sol_amount REAL NOT NULL DEFAULT 0,
            detected_at_ms INTEGER NOT NULL,
            is_creator INTEGER NOT NULL DEFAULT 0,
            feed_source TEXT NOT NULL,
            PRIMARY KEY(mint, seq_no)
        );
        CREATE TABLE IF NOT EXISTS scoring_breakdowns (
            mint TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            quality_score INTEGER NOT NULL,
            urgency_score INTEGER NOT NULL,
            execution_confidence INTEGER NOT NULL,
            total_score INTEGER NOT NULL,
            required_score INTEGER NOT NULL,
            details_json TEXT NOT NULL,
            recorded_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS token_risk_signals (
            mint TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            signal_value TEXT NOT NULL,
            score INTEGER NOT NULL DEFAULT 0,
            detected_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dynamic_keywords (
            keyword TEXT NOT NULL,
            source TEXT NOT NULL,
            score INTEGER NOT NULL DEFAULT 0,
            expires_at_ms INTEGER NOT NULL,
            PRIMARY KEY(keyword, source)
        );
        CREATE TABLE IF NOT EXISTS label_suggestions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label_type TEXT NOT NULL,
            subject TEXT NOT NULL,
            reason TEXT NOT NULL,
            score INTEGER NOT NULL,
            mint TEXT,
            created_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS uri_patterns (
            pattern TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            risk_score INTEGER NOT NULL DEFAULT 0,
            mint_count INTEGER NOT NULL DEFAULT 0,
            last_seen_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS creator_templates (
            creator TEXT NOT NULL,
            template_hash TEXT NOT NULL,
            repeat_count INTEGER NOT NULL DEFAULT 0,
            last_mint TEXT,
            updated_at_ms INTEGER NOT NULL,
            PRIMARY KEY(creator, template_hash)
        );
        CREATE TABLE IF NOT EXISTS funder_blacklist (
            address TEXT PRIMARY KEY,
            reason TEXT NOT NULL,
            updated_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS address_clusters (
            cluster_id TEXT NOT NULL,
            address TEXT NOT NULL,
            cluster_type TEXT NOT NULL,
            score INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(cluster_id, address)
        );
        CREATE TABLE IF NOT EXISTS cluster_edges (
            src TEXT NOT NULL,
            dst TEXT NOT NULL,
            edge_type TEXT NOT NULL,
            weight INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(src, dst, edge_type)
        );
        CREATE TABLE IF NOT EXISTS post_trade_outcomes (
            mint TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            score INTEGER NOT NULL DEFAULT 0,
            metric_type TEXT NOT NULL,
            metric_10s REAL,
            metric_30s REAL,
            metric_60s REAL,
            peak_metric REAL,
            drawdown_metric REAL,
            recorded_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS execution_receipts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mint TEXT NOT NULL,
            signature TEXT,
            route_label TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT NOT NULL,
            path TEXT NOT NULL,
            quality_score INTEGER NOT NULL DEFAULT 0,
            urgency_score INTEGER NOT NULL DEFAULT 0,
            execution_confidence INTEGER NOT NULL DEFAULT 0,
            priority_fee_micro_lamport INTEGER NOT NULL DEFAULT 0,
            jito_tip_lamports INTEGER NOT NULL DEFAULT 0,
            zero_slot_tip_lamports INTEGER NOT NULL DEFAULT 0,
            recorded_at_ms INTEGER NOT NULL
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
        .with_context(|| format!("open SQLite failed: {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}
