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
pub struct LabelSuggestionRecord {
    pub label_type: String,
    pub subject: String,
    pub reason: String,
    pub score: i32,
    pub mint: Option<String>,
    pub created_at_ms: u64,
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

    async fn with_conn<F>(&self, func: F) -> Result<()>
    where
        F: FnOnce(&mut Connection) -> Result<()> + Send + 'static,
    {
        let _guard = self.write_lock.lock().await;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = open_conn(path.as_path())?;
            func(&mut conn)?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("sqlite write task failed")??;
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
