use crate::config::AppConfig;
use crate::filter::db::{
    AddressSnapshotRecord, BuyerProfile, ClusterEdgeRecord, ClusterMemberRecord, CreatorProfile,
    DynamicKeywordRecord, FeedHealthRecord, FilterDb, FilterResultRecord, FilterTimingRecord,
    FunderProfile, Gate3SequenceRecord, Gate3SnapshotRecord, LabelSuggestionRecord,
    PostTradeOutcomeRecord, RawEventRecord, RiskSignalRecord, ScoringBreakdownRecord,
    UriPatternRecord,
};
use crate::filter::risk::{
    analyze_gate1_risk, analyze_runtime_risk, derive_template_identity, Gate1RiskProfile,
    RiskSignalSeed, RuntimeRiskInput, RuntimeRiskProfile,
};
use crate::processor::pumpfun::BondingCurveState;
use crate::scanner::{
    NewToken, PumpBuyEvent, DISC_CREATE, DISC_CREATE_V2, PUMP_PROGRAM_ID,
    SCANNER_NEW_TOKEN_CHANNEL_CAPACITY,
};
use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use reqwest::StatusCode;
use serde_json::{json, Value};
use solana_client::rpc_client::{GetConfirmedSignaturesForAddress2Config, RpcClient};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

const GATE1_BLACK_KEYWORDS: &[&str] = &[
    "test", "rug", "scam", "fake", "honeypot", "rugpull", "ponzi",
];
const GATE1_WHITE_KEYWORDS: &[&str] = &["ai", "agent", "trump", "pepe", "maga"];
const DYNAMIC_KEYWORD_STOPWORDS: &[&str] = &[
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "your",
    "coin",
    "token",
    "official",
    "solana",
    "pump",
    "pumpfun",
    "community",
    "just",
    "latest",
    "launch",
    "new",
    "best",
];
const CREATOR_CACHE_TTL_MS: u64 = 24 * 60 * 60 * 1000;
const BUYER_CACHE_TTL_MS: u64 = 6 * 60 * 60 * 1000;
const FACTORY_WINDOW_MS: u64 = 5 * 60 * 1000;
const FACTORY_THRESHOLD: usize = 3;
const CREATOR_TOTAL_TOKEN_LIMIT: u32 = 100;
const CREATOR_RUG_LIMIT: u32 = 3;
const CURVE_TOTAL_TARGET_SOL: f64 = 69.0;
const CURVE_INITIAL_VIRTUAL_SOL: u64 = 30_000_000_000;
const OLD_WALLET_DAYS: u64 = 7;
const HELIUS_PAGE_LIMIT: usize = 100;
const HELIUS_MAX_PAGES: usize = 5;
const FALLBACK_SM_THRESHOLD: usize = 2;
const DAY_MS: u64 = 24 * 60 * 60 * 1000;
const ADDRESS_SNAPSHOT_CACHE_TTL_MS: u64 = 15 * 60 * 1000;
const FUNDER_WALLET_CLUSTER_LIMIT: u32 = 24;
const FUNDER_RUG_EXPOSURE_LIMIT: u32 = 3;
const GATE3_CLUSTER_DIVERSITY_MIN_BUYS: usize = 4;
const GATE3_MIN_UNIQUE_FUNDERS: usize = 2;
const SINGLE_FUNDER_SCORE_PENALTY: u32 = 6;
const CREATOR_FUNDER_CLUSTER_MIN_BUYERS: usize = 2;
const CREATOR_FUNDER_CLUSTER_MIN_SOL: f64 = 0.5;
const CREATOR_FUNDER_CLUSTER_MAX_SHARE: f64 = 0.85;
const SUSPICIOUS_FUNDER_CLUSTER_WARNING_WALLETS: u32 = 10;
const SUSPICIOUS_FUNDER_CLUSTER_WARNING_RUG_EXPOSURE: u32 = 1;
const SUSPICIOUS_FUNDER_CLUSTER_PENALTY_SCORE: u32 = 6;
const SAME_FUNDER_CLUSTER_PENALTY_MIN_BUYS: usize = 3;
const SAME_FUNDER_CLUSTER_PENALTY_SHARE: f64 = 0.66;
const SAME_FUNDER_CLUSTER_PENALTY_SCORE: u32 = 8;
const QUALITY_CLUSTER_MIN_UNIQUE_FUNDERS: usize = 3;
const QUALITY_CLUSTER_MIN_HOT_FUNDERS: usize = 2;
const QUALITY_CLUSTER_MAX_SHARE: f64 = 0.60;
const QUALITY_CLUSTER_BONUS_SCORE: u32 = 6;
const FAST_QUALITY_CLUSTER_SCORE_RELIEF: u32 = 4;
const RAW_NEW_TOKEN_PERSIST_QUEUE_CAPACITY: usize = 1_024;
const RAW_BUY_PERSIST_QUEUE_CAPACITY: usize = 8_192;
const NARRATIVE_SOURCE_BASE_MARKET: &str = "base_market";
const NARRATIVE_SOURCE_PREHEAT_SOCIAL: &str = "preheat_social";
const NARRATIVE_SOURCE_PREHEAT_TELEGRAM: &str = "preheat_telegram";
const NARRATIVE_SOURCE_PREHEAT_EVENT: &str = "preheat_event";
const NARRATIVE_SOURCE_CONFIRMED_ONCHAIN: &str = "confirmed_onchain";
const NARRATIVE_MASK_BASE_MARKET: u32 = 1 << 0;
const NARRATIVE_MASK_SOCIAL: u32 = 1 << 1;
const NARRATIVE_MASK_TELEGRAM: u32 = 1 << 2;
const NARRATIVE_MASK_EVENT: u32 = 1 << 3;
const NARRATIVE_MASK_ONCHAIN: u32 = 1 << 4;

#[derive(Debug, Clone)]
pub struct BuySignal {
    pub token: NewToken,
    pub score: u32,
    pub quality_score: u32,
    pub urgency_score: u32,
    pub execution_confidence: u32,
    pub path: String,
    pub reason: String,
    pub sm_count: usize,
    pub sm_sol_total: f64,
    pub latency_ms: u64,
    pub trigger_trade: PumpBuyEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateStatus {
    CreatorPending,
    Active,
    Finalizing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
enum NarrativeTier {
    #[default]
    Preheat,
    Base,
    Confirmed,
}

#[derive(Debug, Clone, Default)]
struct NarrativeHotTerm {
    tier: NarrativeTier,
    score: u32,
    source_mask: u32,
}

#[derive(Debug, Clone)]
struct NarrativeKeywordMatch {
    keyword: String,
    tier: NarrativeTier,
    score: u32,
    source_mask: u32,
}

#[derive(Debug, Clone, Default)]
struct CandidateTrace {
    gate1_at_ms: Option<u64>,
    gate2_at_ms: Option<u64>,
    gate3_open_at_ms: Option<u64>,
    gate3_trigger_at_ms: Option<u64>,
    gate4_at_ms: Option<u64>,
    path: Option<String>,
}

#[derive(Debug, Clone)]
struct Candidate {
    token: NewToken,
    created_at: Instant,
    detected_at_ms: u64,
    status: CandidateStatus,
    gate1_risk: Gate1RiskProfile,
    narrative_keywords: Vec<String>,
    dynamic_narrative_keywords: Vec<String>,
    narrative_keyword_matches: Vec<NarrativeKeywordMatch>,
    early_buys: Vec<PumpBuyEvent>,
    buy_signatures: HashSet<String>,
    creator_profile: Option<CreatorProfile>,
    buyer_profiles: HashMap<String, BuyerProfile>,
    pending_buyer_profiles: HashSet<String>,
    trace: CandidateTrace,
}

#[derive(Debug, Clone, Default)]
struct HotLists {
    creator_blacklist: HashSet<String>,
    smart_money: HashSet<String>,
    smart_money_funders: HashSet<String>,
    blocked_buyers: HashSet<String>,
    dynamic_hot_keywords: HashSet<String>,
    narrative_terms: HashMap<String, NarrativeHotTerm>,
}

#[derive(Debug)]
enum InternalMessage {
    CreatorGateResolved {
        mint: String,
        token: NewToken,
        result: CreatorGateResult,
    },
    BuyerProfileResolved {
        mint: String,
        address: String,
        profile: Option<BuyerProfile>,
    },
    Scored {
        mint: String,
        decision: ScoreDecision,
        gate4_at_ms: Option<u64>,
    },
}

#[derive(Debug)]
struct Gate1Result {
    token: NewToken,
    decision: Gate1Decision,
    gate1_at_ms: u64,
}

#[derive(Debug, Clone)]
struct Gate1PersistArtifacts {
    creator: String,
    template_hash: String,
    template_repeat_count: u32,
    mint: String,
    updated_at_ms: u64,
    uri_pattern_record: Option<UriPatternRecord>,
}

#[derive(Debug, Clone)]
enum FilterPersistTask {
    Gate1Artifacts(Gate1PersistArtifacts),
    CreatorLinks {
        token: NewToken,
        profile: CreatorProfile,
    },
    BuyerLinks {
        mint: String,
        profile: BuyerProfile,
    },
    CandidateAnalytics {
        candidate: Candidate,
        passed: bool,
        reject_gate: Option<String>,
        score: Option<u32>,
        reason: String,
        mode: String,
        path: String,
    },
}

#[derive(Debug, Clone)]
struct CreatorGateResult {
    passed: bool,
    reason: String,
    profile: Option<CreatorProfile>,
}

#[derive(Debug, Clone)]
struct ScoreDecision {
    passed: bool,
    gate: String,
    score: u32,
    quality_score: u32,
    urgency_score: u32,
    execution_confidence: u32,
    reason: String,
    signal: Option<BuySignal>,
    mode: String,
    path: String,
    matched_buyers: usize,
    early_buy_count: usize,
    gate4_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SmartMoneyMode {
    Hotlist,
    EarlyBuyerFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate3Path {
    Fast,
    Soft,
}

#[derive(Debug, Clone, Copy)]
struct Gate3Trigger {
    path: Gate3Path,
    threshold: usize,
}

#[derive(Debug, Clone)]
struct WindowStats {
    mode: SmartMoneyMode,
    fast_threshold: usize,
    soft_threshold: usize,
    unique_sm_wallets: HashSet<String>,
    sm_sol_total: f64,
    total_eligible_sol: f64,
    fastest_sm_ms: Option<u64>,
    fast_reached_at_ms: Option<u64>,
    soft_reached_at_ms: Option<u64>,
    buy_count: usize,
    eligible_buyers: usize,
    unique_funders: usize,
    creator_funder_match_count: usize,
    creator_funder_match_sol: f64,
    max_single_buyer_share: f64,
    max_single_buyer: Option<String>,
    creator_buy_count: usize,
    creator_buy_sol: f64,
    largest_funder_cluster_size: usize,
    largest_funder_cluster_share: f64,
    hotlist_funder_hits: usize,
    hotlist_funder_diversity: usize,
    elapsed_ms: u64,
}

#[derive(Debug, Clone, Default)]
struct AddressSnapshot {
    oldest_tx_ms: u64,
    wallet_age_days: u32,
    first_funder: Option<String>,
    source: String,
}

#[derive(Debug, Clone)]
struct CachedAddressSnapshot {
    snapshot: AddressSnapshot,
    fetched_at_ms: u64,
}

#[derive(Debug, Clone, Default)]
struct CreatorClusterProfile {
    funder: Option<String>,
    wallet_count: u32,
    rug_exposure: u32,
}

#[derive(Debug, Clone, Default)]
struct ClusterAdjustment {
    creator_funder: Option<String>,
    creator_cluster_wallet_count: u32,
    creator_cluster_rug_exposure: u32,
    suspicious_funder_penalty: u32,
    same_cluster_first_buy_penalty: u32,
    quality_cluster_bonus: u32,
    fast_required_score_relief: u32,
    largest_funder_cluster_size: usize,
    largest_funder_cluster_share: f64,
    hotlist_funder_hits: usize,
    hotlist_funder_diversity: usize,
    quality_cluster: bool,
}

#[derive(Debug, Clone, Default)]
struct NarrativeAdjustment {
    preheat_hits: usize,
    base_hits: usize,
    confirmed_hits: usize,
    preheat_bonus: u32,
    base_bonus: u32,
    confirmed_bonus: u32,
    fast_required_score_relief: u32,
}

#[derive(Debug, Clone)]
struct ScoringContext {
    participants_score: u32,
    capital_score: u32,
    momentum_score: u32,
    curve_score: u32,
    buyer_quality_score: u32,
    dynamic_narrative_bonus: u32,
    narrative: NarrativeAdjustment,
    funder_diversity_penalty: u32,
    runtime_risk: RuntimeRiskProfile,
    cluster: ClusterAdjustment,
    curve_progress_pct: f64,
    buyer_quality_pct: f64,
    quality_score: u32,
    urgency_score: u32,
    execution_confidence: u32,
    total_score: u32,
    required_score: u32,
}

#[derive(Clone)]
struct SharedState {
    config: Arc<AppConfig>,
    rpc_client: Arc<RpcClient>,
    http: reqwest::Client,
    db: FilterDb,
    persist_tx: mpsc::UnboundedSender<FilterPersistTask>,
    raw_new_token_persist_tx: mpsc::Sender<NewToken>,
    raw_buy_persist_tx: mpsc::Sender<PumpBuyEvent>,
    hotlists: Arc<RwLock<HotLists>>,
    address_snapshot_cache: Arc<RwLock<HashMap<String, CachedAddressSnapshot>>>,
    address_snapshot_refreshes: Arc<RwLock<HashSet<String>>>,
    address_snapshot_helius_cooldown_until_ms: Arc<RwLock<u64>>,
}

pub async fn run(
    config: Arc<AppConfig>,
    db: FilterDb,
    rpc_client: Arc<RpcClient>,
    new_token_rx: mpsc::Receiver<NewToken>,
    mut buy_rx: mpsc::Receiver<PumpBuyEvent>,
    buy_signal_tx: mpsc::Sender<BuySignal>,
) -> Result<()> {
    let (raw_new_token_persist_tx, raw_new_token_persist_rx) =
        mpsc::channel::<NewToken>(RAW_NEW_TOKEN_PERSIST_QUEUE_CAPACITY);
    let (raw_buy_persist_tx, raw_buy_persist_rx) =
        mpsc::channel::<PumpBuyEvent>(RAW_BUY_PERSIST_QUEUE_CAPACITY);
    let (persist_tx, persist_rx) = mpsc::unbounded_channel::<FilterPersistTask>();
    let shared = SharedState {
        config: config.clone(),
        rpc_client,
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .context("filter http client init failed")?,
        db,
        persist_tx,
        raw_new_token_persist_tx,
        raw_buy_persist_tx,
        hotlists: Arc::new(RwLock::new(HotLists::default())),
        address_snapshot_cache: Arc::new(RwLock::new(HashMap::new())),
        address_snapshot_refreshes: Arc::new(RwLock::new(HashSet::new())),
        address_snapshot_helius_cooldown_until_ms: Arc::new(RwLock::new(0)),
    };
    spawn_filter_persist_worker(shared.clone(), persist_rx);
    spawn_raw_new_token_persist_worker(shared.clone(), raw_new_token_persist_rx);
    spawn_raw_buy_persist_worker(shared.clone(), raw_buy_persist_rx);
    if config.dynamic_hot_keywords_enabled {
        if let Err(err) = refresh_dynamic_hot_keywords(&shared).await {
            warn!("dynamic hot keyword refresh failed during startup: {}", err);
        }
    }
    reload_hotlists(&shared).await?;
    if let Err(err) = shared.db.backfill_entity_graph().await {
        warn!("entity graph backfill failed during startup: {}", err);
    }
    let creator_template_counts = match shared.db.list_creator_template_counts().await {
        Ok(counts) => counts,
        Err(err) => {
            warn!(
                "creator template preload failed during startup; using empty cache: {}",
                err
            );
            HashMap::new()
        }
    };
    spawn_hotlist_refresh_worker(shared.clone());
    let (gate1_result_tx, mut gate1_result_rx) =
        mpsc::channel::<Gate1Result>(SCANNER_NEW_TOKEN_CHANNEL_CAPACITY);
    spawn_gate1_worker(
        shared.clone(),
        new_token_rx,
        gate1_result_tx,
        creator_template_counts,
    );

    let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<InternalMessage>();
    let mut candidates: HashMap<String, Candidate> = HashMap::new();

    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut gate1_stream_open = true;
    let mut buy_stream_open = true;
    let mut internal_stream_open = true;

    loop {
        if !gate1_stream_open && !buy_stream_open && !internal_stream_open {
            break;
        }
        tokio::select! {
            biased;
            maybe_gate1 = gate1_result_rx.recv(), if gate1_stream_open => {
                let Some(result) = maybe_gate1 else {
                    gate1_stream_open = false;
                    continue;
                };
                handle_gate1_result(&shared, &internal_tx, &mut candidates, result).await?;
            }
            maybe_msg = internal_rx.recv(), if internal_stream_open => {
                let Some(msg) = maybe_msg else {
                    internal_stream_open = false;
                    continue;
                };
                match msg {
                    InternalMessage::CreatorGateResolved { mint, token, result } => {
                        handle_creator_gate_resolution(&shared, &internal_tx, &mut candidates, mint, token, result).await;
                    }
                    InternalMessage::BuyerProfileResolved { mint, address, profile } => {
                        handle_buyer_profile_resolution(&shared, &internal_tx, &mut candidates, mint, address, profile).await;
                    }
                    InternalMessage::Scored {
                        mint,
                        decision,
                        gate4_at_ms,
                    } => {
                        if let Some(mut candidate) = candidates.remove(&mint) {
                            candidate.trace.gate4_at_ms = gate4_at_ms;
                            record_candidate_outcome(
                                &shared,
                                &candidate,
                                decision.passed,
                                if decision.passed { None } else { Some(decision.gate.clone()) },
                                if decision.score > 0 { Some(decision.score) } else { None },
                                decision.reason.clone(),
                                decision.mode,
                                decision.path,
                                decision.early_buy_count,
                                decision.matched_buyers,
                            ).await;
                        }

                        if let Some(signal) = decision.signal {
                            if let Err(err) = append_jsonl(
                                &shared.config.passed_tokens_file,
                                &json!({
                                    "detected_at_ms": signal.token.detected_at_ms,
                                    "mint": &signal.token.mint,
                                    "symbol": &signal.token.symbol,
                                    "name": &signal.token.name,
                                    "creator": &signal.token.creator,
                                    "score": signal.score,
                                    "sm_count": signal.sm_count,
                                    "sm_sol_total": signal.sm_sol_total,
                                    "latency_ms": signal.latency_ms,
                                    "reason": &signal.reason,
                                }),
                            ).await {
                                warn!("passed_tokens append failed: {}", err);
                            }

                            if buy_signal_tx.send(signal).await.is_err() {
                                warn!("filter: execution channel closed");
                                break;
                            }
                        }
                    }
                }
            }
            maybe_buy = buy_rx.recv(), if buy_stream_open => {
                let Some(buy) = maybe_buy else {
                    buy_stream_open = false;
                    continue;
                };
                handle_buy_event(&shared, &internal_tx, &mut candidates, buy).await;
            }
            _ = tick.tick() => {
                let mut expired = Vec::new();
                for (mint, candidate) in &candidates {
                    if candidate.status != CandidateStatus::Active {
                        continue;
                    }
                    let stats = smart_money_stats(candidate, &shared).await;
                    if let Some(reason) = gate3_reject_reason(candidate, &stats, &shared.config) {
                        let path = gate3_reject_path(&reason).to_string();
                        expired.push((
                            mint.clone(),
                            reason,
                            smart_money_mode_label(stats.mode).to_string(),
                            path,
                            stats.buy_count,
                            stats.unique_sm_wallets.len(),
                        ));
                    }
                }

                for (mint, reason, mode, path, buy_count, matched_buyers) in expired {
                    if let Some(candidate) = candidates.remove(&mint) {
                        record_candidate_outcome(
                            &shared,
                            &candidate,
                            false,
                            Some("gate3".to_string()),
                            None,
                            reason,
                            mode,
                            path,
                            buy_count,
                            matched_buyers,
                        ).await;
                    }
                }
            }
        }
    }

    Ok(())
}

fn spawn_hotlist_refresh_worker(shared: SharedState) {
    let hot_reload_enabled = shared.config.filter_hot_reload_secs > 0;
    let dynamic_hot_enabled = shared.config.dynamic_hot_keywords_enabled;
    if !hot_reload_enabled && !dynamic_hot_enabled {
        return;
    }

    tokio::spawn(async move {
        let mut hot_reload = tokio::time::interval(Duration::from_secs(
            shared.config.filter_hot_reload_secs.max(1),
        ));
        hot_reload.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        hot_reload.tick().await;

        let mut dynamic_hot_refresh = tokio::time::interval(Duration::from_secs(
            shared.config.dynamic_hot_refresh_secs.max(30),
        ));
        dynamic_hot_refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        dynamic_hot_refresh.tick().await;

        loop {
            tokio::select! {
                _ = hot_reload.tick(), if hot_reload_enabled => {
                    if let Err(err) = reload_hotlists(&shared).await {
                        warn!("filter: hot reload failed: {}", err);
                    }
                }
                _ = dynamic_hot_refresh.tick(), if dynamic_hot_enabled => {
                    if let Err(err) = refresh_dynamic_hot_keywords(&shared).await {
                        warn!("dynamic hot keyword refresh failed: {}", err);
                    } else if let Err(err) = reload_hotlists(&shared).await {
                        warn!("filter: hot reload after dynamic refresh failed: {}", err);
                    }
                }
            }
        }
    });
}

fn spawn_gate1_worker(
    shared: SharedState,
    mut new_token_rx: mpsc::Receiver<NewToken>,
    gate1_result_tx: mpsc::Sender<Gate1Result>,
    mut creator_template_counts: HashMap<(String, String), u32>,
) {
    tokio::spawn(async move {
        let mut creator_window: HashMap<String, VecDeque<u64>> = HashMap::new();
        while let Some(token) = new_token_rx.recv().await {
            let (decision, persist_artifacts) = gate1_check(
                &shared,
                &mut creator_window,
                &mut creator_template_counts,
                &token,
            )
            .await;
            let gate1_at_ms = now_ms();
            enqueue_raw_new_token_persist(&shared, &token);
            if let Some(artifacts) = persist_artifacts {
                enqueue_persist_task(&shared, FilterPersistTask::Gate1Artifacts(artifacts));
            }
            if gate1_result_tx
                .send(Gate1Result {
                    token,
                    decision,
                    gate1_at_ms,
                })
                .await
                .is_err()
            {
                break;
            }
        }
        info!("filter: gate1 worker stopped");
    });
}

#[derive(Debug, Clone)]
struct Gate1Decision {
    passed: bool,
    reason: String,
    risk: Gate1RiskProfile,
    narrative_keywords: Vec<String>,
    dynamic_narrative_keywords: Vec<String>,
    narrative_keyword_matches: Vec<NarrativeKeywordMatch>,
}

async fn handle_gate1_result(
    shared: &SharedState,
    internal_tx: &mpsc::UnboundedSender<InternalMessage>,
    candidates: &mut HashMap<String, Candidate>,
    result: Gate1Result,
) -> Result<()> {
    let Gate1Result {
        token,
        decision,
        gate1_at_ms,
    } = result;
    if candidates.contains_key(&token.mint) {
        return Ok(());
    }

    let Gate1Decision {
        passed: gate1_passed,
        reason: gate1_reason,
        risk: gate1_risk,
        narrative_keywords: gate1_narrative_keywords,
        dynamic_narrative_keywords: gate1_dynamic_keywords,
        narrative_keyword_matches,
    } = decision;
    if !gate1_passed {
        let trace = CandidateTrace {
            gate1_at_ms: Some(gate1_at_ms),
            ..Default::default()
        };
        record_token_outcome(
            shared,
            &token,
            &trace,
            false,
            Some("gate1".to_string()),
            None,
            gate1_reason,
            "gate1".to_string(),
            "immediate".to_string(),
            0,
            0,
            Some(risk_signal_records_from_seeds(
                &token.mint,
                &gate1_risk.signals,
                now_ms(),
            )),
        )
        .await;
        return Ok(());
    }

    candidates.insert(
        token.mint.clone(),
        Candidate {
            token: token.clone(),
            created_at: Instant::now(),
            detected_at_ms: token.detected_at_ms,
            status: CandidateStatus::CreatorPending,
            gate1_risk: gate1_risk,
            narrative_keywords: gate1_narrative_keywords,
            dynamic_narrative_keywords: gate1_dynamic_keywords,
            narrative_keyword_matches,
            early_buys: Vec::new(),
            buy_signatures: HashSet::new(),
            creator_profile: None,
            buyer_profiles: HashMap::new(),
            pending_buyer_profiles: HashSet::new(),
            trace: CandidateTrace {
                gate1_at_ms: Some(gate1_at_ms),
                ..Default::default()
            },
        },
    );

    let shared_clone = shared.clone();
    let tx_clone = internal_tx.clone();
    tokio::spawn(async move {
        let result = creator_gate(&shared_clone, &token)
            .await
            .unwrap_or_else(|err| CreatorGateResult {
                passed: true,
                reason: format!("gate2 fallback pass: {}", err),
                profile: None,
            });
        let _ = tx_clone.send(InternalMessage::CreatorGateResolved {
            mint: token.mint.clone(),
            token,
            result,
        });
    });

    Ok(())
}

async fn handle_creator_gate_resolution(
    shared: &SharedState,
    internal_tx: &mpsc::UnboundedSender<InternalMessage>,
    candidates: &mut HashMap<String, Candidate>,
    mint: String,
    token: NewToken,
    result: CreatorGateResult,
) {
    let Some(candidate) = candidates.get_mut(&mint) else {
        return;
    };

    candidate.trace.gate2_at_ms = Some(now_ms());
    if !result.passed {
        let candidate = candidates.remove(&mint).unwrap_or_else(|| unreachable!());
        record_candidate_outcome(
            shared,
            &candidate,
            false,
            Some("gate2".to_string()),
            None,
            result.reason,
            "creator_profile".to_string(),
            "creator_gate".to_string(),
            candidate.early_buys.len(),
            0,
        )
        .await;
        return;
    }

    let Some(candidate) = candidates.get_mut(&mint) else {
        warn!(
            "filter: candidate disappeared after gate2 pass | mint={}",
            token.mint
        );
        return;
    };

    candidate.status = CandidateStatus::Active;
    candidate.creator_profile = result.profile;
    if let Some(profile) = candidate.creator_profile.as_ref() {
        enqueue_persist_task(
            shared,
            FilterPersistTask::CreatorLinks {
                token: candidate.token.clone(),
                profile: profile.clone(),
            },
        );
    }
    candidate.trace.gate3_open_at_ms.get_or_insert_with(now_ms);

    if let Some(trigger) = should_finalize(candidate, shared).await {
        candidate.status = CandidateStatus::Finalizing;
        candidate.trace.gate3_trigger_at_ms = Some(now_ms());
        candidate.trace.path = Some(gate3_path_label(trigger.path).to_string());
        spawn_score_task(shared, internal_tx, candidate.clone());
    }
}

async fn handle_buyer_profile_resolution(
    shared: &SharedState,
    internal_tx: &mpsc::UnboundedSender<InternalMessage>,
    candidates: &mut HashMap<String, Candidate>,
    mint: String,
    address: String,
    profile: Option<BuyerProfile>,
) {
    let Some(candidate) = candidates.get_mut(&mint) else {
        return;
    };
    candidate.pending_buyer_profiles.remove(&address);
    if let Some(profile) = profile {
        enqueue_persist_task(
            shared,
            FilterPersistTask::BuyerLinks {
                mint: candidate.token.mint.clone(),
                profile: profile.clone(),
            },
        );
        candidate.buyer_profiles.insert(address, profile);
    }

    if candidate.status != CandidateStatus::Active {
        return;
    }

    if let Some(trigger) = should_finalize(candidate, shared).await {
        candidate.status = CandidateStatus::Finalizing;
        candidate.trace.gate3_trigger_at_ms = Some(now_ms());
        candidate.trace.path = Some(gate3_path_label(trigger.path).to_string());
        spawn_score_task(shared, internal_tx, candidate.clone());
    }
}

async fn handle_buy_event(
    shared: &SharedState,
    internal_tx: &mpsc::UnboundedSender<InternalMessage>,
    candidates: &mut HashMap<String, Candidate>,
    buy: PumpBuyEvent,
) {
    enqueue_raw_buy_persist(shared, &buy);
    let mut reject_now: Option<(String, String, String, String, usize, usize)> = None;

    {
        let Some(candidate) = candidates.get_mut(&buy.mint) else {
            return;
        };
        if candidate.status == CandidateStatus::Finalizing {
            return;
        }
        if let Some(existing_buy) = candidate
            .early_buys
            .iter_mut()
            .find(|existing| existing.signature == buy.signature)
        {
            if upgrade_existing_buy_event(existing_buy, &buy) {
                evaluate_candidate_after_buy(shared, internal_tx, candidate, &mut reject_now).await;
            }
            return;
        }
        if candidate.early_buys.len() >= shared.config.smart_money_max_buys {
            return;
        }
        if buy
            .detected_at
            .duration_since(candidate.created_at)
            .as_millis() as u64
            > effective_hard_reject_ms(&shared.config)
        {
            return;
        }

        candidate.buy_signatures.insert(buy.signature.clone());
        candidate.early_buys.push(buy.clone());
        candidate.trace.gate3_open_at_ms.get_or_insert_with(now_ms);

        maybe_spawn_buyer_profile_fetch(shared, internal_tx, candidate, &buy);

        evaluate_candidate_after_buy(shared, internal_tx, candidate, &mut reject_now).await;
    }

    if let Some((mint, reason, mode, path, buy_count, matched_buyers)) = reject_now {
        if let Some(candidate) = candidates.remove(&mint) {
            record_candidate_outcome(
                shared,
                &candidate,
                false,
                Some("gate3".to_string()),
                None,
                reason,
                mode,
                path,
                buy_count,
                matched_buyers,
            )
            .await;
        }
    }
}

async fn evaluate_candidate_after_buy(
    shared: &SharedState,
    internal_tx: &mpsc::UnboundedSender<InternalMessage>,
    candidate: &mut Candidate,
    reject_now: &mut Option<(String, String, String, String, usize, usize)>,
) {
    if candidate.status != CandidateStatus::Active {
        return;
    }

    let stats = smart_money_stats(candidate, shared).await;
    if let Some(reason) = gate3_reject_reason(candidate, &stats, &shared.config) {
        if gate3_is_immediate_reject_reason(&reason) {
            *reject_now = Some((
                candidate.token.mint.clone(),
                reason.clone(),
                smart_money_mode_label(stats.mode).to_string(),
                gate3_reject_path(&reason).to_string(),
                stats.buy_count,
                stats.unique_sm_wallets.len(),
            ));
        }
    } else if let Some(trigger) = gate3_trigger_from_stats(shared.config.as_ref(), &stats) {
        candidate.status = CandidateStatus::Finalizing;
        candidate.trace.gate3_trigger_at_ms = Some(now_ms());
        candidate.trace.path = Some(gate3_path_label(trigger.path).to_string());
        spawn_score_task(shared, internal_tx, candidate.clone());
    }
}

fn upgrade_existing_buy_event(existing: &mut PumpBuyEvent, incoming: &PumpBuyEvent) -> bool {
    let existing_rank = buy_feed_rank(&existing.feed_source);
    let incoming_rank = buy_feed_rank(&incoming.feed_source);
    let mut upgraded = false;

    if incoming.sol_amount_lamports > existing.sol_amount_lamports {
        existing.sol_amount_lamports = incoming.sol_amount_lamports;
        upgraded = true;
    }

    if incoming_rank > existing_rank {
        existing.feed_source = incoming.feed_source.clone();
        existing.token_program = incoming.token_program;
        existing.instruction_data = incoming.instruction_data.clone();
        existing.instruction_accounts = incoming.instruction_accounts.clone();
        upgraded = true;
    }

    upgraded
}

fn buy_feed_rank(feed_source: &str) -> u8 {
    if feed_source.to_ascii_lowercase().contains("deshred") {
        0
    } else {
        1
    }
}

fn maybe_spawn_buyer_profile_fetch(
    shared: &SharedState,
    internal_tx: &mpsc::UnboundedSender<InternalMessage>,
    candidate: &mut Candidate,
    buy: &PumpBuyEvent,
) {
    let Some(api_key) = shared.config.helius_api_key.clone() else {
        return;
    };

    let address = buy.buyer.to_string();
    if candidate.buyer_profiles.contains_key(&address)
        || candidate.pending_buyer_profiles.contains(&address)
    {
        return;
    }

    candidate.pending_buyer_profiles.insert(address.clone());
    let shared_clone = shared.clone();
    let tx_clone = internal_tx.clone();
    let mint = candidate.token.mint.clone();
    tokio::spawn(async move {
        let profile = fetch_buyer_profile(&shared_clone, &api_key, &address)
            .await
            .map_err(|err| {
                warn!(
                    "filter: buyer profile fetch failed | buyer={} | {}",
                    address, err
                );
                err
            })
            .ok();
        let _ = tx_clone.send(InternalMessage::BuyerProfileResolved {
            mint,
            address,
            profile,
        });
    });
}

fn spawn_score_task(
    shared: &SharedState,
    internal_tx: &mpsc::UnboundedSender<InternalMessage>,
    candidate: Candidate,
) {
    let shared = shared.clone();
    let tx = internal_tx.clone();
    tokio::spawn(async move {
        let mint = candidate.token.mint.clone();
        let decision = score_candidate(&shared, candidate).await;
        let gate4_at_ms = decision.gate4_at_ms;
        let _ = tx.send(InternalMessage::Scored {
            mint,
            decision,
            gate4_at_ms,
        });
    });
}

fn collect_narrative_keywords(
    token: &NewToken,
    narrative_terms: &HashMap<String, NarrativeHotTerm>,
) -> (Vec<String>, Vec<String>, Vec<NarrativeKeywordMatch>) {
    let haystack = format!("{} {}", token.name, token.symbol);
    let tokens = tokenize_keyword_text(&haystack);

    let mut narrative_keywords: Vec<String> = GATE1_WHITE_KEYWORDS
        .iter()
        .filter(|kw| tokens.contains(**kw))
        .map(|kw| (*kw).to_string())
        .collect();

    let mut matched_terms: Vec<NarrativeKeywordMatch> = narrative_terms
        .iter()
        .filter(|(kw, _)| tokens.contains(kw.as_str()))
        .map(|(keyword, term)| NarrativeKeywordMatch {
            keyword: keyword.clone(),
            tier: term.tier,
            score: term.score,
            source_mask: term.source_mask,
        })
        .collect();

    matched_terms.sort_by(|a, b| {
        b.tier
            .cmp(&a.tier)
            .then_with(|| b.score.cmp(&a.score))
            .then_with(|| a.keyword.cmp(&b.keyword))
    });
    matched_terms.dedup_by(|a, b| a.keyword == b.keyword);

    let mut dynamic_narrative_keywords: Vec<String> = matched_terms
        .iter()
        .map(|item| item.keyword.clone())
        .collect();

    dynamic_narrative_keywords.sort();
    dynamic_narrative_keywords.dedup();
    narrative_keywords.extend(dynamic_narrative_keywords.iter().cloned());
    narrative_keywords.sort();
    narrative_keywords.dedup();

    (
        narrative_keywords,
        dynamic_narrative_keywords,
        matched_terms,
    )
}

fn narrative_tier_for_source(source: &str) -> NarrativeTier {
    if source.starts_with("confirmed_") {
        NarrativeTier::Confirmed
    } else if source.starts_with("base_") {
        NarrativeTier::Base
    } else {
        NarrativeTier::Preheat
    }
}

fn narrative_source_mask(source: &str) -> u32 {
    match source {
        NARRATIVE_SOURCE_BASE_MARKET => NARRATIVE_MASK_BASE_MARKET,
        NARRATIVE_SOURCE_PREHEAT_SOCIAL => NARRATIVE_MASK_SOCIAL,
        NARRATIVE_SOURCE_PREHEAT_TELEGRAM => NARRATIVE_MASK_TELEGRAM,
        NARRATIVE_SOURCE_PREHEAT_EVENT => NARRATIVE_MASK_EVENT,
        NARRATIVE_SOURCE_CONFIRMED_ONCHAIN => NARRATIVE_MASK_ONCHAIN,
        _ => 0,
    }
}

fn merge_narrative_keyword_record(
    map: &mut HashMap<String, NarrativeHotTerm>,
    record: &DynamicKeywordRecord,
) {
    let tier = narrative_tier_for_source(&record.source);
    let source_mask = narrative_source_mask(&record.source);
    let entry = map.entry(record.keyword.clone()).or_default();
    if tier > entry.tier {
        entry.tier = tier;
    }
    entry.score = entry.score.saturating_add(record.score);
    entry.source_mask |= source_mask;
}

fn normalize_seed_keywords(lines: Vec<String>) -> Vec<String> {
    extract_dynamic_keywords_from_texts(lines)
}

fn build_ranked_keyword_records(
    source: &str,
    keywords: Vec<String>,
    now_ms: u64,
    ttl_secs: u64,
) -> Vec<DynamicKeywordRecord> {
    let expires_at_ms = now_ms.saturating_add(ttl_secs.saturating_mul(1000));
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    for keyword in keywords {
        if seen.insert(keyword.clone()) {
            ordered.push(keyword);
        }
    }
    let total = ordered.len().max(1) as u32;
    ordered
        .into_iter()
        .enumerate()
        .map(|(idx, keyword)| DynamicKeywordRecord {
            keyword,
            source: source.to_string(),
            score: total.saturating_sub(idx as u32),
            expires_at_ms,
        })
        .collect()
}

fn build_counted_keyword_records(
    source: &str,
    keywords: Vec<(String, u32)>,
    now_ms: u64,
    ttl_secs: u64,
) -> Vec<DynamicKeywordRecord> {
    let expires_at_ms = now_ms.saturating_add(ttl_secs.saturating_mul(1000));
    keywords
        .into_iter()
        .map(|(keyword, score)| DynamicKeywordRecord {
            keyword,
            source: source.to_string(),
            score,
            expires_at_ms,
        })
        .collect()
}

fn sort_narrative_snapshot(
    map: &HashMap<String, NarrativeHotTerm>,
    limit: usize,
) -> Vec<(String, NarrativeHotTerm)> {
    let mut items: Vec<(String, NarrativeHotTerm)> = map
        .iter()
        .map(|(keyword, term)| (keyword.clone(), term.clone()))
        .collect();
    items.sort_by(|a, b| {
        b.1.tier
            .cmp(&a.1.tier)
            .then_with(|| b.1.score.cmp(&a.1.score))
            .then_with(|| a.0.cmp(&b.0))
    });
    items.truncate(limit.max(1));
    items
}

async fn fetch_onchain_confirmed_keywords(shared: &SharedState) -> Result<Vec<(String, u32)>> {
    let now = now_ms();
    let from_ms = now.saturating_sub(
        shared
            .config
            .narrative_onchain_confirm_window_secs
            .saturating_mul(1000),
    );
    let rows = shared.db.list_raw_events_window(from_ms, now).await?;
    let mut mints_by_keyword: HashMap<String, HashSet<String>> = HashMap::new();

    for row in rows {
        if row.event_type != "new_token" {
            continue;
        }
        let Ok(payload) = serde_json::from_str::<Value>(&row.payload_json) else {
            continue;
        };
        let mut texts = Vec::new();
        if let Some(name) = payload.get("name").and_then(Value::as_str) {
            texts.push(name.to_string());
        }
        if let Some(symbol) = payload.get("symbol").and_then(Value::as_str) {
            texts.push(symbol.to_string());
        }
        if texts.is_empty() {
            continue;
        }
        let keywords = extract_dynamic_keywords_from_texts(texts);
        for keyword in keywords {
            mints_by_keyword
                .entry(keyword)
                .or_default()
                .insert(row.mint.clone());
        }
    }

    let mut ranked: Vec<(String, u32)> = mints_by_keyword
        .into_iter()
        .filter_map(|(keyword, mints)| {
            let count = mints.len();
            (count >= shared.config.narrative_onchain_confirm_min_mints)
                .then_some((keyword, count as u32))
        })
        .collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Ok(ranked)
}

fn tokenize_keyword_text(input: &str) -> HashSet<String> {
    let mut terms = HashSet::new();
    let mut current = String::new();
    for ch in input.to_lowercase().chars() {
        if ch.is_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            terms.insert(current.clone());
            current.clear();
        }
    }
    if !current.is_empty() {
        terms.insert(current);
    }
    terms
}

async fn gate1_check(
    shared: &SharedState,
    creator_window: &mut HashMap<String, VecDeque<u64>>,
    creator_template_counts: &mut HashMap<(String, String), u32>,
    token: &NewToken,
) -> (Gate1Decision, Option<Gate1PersistArtifacts>) {
    let hotlists = shared.hotlists.read().await;
    if hotlists.creator_blacklist.contains(&token.creator) {
        return (
            Gate1Decision {
                passed: false,
                reason: "gate1 reject: creator blacklist hit".to_string(),
                risk: Gate1RiskProfile::default(),
                narrative_keywords: Vec::new(),
                dynamic_narrative_keywords: Vec::new(),
                narrative_keyword_matches: Vec::new(),
            },
            None,
        );
    }

    let now = now_ms();
    let window = creator_window.entry(token.creator.clone()).or_default();
    window.push_back(now);
    while let Some(front) = window.front().copied() {
        if now.saturating_sub(front) > FACTORY_WINDOW_MS {
            window.pop_front();
        } else {
            break;
        }
    }
    if window.len() >= FACTORY_THRESHOLD {
        return (
            Gate1Decision {
                passed: false,
                reason: format!(
                    "gate1 reject: factory creator pattern ({} launches in 5m)",
                    window.len()
                ),
                risk: Gate1RiskProfile::default(),
                narrative_keywords: Vec::new(),
                dynamic_narrative_keywords: Vec::new(),
                narrative_keyword_matches: Vec::new(),
            },
            None,
        );
    }

    if token.name.trim().is_empty() || token.symbol.trim().is_empty() {
        return (
            Gate1Decision {
                passed: false,
                reason: "gate1 reject: empty name or symbol".to_string(),
                risk: Gate1RiskProfile::default(),
                narrative_keywords: Vec::new(),
                dynamic_narrative_keywords: Vec::new(),
                narrative_keyword_matches: Vec::new(),
            },
            None,
        );
    }
    if token.symbol.chars().count() > 10 {
        return (
            Gate1Decision {
                passed: false,
                reason: format!(
                    "gate1 reject: symbol too long ({})",
                    token.symbol.chars().count()
                ),
                risk: Gate1RiskProfile::default(),
                narrative_keywords: Vec::new(),
                dynamic_narrative_keywords: Vec::new(),
                narrative_keyword_matches: Vec::new(),
            },
            None,
        );
    }

    let lower = format!(
        "{} {}",
        token.name.to_lowercase(),
        token.symbol.to_lowercase()
    );
    if let Some(keyword) = GATE1_BLACK_KEYWORDS.iter().find(|kw| lower.contains(**kw)) {
        return (
            Gate1Decision {
                passed: false,
                reason: format!("gate1 reject: blacklist keyword {}", keyword),
                risk: Gate1RiskProfile::default(),
                narrative_keywords: Vec::new(),
                dynamic_narrative_keywords: Vec::new(),
                narrative_keyword_matches: Vec::new(),
            },
            None,
        );
    }

    let (narrative_keywords, dynamic_narrative_keywords, narrative_keyword_matches) =
        collect_narrative_keywords(token, &hotlists.narrative_terms);
    drop(hotlists);

    let (uri_host, uri_pattern, template_hash) = derive_template_identity(token);
    let template_key = (token.creator.clone(), template_hash.clone());
    let template_repeat_count = creator_template_counts
        .entry(template_key)
        .and_modify(|count| *count = count.saturating_add(1))
        .or_insert(1);
    let risk = analyze_gate1_risk(
        token,
        uri_host.clone(),
        uri_pattern.clone(),
        template_hash,
        *template_repeat_count,
        shared.config.as_ref(),
    );
    let persist_artifacts = Gate1PersistArtifacts {
        creator: token.creator.clone(),
        template_hash: risk.template_hash.clone(),
        template_repeat_count: risk.template_repeat_count,
        mint: token.mint.clone(),
        updated_at_ms: now,
        uri_pattern_record: uri_pattern.map(|pattern| UriPatternRecord {
            pattern,
            label: uri_host.unwrap_or_else(|| "unknown".to_string()),
            risk_score: risk.penalty_score as i32,
            mint_count: 1,
            last_seen_ms: now,
        }),
    };
    if let Some(reason) = risk.hard_reject_reason.clone() {
        return (
            Gate1Decision {
                passed: false,
                reason,
                risk,
                narrative_keywords,
                dynamic_narrative_keywords,
                narrative_keyword_matches,
            },
            Some(persist_artifacts),
        );
    }

    (
        Gate1Decision {
            passed: true,
            reason: "gate1 pass".to_string(),
            risk,
            narrative_keywords,
            dynamic_narrative_keywords,
            narrative_keyword_matches,
        },
        Some(persist_artifacts),
    )
}

async fn creator_gate(shared: &SharedState, token: &NewToken) -> Result<CreatorGateResult> {
    let cached = shared.db.get_creator_profile(&token.creator).await?;
    if let Some(profile) = cached.as_ref() {
        if now_ms().saturating_sub(profile.fetched_at_ms) <= CREATOR_CACHE_TTL_MS {
            return apply_creator_entity_rules(
                shared,
                apply_creator_rules(shared.config.as_ref(), profile.clone()),
            )
            .await;
        }
    }

    let Some(api_key) = shared.config.helius_api_key.as_deref() else {
        return apply_creator_entity_rules(
            shared,
            CreatorGateResult {
                passed: true,
                reason: "gate2 pass: HELIUS_API_KEY not configured".to_string(),
                profile: cached,
            },
        )
        .await;
    };

    let timeout_ms = shared.config.creator_gate_timeout_ms.max(1);
    let result = match tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        creator_gate_remote(shared, token, api_key, cached.clone()),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => creator_gate_timeout_fallback(shared.config.as_ref(), cached, timeout_ms),
    };

    apply_creator_entity_rules(shared, result).await
}

async fn apply_creator_entity_rules(
    shared: &SharedState,
    mut result: CreatorGateResult,
) -> Result<CreatorGateResult> {
    if !result.passed {
        return Ok(result);
    }

    let Some(profile) = result.profile.clone() else {
        return Ok(result);
    };
    let Some(funder) = profile.first_funder.as_deref() else {
        return Ok(result);
    };

    if let Some(reason) = shared.db.get_funder_blacklist_reason(funder).await? {
        result.passed = false;
        result.reason = format!("gate2 reject: funder blacklist hit {} ({})", funder, reason);
        return Ok(result);
    }

    let Some(funder_profile) = shared.db.get_funder_profile(funder).await? else {
        return Ok(result);
    };

    if funder_profile.rug_exposure >= FUNDER_RUG_EXPOSURE_LIMIT {
        result.passed = false;
        result.reason = format!(
            "gate2 reject: funder cluster rug exposure {} for {}",
            funder_profile.rug_exposure, funder
        );
        return Ok(result);
    }

    if funder_profile.wallet_count >= FUNDER_WALLET_CLUSTER_LIMIT
        && profile.total_tokens >= shared.config.creator_fresh_wallet_token_limit
    {
        result.passed = false;
        result.reason = format!(
            "gate2 reject: funder {} funded {} wallets",
            funder, funder_profile.wallet_count
        );
        return Ok(result);
    }

    result.reason = format!(
        "{} | funder_cluster_wallets={} rug_exposure={} suspicious_cluster={}",
        result.reason,
        funder_profile.wallet_count,
        funder_profile.rug_exposure,
        funder_profile.wallet_count >= SUSPICIOUS_FUNDER_CLUSTER_WARNING_WALLETS
            || funder_profile.rug_exposure >= SUSPICIOUS_FUNDER_CLUSTER_WARNING_RUG_EXPOSURE
    );
    Ok(result)
}

async fn creator_gate_remote(
    shared: &SharedState,
    token: &NewToken,
    api_key: &str,
    cached: Option<CreatorProfile>,
) -> Result<CreatorGateResult> {
    let stale_rug_count = cached.as_ref().map(|p| p.rug_count).unwrap_or_default();
    let (mints, snapshot) = tokio::try_join!(
        fetch_creator_mints(shared, api_key, &token.creator),
        fetch_address_snapshot(shared, api_key, &token.creator)
    )?;

    let total_tokens = mints.len() as u32;
    let mut profile = CreatorProfile {
        address: token.creator.clone(),
        total_tokens,
        graduated: 0,
        rug_count: stale_rug_count,
        oldest_tx_ms: snapshot.oldest_tx_ms,
        wallet_age_days: snapshot.wallet_age_days,
        first_funder: snapshot.first_funder,
        fetched_at_ms: now_ms(),
    };

    if let Some(decision) = apply_creator_rules_without_graduated(shared.config.as_ref(), &profile)
    {
        shared.db.upsert_creator_profile(&profile).await?;
        return Ok(decision);
    }

    if needs_creator_graduated_count(shared.config.as_ref(), &profile) {
        profile.graduated = fetch_graduated_count(shared, &mints).await?;
    }

    shared.db.upsert_creator_profile(&profile).await?;
    Ok(apply_creator_rules(shared.config.as_ref(), profile))
}

fn creator_gate_timeout_fallback(
    config: &AppConfig,
    cached: Option<CreatorProfile>,
    timeout_ms: u64,
) -> CreatorGateResult {
    if let Some(profile) = cached {
        let mut result = apply_creator_rules(config, profile);
        result.reason = format!(
            "{} | gate2 cache fallback timeout={}ms",
            result.reason, timeout_ms
        );
        return result;
    }

    CreatorGateResult {
        passed: true,
        reason: format!("gate2 soft-pass: timeout {}ms", timeout_ms),
        profile: None,
    }
}

fn apply_creator_rules_without_graduated(
    config: &AppConfig,
    profile: &CreatorProfile,
) -> Option<CreatorGateResult> {
    if profile.total_tokens > CREATOR_TOTAL_TOKEN_LIMIT {
        return Some(CreatorGateResult {
            passed: false,
            reason: format!(
                "gate2 reject: creator total launches too high ({})",
                profile.total_tokens
            ),
            profile: Some(profile.clone()),
        });
    }
    if profile.rug_count >= CREATOR_RUG_LIMIT {
        return Some(CreatorGateResult {
            passed: false,
            reason: format!("gate2 reject: creator rug count {}", profile.rug_count),
            profile: Some(profile.clone()),
        });
    }
    if profile.wallet_age_days < config.creator_min_wallet_age_days as u32
        && profile.total_tokens >= config.creator_fresh_wallet_token_limit
    {
        return Some(CreatorGateResult {
            passed: false,
            reason: format!(
                "gate2 reject: fresh wallet age={}d launches={}",
                profile.wallet_age_days, profile.total_tokens
            ),
            profile: Some(profile.clone()),
        });
    }
    None
}

fn needs_creator_graduated_count(config: &AppConfig, profile: &CreatorProfile) -> bool {
    profile.total_tokens >= config.creator_fresh_wallet_token_limit
}

fn apply_creator_rules(config: &AppConfig, profile: CreatorProfile) -> CreatorGateResult {
    if profile.total_tokens > CREATOR_TOTAL_TOKEN_LIMIT {
        return CreatorGateResult {
            passed: false,
            reason: format!(
                "gate2 reject: creator total launches too high ({})",
                profile.total_tokens
            ),
            profile: Some(profile),
        };
    }
    if profile.rug_count >= CREATOR_RUG_LIMIT {
        return CreatorGateResult {
            passed: false,
            reason: format!("gate2 reject: creator rug count {}", profile.rug_count),
            profile: Some(profile),
        };
    }
    if profile.wallet_age_days < config.creator_min_wallet_age_days as u32
        && profile.total_tokens >= config.creator_fresh_wallet_token_limit
    {
        return CreatorGateResult {
            passed: false,
            reason: format!(
                "gate2 reject: fresh wallet age={}d launches={}",
                profile.wallet_age_days, profile.total_tokens
            ),
            profile: Some(profile),
        };
    }
    if profile.total_tokens >= config.creator_fresh_wallet_token_limit && profile.graduated == 0 {
        return CreatorGateResult {
            passed: false,
            reason: format!(
                "gate2 reject: launches={} but graduated=0",
                profile.total_tokens
            ),
            profile: Some(profile),
        };
    }

    CreatorGateResult {
        passed: true,
        reason: format!(
            "gate2 pass: creator={} launches={} graduated={} age_days={} first_funder={}",
            profile.address,
            profile.total_tokens,
            profile.graduated,
            profile.wallet_age_days,
            profile.first_funder.as_deref().unwrap_or("-"),
        ),
        profile: Some(profile),
    }
}

fn effective_soft_threshold(config: &AppConfig, mode: SmartMoneyMode) -> usize {
    match mode {
        SmartMoneyMode::Hotlist => config.smart_money_threshold.max(1),
        SmartMoneyMode::EarlyBuyerFallback => {
            config.smart_money_threshold.max(FALLBACK_SM_THRESHOLD)
        }
    }
}

fn effective_fast_threshold(config: &AppConfig, mode: SmartMoneyMode) -> usize {
    let soft = effective_soft_threshold(config, mode);
    config.smart_money_fast_threshold.max(1).min(soft)
}

fn effective_soft_window_ms(config: &AppConfig) -> u64 {
    let hard = max_supported_window_ms(config);
    config
        .smart_money_soft_window_ms
        .max(config.smart_money_fast_window_ms)
        .min(hard)
}

fn effective_fast_window_ms(config: &AppConfig) -> u64 {
    config
        .smart_money_fast_window_ms
        .min(effective_soft_window_ms(config))
}

fn max_supported_window_ms(config: &AppConfig) -> u64 {
    config.smart_money_window_secs.saturating_mul(1000)
}

fn effective_hard_reject_ms(config: &AppConfig) -> u64 {
    config
        .gate3_hard_reject_ms
        .max(effective_soft_window_ms(config))
        .min(max_supported_window_ms(config))
}

fn smart_money_mode_label(mode: SmartMoneyMode) -> &'static str {
    match mode {
        SmartMoneyMode::Hotlist => "address_or_funder_hotlist",
        SmartMoneyMode::EarlyBuyerFallback => "early_buyers_fallback",
    }
}

fn smart_money_mode(config: &AppConfig, hotlists: &HotLists) -> SmartMoneyMode {
    if config.disable_smart_money_filter {
        SmartMoneyMode::EarlyBuyerFallback
    } else if hotlists.smart_money.is_empty() && hotlists.smart_money_funders.is_empty() {
        SmartMoneyMode::EarlyBuyerFallback
    } else {
        SmartMoneyMode::Hotlist
    }
}

fn gate3_path_label(path: Gate3Path) -> &'static str {
    match path {
        Gate3Path::Fast => "fast",
        Gate3Path::Soft => "soft",
    }
}

async fn should_finalize(candidate: &Candidate, shared: &SharedState) -> Option<Gate3Trigger> {
    let stats = smart_money_stats(candidate, shared).await;
    if gate3_reject_reason(candidate, &stats, shared.config.as_ref()).is_some() {
        return None;
    }
    gate3_trigger_from_stats(shared.config.as_ref(), &stats)
}

fn gate3_trigger_from_stats(config: &AppConfig, stats: &WindowStats) -> Option<Gate3Trigger> {
    if gate3_fast_ready(config, stats)
        && stats
            .fast_reached_at_ms
            .is_some_and(|elapsed| elapsed <= effective_fast_window_ms(config))
    {
        return Some(Gate3Trigger {
            path: Gate3Path::Fast,
            threshold: gate3_fast_threshold(stats),
        });
    }
    if gate3_soft_ready(config, stats)
        && stats
            .soft_reached_at_ms
            .is_some_and(|elapsed| elapsed <= effective_soft_window_ms(config))
    {
        return Some(Gate3Trigger {
            path: Gate3Path::Soft,
            threshold: stats.soft_threshold,
        });
    }
    None
}

fn gate3_fast_threshold(stats: &WindowStats) -> usize {
    if gate3_quality_cluster(stats) && stats.fast_threshold > 1 {
        stats.fast_threshold - 1
    } else {
        stats.fast_threshold.max(1)
    }
}

fn gate3_fast_min_sol(config: &AppConfig, stats: &WindowStats) -> f64 {
    if gate3_quality_cluster(stats) {
        (config.gate3_fast_min_sol * 0.85).max(0.05)
    } else {
        config.gate3_fast_min_sol
    }
}

fn gate3_same_cluster_pressure(stats: &WindowStats) -> bool {
    stats.buy_count >= SAME_FUNDER_CLUSTER_PENALTY_MIN_BUYS
        && stats.largest_funder_cluster_size >= SAME_FUNDER_CLUSTER_PENALTY_MIN_BUYS
        && stats.largest_funder_cluster_share >= SAME_FUNDER_CLUSTER_PENALTY_SHARE
}

fn gate3_quality_cluster(stats: &WindowStats) -> bool {
    stats.unique_funders >= QUALITY_CLUSTER_MIN_UNIQUE_FUNDERS
        && stats.hotlist_funder_diversity >= QUALITY_CLUSTER_MIN_HOT_FUNDERS
        && stats.creator_funder_match_count == 0
        && stats.largest_funder_cluster_share <= QUALITY_CLUSTER_MAX_SHARE
}

fn gate3_fast_ready(config: &AppConfig, stats: &WindowStats) -> bool {
    !gate3_same_cluster_pressure(stats)
        && stats.unique_sm_wallets.len() >= gate3_fast_threshold(stats)
        && stats.sm_sol_total >= gate3_fast_min_sol(config, stats)
}

fn gate3_soft_ready(config: &AppConfig, stats: &WindowStats) -> bool {
    stats.unique_sm_wallets.len() >= stats.soft_threshold
        && stats.sm_sol_total >= config.gate3_soft_min_sol
}

fn gate3_reject_path(reason: &str) -> &'static str {
    if reason.contains("creator self-buy") {
        "creator_self_buy"
    } else if reason.contains("creator-linked cluster") {
        "creator_cluster"
    } else if reason.contains("early concentration") {
        "concentration"
    } else if reason.contains("hard window closed") {
        "timeout"
    } else if reason.contains("max buys reached") {
        "max_buys"
    } else {
        "insufficient"
    }
}

fn gate3_is_immediate_reject_reason(reason: &str) -> bool {
    reason.contains("creator self-buy")
        || reason.contains("creator-linked cluster")
        || reason.contains("early concentration")
        || reason.contains("max buys reached")
}

fn gate3_reject_reason(
    candidate: &Candidate,
    stats: &WindowStats,
    config: &AppConfig,
) -> Option<String> {
    let external_buyers = stats
        .eligible_buyers
        .saturating_sub(usize::from(stats.creator_buy_count > 0));
    let external_sol = (stats.total_eligible_sol - stats.creator_buy_sol).max(0.0);
    let creator_buy_share = if stats.total_eligible_sol > 0.0 {
        stats.creator_buy_sol / stats.total_eligible_sol
    } else {
        0.0
    };
    let creator_cluster_share = if external_sol > 0.0 {
        stats.creator_funder_match_sol / external_sol
    } else {
        0.0
    };
    let strong_external_support = external_buyers
        >= config.gate3_creator_self_buy_min_external_buyers
        && external_sol >= config.gate3_creator_self_buy_min_external_sol;
    if config.gate3_creator_self_buy_block
        && stats.creator_buy_count > 0
        && ((stats.creator_buy_sol > config.gate3_creator_self_buy_max_sol
            && creator_buy_share > config.gate3_creator_self_buy_hard_share)
            || (stats.creator_buy_sol > config.gate3_creator_self_buy_hard_sol
                && !strong_external_support)
            || (stats.creator_buy_sol > config.gate3_creator_self_buy_max_sol
                && creator_buy_share > config.gate3_creator_self_buy_max_share
                && !strong_external_support))
    {
        return Some(format!(
            "gate3 reject: creator self-buy detected | count={} | sol={:.2}/{:.2}/{:.2} | share={:.2}/{:.2}/{:.2} | external_buyers={}/{} | external_sol={:.2}/{:.2}",
            stats.creator_buy_count,
            stats.creator_buy_sol,
            config.gate3_creator_self_buy_max_sol,
            config.gate3_creator_self_buy_hard_sol,
            creator_buy_share,
            config.gate3_creator_self_buy_max_share,
            config.gate3_creator_self_buy_hard_share,
            external_buyers,
            config.gate3_creator_self_buy_min_external_buyers,
            external_sol,
            config.gate3_creator_self_buy_min_external_sol,
        ));
    }
    if let Some(creator_funder) = candidate
        .creator_profile
        .as_ref()
        .and_then(|profile| profile.first_funder.as_deref())
    {
        if stats.creator_funder_match_count >= CREATOR_FUNDER_CLUSTER_MIN_BUYERS
            && stats.creator_funder_match_sol >= CREATOR_FUNDER_CLUSTER_MIN_SOL
            && creator_cluster_share >= CREATOR_FUNDER_CLUSTER_MAX_SHARE
        {
            return Some(format!(
                "gate3 reject: creator-linked cluster concentration | funder={} | matched_buyers={} | matched_sol={:.2}/{:.2} | share={:.2}/{:.2}",
                creator_funder,
                stats.creator_funder_match_count,
                stats.creator_funder_match_sol,
                external_sol,
                creator_cluster_share,
                CREATOR_FUNDER_CLUSTER_MAX_SHARE,
            ));
        }
    }
    if config.gate3_early_concentration_reject
        && candidate.early_buys.len() >= config.gate3_early_concentration_min_buys
        && stats.max_single_buyer_share > config.gate3_max_single_buyer_share
    {
        return Some(format!(
            "gate3 reject: early concentration | buyer={} | share={:.2} | max_allowed={:.2} | eligible_sol={:.2} | first_buys={}",
            stats.max_single_buyer.as_deref().unwrap_or("-"),
            stats.max_single_buyer_share,
            config.gate3_max_single_buyer_share,
            stats.total_eligible_sol,
            stats.buy_count,
        ));
    }
    if stats.buy_count >= GATE3_CLUSTER_DIVERSITY_MIN_BUYS
        && stats.eligible_buyers >= GATE3_MIN_UNIQUE_FUNDERS
        && stats.unique_funders < GATE3_MIN_UNIQUE_FUNDERS
    {
        return Some(format!(
            "gate3 reject: low funder diversity | unique_buyers={} | unique_funders={} | first_buys={}",
            stats.eligible_buyers,
            stats.unique_funders,
            stats.buy_count,
        ));
    }
    if stats.elapsed_ms > effective_hard_reject_ms(config) {
        return Some(format!(
            "gate3 reject: hard window closed | mode={} | matched={} | threshold={} | sol={:.2}/{:.2} | first_buys={} | elapsed_ms={}",
            smart_money_mode_label(stats.mode),
            stats.unique_sm_wallets.len(),
            stats.soft_threshold,
            stats.sm_sol_total,
            config.gate3_soft_min_sol,
            stats.buy_count,
            stats.elapsed_ms,
        ));
    }
    if candidate.early_buys.len() >= config.smart_money_max_buys && !gate3_soft_ready(config, stats)
    {
        return Some(format!(
            "gate3 reject: max buys reached | mode={} | matched={} | threshold={} | sol={:.2}/{:.2} | first_buys={}",
            smart_money_mode_label(stats.mode),
            stats.unique_sm_wallets.len(),
            stats.soft_threshold,
            stats.sm_sol_total,
            config.gate3_soft_min_sol,
            stats.buy_count,
        ));
    }
    None
}

async fn smart_money_stats(candidate: &Candidate, shared: &SharedState) -> WindowStats {
    let hotlists = shared.hotlists.read().await;
    let mode = smart_money_mode(shared.config.as_ref(), &hotlists);
    let fast_threshold = effective_fast_threshold(&shared.config, mode);
    let soft_threshold = effective_soft_threshold(&shared.config, mode);
    let mut unique_sm_wallets = HashSet::new();
    let mut eligible_buyers = HashSet::new();
    let mut unique_funders: HashSet<String> = HashSet::new();
    let mut funder_buy_counts: HashMap<String, usize> = HashMap::new();
    let mut hotlist_funders: HashSet<String> = HashSet::new();
    let mut hotlist_funder_hits = 0usize;
    let mut eligible_sol_total = 0.0f64;
    let mut sm_sol_total = 0.0f64;
    let creator_first_funder = candidate
        .creator_profile
        .as_ref()
        .and_then(|profile| profile.first_funder.as_deref());
    let mut fastest_sm_ms: Option<u64> = None;
    let mut fast_reached_at_ms: Option<u64> = None;
    let mut soft_reached_at_ms: Option<u64> = None;
    let mut buyer_sol_totals: HashMap<String, f64> = HashMap::new();
    let mut max_single_buyer_share = 0.0f64;
    let mut max_single_buyer: Option<String> = None;
    let mut creator_buy_count = 0usize;
    let mut creator_buy_sol = 0.0f64;
    let mut creator_funder_match_count = 0usize;
    let mut creator_funder_match_sol = 0.0f64;
    let mut eligible_buy_count = 0usize;

    for buy in &candidate.early_buys {
        let buyer = buy.buyer.to_string();
        if hotlists.blocked_buyers.contains(&buyer) {
            continue;
        }
        eligible_buy_count = eligible_buy_count.saturating_add(1);
        eligible_buyers.insert(buyer.clone());
        let buyer_funder = candidate
            .buyer_profiles
            .get(&buyer)
            .and_then(|profile| profile.first_funder.clone());
        if let Some(funder) = buyer_funder.as_ref() {
            unique_funders.insert(funder.clone());
            *funder_buy_counts.entry(funder.clone()).or_default() += 1;
            if hotlists.smart_money_funders.contains(funder) {
                hotlist_funder_hits = hotlist_funder_hits.saturating_add(1);
                hotlist_funders.insert(funder.clone());
            }
        }
        let buy_sol = buy.sol_amount_lamports as f64 / 1e9;
        eligible_sol_total += buy_sol;
        let buyer_total = buyer_sol_totals.entry(buyer.clone()).or_default();
        *buyer_total += buy_sol;
        if buyer == candidate.token.creator {
            creator_buy_count += 1;
            creator_buy_sol += buy_sol;
        } else if creator_first_funder
            .zip(buyer_funder.as_deref())
            .is_some_and(|(creator_funder, buyer_funder)| creator_funder == buyer_funder)
        {
            creator_funder_match_count += 1;
            creator_funder_match_sol += buy_sol;
        }

        let matched = match mode {
            SmartMoneyMode::Hotlist => {
                buyer_matches_hotlist(&buyer, candidate.buyer_profiles.get(&buyer), &hotlists)
            }
            SmartMoneyMode::EarlyBuyerFallback => true,
        };
        if !matched {
            continue;
        }

        unique_sm_wallets.insert(buyer);
        sm_sol_total += buy_sol;
        let elapsed_ms = buy.detected_at_ms.saturating_sub(candidate.detected_at_ms);
        let current_largest_funder_cluster_size = funder_buy_counts
            .values()
            .copied()
            .max()
            .unwrap_or_default();
        let current_largest_funder_cluster_share = if eligible_buyers.is_empty() {
            0.0
        } else {
            current_largest_funder_cluster_size as f64 / eligible_buyers.len() as f64
        };
        let current_quality_cluster = unique_funders.len() >= QUALITY_CLUSTER_MIN_UNIQUE_FUNDERS
            && hotlist_funders.len() >= QUALITY_CLUSTER_MIN_HOT_FUNDERS
            && creator_funder_match_count == 0
            && current_largest_funder_cluster_share <= QUALITY_CLUSTER_MAX_SHARE;
        let current_fast_threshold = if current_quality_cluster && fast_threshold > 1 {
            fast_threshold - 1
        } else {
            fast_threshold.max(1)
        };
        let current_fast_min_sol = if current_quality_cluster {
            (shared.config.gate3_fast_min_sol * 0.85).max(0.05)
        } else {
            shared.config.gate3_fast_min_sol
        };
        let current_same_cluster_pressure = eligible_buy_count
            >= SAME_FUNDER_CLUSTER_PENALTY_MIN_BUYS
            && current_largest_funder_cluster_size >= SAME_FUNDER_CLUSTER_PENALTY_MIN_BUYS
            && current_largest_funder_cluster_share >= SAME_FUNDER_CLUSTER_PENALTY_SHARE;
        fastest_sm_ms = Some(match fastest_sm_ms {
            Some(current) => current.min(elapsed_ms),
            None => elapsed_ms,
        });
        if fast_reached_at_ms.is_none()
            && !current_same_cluster_pressure
            && unique_sm_wallets.len() >= current_fast_threshold
            && sm_sol_total >= current_fast_min_sol
        {
            fast_reached_at_ms = Some(elapsed_ms);
        }
        if soft_reached_at_ms.is_none()
            && unique_sm_wallets.len() >= soft_threshold
            && sm_sol_total >= shared.config.gate3_soft_min_sol
        {
            soft_reached_at_ms = Some(elapsed_ms);
        }
    }

    let largest_funder_cluster_size = funder_buy_counts
        .values()
        .copied()
        .max()
        .unwrap_or_default();
    let largest_funder_cluster_share = if eligible_buyers.is_empty() {
        0.0
    } else {
        largest_funder_cluster_size as f64 / eligible_buyers.len() as f64
    };

    if eligible_sol_total > 0.0 {
        for (buyer, total) in &buyer_sol_totals {
            let share = *total / eligible_sol_total;
            if share >= max_single_buyer_share {
                max_single_buyer_share = share;
                max_single_buyer = Some(buyer.clone());
            }
        }
    }

    WindowStats {
        mode,
        fast_threshold,
        soft_threshold,
        unique_sm_wallets,
        sm_sol_total,
        total_eligible_sol: eligible_sol_total,
        fastest_sm_ms,
        fast_reached_at_ms,
        soft_reached_at_ms,
        buy_count: candidate.early_buys.len(),
        eligible_buyers: eligible_buyers.len(),
        unique_funders: unique_funders.len(),
        creator_funder_match_count,
        creator_funder_match_sol,
        max_single_buyer_share,
        max_single_buyer,
        creator_buy_count,
        creator_buy_sol,
        largest_funder_cluster_size,
        largest_funder_cluster_share,
        hotlist_funder_hits,
        hotlist_funder_diversity: hotlist_funders.len(),
        elapsed_ms: candidate.created_at.elapsed().as_millis() as u64,
    }
}

fn buyer_matches_hotlist(buyer: &str, profile: Option<&BuyerProfile>, hotlists: &HotLists) -> bool {
    if hotlists.blocked_buyers.contains(buyer) {
        return false;
    }
    if hotlists.smart_money.contains(buyer) {
        return true;
    }
    profile
        .and_then(|item| item.first_funder.as_deref())
        .map(|funder| hotlists.smart_money_funders.contains(funder))
        .unwrap_or(false)
}

async fn load_creator_cluster_profile(
    shared: &SharedState,
    candidate: &Candidate,
) -> CreatorClusterProfile {
    let Some(funder) = candidate
        .creator_profile
        .as_ref()
        .and_then(|profile| profile.first_funder.clone())
    else {
        return CreatorClusterProfile::default();
    };

    let snapshot = shared
        .db
        .get_funder_profile(&funder)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    CreatorClusterProfile {
        funder: Some(funder),
        wallet_count: snapshot.wallet_count,
        rug_exposure: snapshot.rug_exposure,
    }
}

fn build_cluster_adjustment(
    stats: &WindowStats,
    creator_cluster: &CreatorClusterProfile,
    path: Gate3Path,
) -> ClusterAdjustment {
    let suspicious_funder_penalty = if creator_cluster.wallet_count
        >= SUSPICIOUS_FUNDER_CLUSTER_WARNING_WALLETS
        || creator_cluster.rug_exposure >= SUSPICIOUS_FUNDER_CLUSTER_WARNING_RUG_EXPOSURE
    {
        SUSPICIOUS_FUNDER_CLUSTER_PENALTY_SCORE
    } else {
        0
    };
    let same_cluster_first_buy_penalty = if stats.buy_count >= SAME_FUNDER_CLUSTER_PENALTY_MIN_BUYS
        && stats.largest_funder_cluster_size >= SAME_FUNDER_CLUSTER_PENALTY_MIN_BUYS
        && stats.largest_funder_cluster_share >= SAME_FUNDER_CLUSTER_PENALTY_SHARE
    {
        SAME_FUNDER_CLUSTER_PENALTY_SCORE
    } else {
        0
    };
    let quality_cluster = stats.unique_funders >= QUALITY_CLUSTER_MIN_UNIQUE_FUNDERS
        && stats.hotlist_funder_diversity >= QUALITY_CLUSTER_MIN_HOT_FUNDERS
        && stats.creator_funder_match_count == 0
        && stats.largest_funder_cluster_share <= QUALITY_CLUSTER_MAX_SHARE;
    let quality_cluster_bonus = if quality_cluster {
        QUALITY_CLUSTER_BONUS_SCORE
    } else {
        0
    };
    let fast_required_score_relief = if quality_cluster && path == Gate3Path::Fast {
        FAST_QUALITY_CLUSTER_SCORE_RELIEF
    } else {
        0
    };

    ClusterAdjustment {
        creator_funder: creator_cluster.funder.clone(),
        creator_cluster_wallet_count: creator_cluster.wallet_count,
        creator_cluster_rug_exposure: creator_cluster.rug_exposure,
        suspicious_funder_penalty,
        same_cluster_first_buy_penalty,
        quality_cluster_bonus,
        fast_required_score_relief,
        largest_funder_cluster_size: stats.largest_funder_cluster_size,
        largest_funder_cluster_share: stats.largest_funder_cluster_share,
        hotlist_funder_hits: stats.hotlist_funder_hits,
        hotlist_funder_diversity: stats.hotlist_funder_diversity,
        quality_cluster,
    }
}

fn build_narrative_adjustment(
    candidate: &Candidate,
    shared: &SharedState,
    path: Gate3Path,
) -> NarrativeAdjustment {
    let mut adjustment = NarrativeAdjustment::default();
    for term in &candidate.narrative_keyword_matches {
        match term.tier {
            NarrativeTier::Preheat => adjustment.preheat_hits += 1,
            NarrativeTier::Base => adjustment.base_hits += 1,
            NarrativeTier::Confirmed => adjustment.confirmed_hits += 1,
        }
    }
    adjustment.preheat_bonus = (adjustment.preheat_hits as u32)
        .saturating_mul(shared.config.narrative_preheat_bonus_per_hit);
    adjustment.base_bonus =
        (adjustment.base_hits as u32).saturating_mul(shared.config.narrative_base_bonus_per_hit);
    adjustment.confirmed_bonus = (adjustment.confirmed_hits as u32)
        .saturating_mul(shared.config.narrative_confirmed_bonus_per_hit);
    let mut overflow = adjustment
        .preheat_bonus
        .saturating_add(adjustment.base_bonus)
        .saturating_add(adjustment.confirmed_bonus)
        .saturating_sub(shared.config.dynamic_narrative_bonus_cap);
    if overflow > 0 {
        let cut_confirmed = adjustment.confirmed_bonus.min(overflow);
        adjustment.confirmed_bonus = adjustment.confirmed_bonus.saturating_sub(cut_confirmed);
        overflow = overflow.saturating_sub(cut_confirmed);
    }
    if overflow > 0 {
        let cut_base = adjustment.base_bonus.min(overflow);
        adjustment.base_bonus = adjustment.base_bonus.saturating_sub(cut_base);
        overflow = overflow.saturating_sub(cut_base);
    }
    if overflow > 0 {
        adjustment.preheat_bonus = adjustment.preheat_bonus.saturating_sub(overflow);
    }
    if path == Gate3Path::Fast && adjustment.confirmed_hits > 0 {
        adjustment.fast_required_score_relief = shared.config.narrative_confirmed_fast_score_relief;
    }
    adjustment
}

async fn build_scoring_context(
    shared: &SharedState,
    candidate: &Candidate,
    stats: &WindowStats,
    path: Gate3Path,
) -> ScoringContext {
    let participants_score = match stats.unique_sm_wallets.len() {
        0 => 0,
        1 => 10,
        2 => 20,
        _ => 30,
    };
    let capital_score = if stats.sm_sol_total >= 2.0 {
        20
    } else if stats.sm_sol_total >= 0.5 {
        10
    } else {
        0
    };
    let momentum_score = if stats.buy_count >= 15 {
        20
    } else if stats.buy_count >= 8 {
        13
    } else if stats.buy_count >= 4 {
        6
    } else {
        0
    };
    let curve_progress_pct = fetch_curve_progress_pct(shared, &candidate.token)
        .await
        .unwrap_or(0.0);
    let curve_score = if curve_progress_pct > 5.0 {
        15
    } else if curve_progress_pct > 2.0 {
        10
    } else if curve_progress_pct > 0.5 {
        5
    } else {
        0
    };
    let buyer_quality_pct = fetch_buyer_quality_pct(shared, candidate)
        .await
        .unwrap_or(0.0);
    let buyer_quality_score = (buyer_quality_pct * 15.0).round().clamp(0.0, 15.0) as u32;
    let narrative = build_narrative_adjustment(candidate, shared, path);
    let dynamic_narrative_bonus = narrative
        .preheat_bonus
        .saturating_add(narrative.base_bonus)
        .saturating_add(narrative.confirmed_bonus)
        .min(shared.config.dynamic_narrative_bonus_cap);
    let funder_diversity_penalty = if stats.eligible_buyers >= GATE3_MIN_UNIQUE_FUNDERS
        && stats.unique_funders < GATE3_MIN_UNIQUE_FUNDERS
    {
        SINGLE_FUNDER_SCORE_PENALTY
    } else {
        0
    };
    let runtime_risk = analyze_runtime_risk(
        &candidate.gate1_risk,
        RuntimeRiskInput {
            eligible_buyers: stats.eligible_buyers,
            unique_funders: stats.unique_funders,
            total_eligible_sol: stats.total_eligible_sol,
            creator_funder_match_count: stats.creator_funder_match_count,
            creator_funder_match_sol: stats.creator_funder_match_sol,
            max_single_buyer_share: stats.max_single_buyer_share,
        },
        shared.config.as_ref(),
    );
    let creator_cluster = load_creator_cluster_profile(shared, candidate).await;
    let cluster = build_cluster_adjustment(stats, &creator_cluster, path);
    let base_quality_score = participants_score
        + capital_score
        + curve_score
        + buyer_quality_score
        + cluster.quality_cluster_bonus;
    let total_quality_penalty = funder_diversity_penalty
        .saturating_add(runtime_risk.penalty_score)
        .saturating_add(cluster.suspicious_funder_penalty)
        .saturating_add(cluster.same_cluster_first_buy_penalty)
        .min(base_quality_score);
    let quality_score = base_quality_score.saturating_sub(total_quality_penalty);
    let urgency_score = momentum_score + dynamic_narrative_bonus;
    let total_score = quality_score + urgency_score;
    let required_score = match path {
        Gate3Path::Fast => shared
            .config
            .filter_fast_min_score
            .min(shared.config.filter_min_score)
            .saturating_sub(cluster.fast_required_score_relief)
            .saturating_sub(narrative.fast_required_score_relief),
        Gate3Path::Soft => shared
            .config
            .filter_soft_min_score
            .min(shared.config.filter_min_score),
    };
    let execution_confidence = quality_score.saturating_add(urgency_score).min(100);

    ScoringContext {
        participants_score,
        capital_score,
        momentum_score,
        curve_score,
        buyer_quality_score,
        dynamic_narrative_bonus,
        narrative,
        funder_diversity_penalty,
        runtime_risk,
        cluster,
        curve_progress_pct,
        buyer_quality_pct,
        quality_score,
        urgency_score,
        execution_confidence,
        total_score,
        required_score,
    }
}

async fn score_candidate(shared: &SharedState, mut candidate: Candidate) -> ScoreDecision {
    let stats = smart_money_stats(&candidate, shared).await;
    let Some(trigger) = gate3_trigger_from_stats(shared.config.as_ref(), &stats) else {
        return ScoreDecision {
            passed: false,
            gate: "gate3".to_string(),
            score: 0,
            quality_score: 0,
            urgency_score: 0,
            execution_confidence: 0,
            reason: format!(
                "gate3 reject: below threshold | mode={} | matched={} | fast_threshold={} | soft_threshold={} | sol={:.2} | fast_sol={:.2} | soft_sol={:.2} | first_buys={}",
                smart_money_mode_label(stats.mode),
                stats.unique_sm_wallets.len(),
                stats.fast_threshold,
                stats.soft_threshold,
                stats.sm_sol_total,
                shared.config.gate3_fast_min_sol,
                shared.config.gate3_soft_min_sol,
                stats.buy_count
            ),
            signal: None,
            mode: smart_money_mode_label(stats.mode).to_string(),
            path: "insufficient".to_string(),
            matched_buyers: stats.unique_sm_wallets.len(),
            early_buy_count: stats.buy_count,
            gate4_at_ms: None,
        };
    };

    candidate.trace.gate4_at_ms = Some(now_ms());
    if candidate.trace.gate3_trigger_at_ms.is_none() {
        candidate.trace.gate3_trigger_at_ms = Some(now_ms());
    }
    candidate.trace.path = Some(gate3_path_label(trigger.path).to_string());

    let scoring = build_scoring_context(shared, &candidate, &stats, trigger.path).await;
    let reason = format!(
        "mode={} path={} participants={} capital={} momentum={} curve={} buyer_quality={} narrative_bonus={} preheat_hits={} base_hits={} confirmed_hits={} risk_penalty={} suspicious_cluster_penalty={} same_cluster_penalty={} cluster_bonus={} fast_relief={} narrative_fast_relief={} total={} required={} | matched={} eligible={} unique_funders={} sol={:.2} fastest={}ms largest_cluster={}/{}({:.2}) hot_funders={} narrative={}",
        smart_money_mode_label(stats.mode),
        gate3_path_label(trigger.path),
        scoring.participants_score,
        scoring.capital_score,
        scoring.momentum_score,
        scoring.curve_score,
        scoring.buyer_quality_score,
        scoring.dynamic_narrative_bonus,
        scoring.narrative.preheat_hits,
        scoring.narrative.base_hits,
        scoring.narrative.confirmed_hits,
        scoring.runtime_risk.penalty_score,
        scoring.cluster.suspicious_funder_penalty,
        scoring.cluster.same_cluster_first_buy_penalty,
        scoring.cluster.quality_cluster_bonus,
        scoring.cluster.fast_required_score_relief,
        scoring.narrative.fast_required_score_relief,
        scoring.total_score,
        scoring.required_score,
        stats.unique_sm_wallets.len(),
        stats.eligible_buyers,
        stats.unique_funders,
        stats.sm_sol_total,
        stats.fastest_sm_ms.unwrap_or_default(),
        scoring.cluster.largest_funder_cluster_size,
        stats.eligible_buyers.max(1),
        scoring.cluster.largest_funder_cluster_share,
        scoring.cluster.hotlist_funder_diversity,
        if candidate.narrative_keywords.is_empty() {
            "-".to_string()
        } else {
            candidate.narrative_keywords.join("|")
        }
    );

    if scoring.total_score < scoring.required_score {
        return ScoreDecision {
            passed: false,
            gate: "gate4".to_string(),
            score: scoring.total_score,
            quality_score: scoring.quality_score,
            urgency_score: scoring.urgency_score,
            execution_confidence: scoring.execution_confidence,
            reason,
            signal: None,
            mode: smart_money_mode_label(stats.mode).to_string(),
            path: gate3_path_label(trigger.path).to_string(),
            matched_buyers: stats.unique_sm_wallets.len(),
            early_buy_count: stats.buy_count,
            gate4_at_ms: candidate.trace.gate4_at_ms,
        };
    }

    let Some(trigger_trade) = select_trigger_trade(&candidate, shared).await else {
        return ScoreDecision {
            passed: false,
            gate: "gate4".to_string(),
            score: scoring.total_score,
            quality_score: scoring.quality_score,
            urgency_score: scoring.urgency_score,
            execution_confidence: scoring.execution_confidence,
            reason: format!("{} | missing trigger buy context", reason),
            signal: None,
            mode: smart_money_mode_label(stats.mode).to_string(),
            path: gate3_path_label(trigger.path).to_string(),
            matched_buyers: stats.unique_sm_wallets.len(),
            early_buy_count: stats.buy_count,
            gate4_at_ms: candidate.trace.gate4_at_ms,
        };
    };

    let latency_ms = now_ms().saturating_sub(candidate.detected_at_ms);
    ScoreDecision {
        passed: true,
        gate: "pass".to_string(),
        score: scoring.total_score,
        quality_score: scoring.quality_score,
        urgency_score: scoring.urgency_score,
        execution_confidence: scoring.execution_confidence,
        reason: reason.clone(),
        signal: Some(BuySignal {
            token: candidate.token,
            score: scoring.total_score,
            quality_score: scoring.quality_score,
            urgency_score: scoring.urgency_score,
            execution_confidence: scoring.execution_confidence,
            path: gate3_path_label(trigger.path).to_string(),
            reason,
            sm_count: stats.unique_sm_wallets.len(),
            sm_sol_total: stats.sm_sol_total,
            latency_ms,
            trigger_trade,
        }),
        mode: smart_money_mode_label(stats.mode).to_string(),
        path: gate3_path_label(trigger.path).to_string(),
        matched_buyers: stats.unique_sm_wallets.len(),
        early_buy_count: stats.buy_count,
        gate4_at_ms: candidate.trace.gate4_at_ms,
    }
}

async fn select_trigger_trade(candidate: &Candidate, shared: &SharedState) -> Option<PumpBuyEvent> {
    let hotlists = shared.hotlists.read().await;
    let hotlist_mode =
        smart_money_mode(shared.config.as_ref(), &hotlists) == SmartMoneyMode::Hotlist;
    if !hotlist_mode {
        return candidate
            .early_buys
            .iter()
            .find(|buy| buy_is_trigger_eligible(shared.config.as_ref(), candidate, buy, &hotlists))
            .cloned();
    }

    candidate
        .early_buys
        .iter()
        .find(|buy| {
            let buyer = buy.buyer.to_string();
            buy_is_trigger_eligible(shared.config.as_ref(), candidate, buy, &hotlists)
                && buyer_matches_hotlist(&buyer, candidate.buyer_profiles.get(&buyer), &hotlists)
        })
        .cloned()
}

fn buy_is_trigger_eligible(
    config: &AppConfig,
    candidate: &Candidate,
    buy: &PumpBuyEvent,
    hotlists: &HotLists,
) -> bool {
    let buyer = buy.buyer.to_string();
    if hotlists.blocked_buyers.contains(&buyer) {
        return false;
    }
    if config.gate3_creator_self_buy_block && buyer == candidate.token.creator {
        return false;
    }
    true
}

async fn fetch_creator_mints(
    shared: &SharedState,
    api_key: &str,
    creator: &str,
) -> Result<Vec<String>> {
    let mut mints = HashSet::new();
    let mut before_signature: Option<String> = None;

    for _ in 0..HELIUS_MAX_PAGES {
        let url = format!(
            "https://api-mainnet.helius-rpc.com/v0/addresses/{}/transactions",
            creator
        );
        let mut request = shared.http.get(url).query(&[
            ("api-key", api_key),
            ("commitment", "confirmed"),
            ("limit", "100"),
            ("sort-order", "desc"),
        ]);
        if let Some(before) = before_signature.as_deref() {
            request = request.query(&[("before-signature", before)]);
        }
        let items: Vec<Value> = request
            .send()
            .await
            .with_context(|| format!("Helius creator query failed: {}", creator))?
            .error_for_status()
            .with_context(|| format!("Helius creator response invalid: {}", creator))?
            .json()
            .await
            .context("Helius creator json decode failed")?;
        if items.is_empty() {
            break;
        }
        for item in &items {
            for mint in extract_pump_create_mints(item) {
                mints.insert(mint);
            }
        }
        before_signature = items
            .last()
            .and_then(|item| item.get("signature"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        if before_signature.is_none()
            || items.len() < HELIUS_PAGE_LIMIT
            || mints.len() > CREATOR_TOTAL_TOKEN_LIMIT as usize
        {
            break;
        }
    }

    Ok(mints.into_iter().collect())
}

fn extract_pump_create_mints(item: &Value) -> Vec<String> {
    let mut mints = Vec::new();
    let Some(instructions) = item.get("instructions").and_then(Value::as_array) else {
        return mints;
    };
    for instruction in instructions {
        if instruction.get("programId").and_then(Value::as_str) != Some(PUMP_PROGRAM_ID) {
            continue;
        }
        let Some(data) = instruction.get("data").and_then(Value::as_str) else {
            continue;
        };
        let Ok(decoded) = bs58::decode(data).into_vec() else {
            continue;
        };
        if decoded.len() < 8 {
            continue;
        }
        let Ok(disc) = <[u8; 8]>::try_from(&decoded[..8]) else {
            continue;
        };
        if disc != DISC_CREATE && disc != DISC_CREATE_V2 {
            continue;
        }
        let Some(accounts) = instruction.get("accounts").and_then(Value::as_array) else {
            continue;
        };
        if let Some(mint) = accounts.first().and_then(Value::as_str) {
            mints.push(mint.to_string());
        }
    }
    mints
}

async fn fetch_graduated_count(shared: &SharedState, mints: &[String]) -> Result<u32> {
    if mints.is_empty() {
        return Ok(0);
    }
    let pump_program = Pubkey::from_str(PUMP_PROGRAM_ID)?;
    let curves: Vec<Pubkey> = mints
        .iter()
        .filter_map(|mint| Pubkey::from_str(mint).ok())
        .map(|mint| {
            Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &pump_program).0
        })
        .collect();
    let rpc = shared.rpc_client.clone();
    tokio::task::spawn_blocking(move || {
        let mut graduated = 0u32;
        for chunk in curves.chunks(100) {
            let accounts = rpc
                .get_multiple_accounts_with_commitment(chunk, CommitmentConfig::confirmed())?
                .value;
            for account in accounts {
                match account {
                    Some(account) if account.owner != pump_program => graduated += 1,
                    Some(account) => {
                        if let Ok(state) = BondingCurveState::from_account_data(&account.data) {
                            if state.complete {
                                graduated += 1;
                            }
                        }
                    }
                    None => {}
                }
            }
        }
        Ok::<u32, anyhow::Error>(graduated)
    })
    .await
    .context("creator graduated count task failed")?
}

async fn fetch_curve_progress_pct(shared: &SharedState, token: &NewToken) -> Result<f64> {
    let bonding_curve = Pubkey::from_str(&token.bonding_curve)?;
    let rpc = shared.rpc_client.clone();
    tokio::task::spawn_blocking(move || {
        let account =
            rpc.get_account_with_commitment(&bonding_curve, CommitmentConfig::confirmed())?;
        let Some(account) = account.value else {
            anyhow::bail!("bonding curve missing");
        };
        let state = BondingCurveState::from_account_data(&account.data)?;
        let progressed = state
            .virtual_sol_reserves
            .saturating_sub(CURVE_INITIAL_VIRTUAL_SOL);
        Ok::<f64, anyhow::Error>(
            ((progressed as f64 / 1e9) / CURVE_TOTAL_TARGET_SOL * 100.0).clamp(0.0, 100.0),
        )
    })
    .await
    .context("curve progress task failed")?
}

async fn fetch_buyer_quality_pct(shared: &SharedState, candidate: &Candidate) -> Result<f64> {
    let mut unique_buyers = Vec::new();
    let mut seen = HashSet::new();
    for buy in &candidate.early_buys {
        let address = buy.buyer.to_string();
        if seen.insert(address.clone()) {
            unique_buyers.push(address);
        }
        if unique_buyers.len() >= shared.config.smart_money_max_buys {
            break;
        }
    }
    if unique_buyers.is_empty() {
        return Ok(0.0);
    }
    let unique_buyer_count = unique_buyers.len();
    let cutoff = now_ms().saturating_sub(OLD_WALLET_DAYS * DAY_MS);
    let api_key = shared.config.helius_api_key.clone();
    let shared_clone = shared.clone();
    let known_profiles = candidate.buyer_profiles.clone();
    let old_count = stream::iter(unique_buyers)
        .map(|address| {
            let shared = shared_clone.clone();
            let api_key = api_key.clone();
            let known_profiles = known_profiles.clone();
            async move {
                if let Some(profile) = known_profiles.get(&address) {
                    return usize::from(profile.oldest_tx_ms > 0 && profile.oldest_tx_ms <= cutoff);
                }
                if let Some(api_key) = api_key.as_deref() {
                    return usize::from(
                        wallet_is_old(&shared, api_key, &address, cutoff)
                            .await
                            .unwrap_or(false),
                    );
                }
                0usize
            }
        })
        .buffer_unordered(8)
        .fold(0usize, |acc, count| async move { acc + count })
        .await;

    Ok(old_count as f64 / unique_buyer_count.max(1) as f64)
}

async fn wallet_is_old(
    shared: &SharedState,
    api_key: &str,
    address: &str,
    cutoff_ms: u64,
) -> Result<bool> {
    let profile = fetch_address_snapshot(shared, api_key, address).await?;
    Ok(profile.oldest_tx_ms > 0 && profile.oldest_tx_ms <= cutoff_ms)
}

async fn fetch_buyer_profile(
    shared: &SharedState,
    api_key: &str,
    address: &str,
) -> Result<BuyerProfile> {
    let cached = shared.db.get_buyer_profile(address).await?;
    if let Some(profile) = cached.as_ref() {
        if now_ms().saturating_sub(profile.fetched_at_ms) <= BUYER_CACHE_TTL_MS {
            return Ok(profile.clone());
        }
    }

    let snapshot = fetch_address_snapshot(shared, api_key, address).await?;
    let profile = BuyerProfile {
        address: address.to_string(),
        oldest_tx_ms: snapshot.oldest_tx_ms,
        wallet_age_days: snapshot.wallet_age_days,
        first_funder: snapshot.first_funder,
        fetched_at_ms: now_ms(),
    };
    shared.db.upsert_buyer_profile(&profile).await?;
    Ok(profile)
}

async fn fetch_address_snapshot(
    shared: &SharedState,
    api_key: &str,
    address: &str,
) -> Result<AddressSnapshot> {
    if let Some(snapshot) =
        get_cached_address_snapshot(shared, address, ADDRESS_SNAPSHOT_CACHE_TTL_MS).await
    {
        return Ok(snapshot);
    }

    if let Some(snapshot) =
        get_persisted_address_snapshot(shared, address, ADDRESS_SNAPSHOT_CACHE_TTL_MS).await
    {
        return Ok(snapshot);
    }

    let stale_snapshot = match get_cached_address_snapshot(shared, address, u64::MAX).await {
        Some(snapshot) => Some(snapshot),
        None => get_persisted_address_snapshot(shared, address, u64::MAX).await,
    };

    if let Some(mut snapshot) = stale_snapshot {
        snapshot.source = format!("stale:{}", snapshot.source);
        maybe_spawn_address_snapshot_refresh(shared, address.to_string(), api_key.to_string())
            .await;
        warn!(
            "address snapshot stale-while-revalidate | address={} | source={}",
            address, snapshot.source
        );
        return Ok(snapshot);
    }

    fetch_address_snapshot_blocking(shared, api_key, address).await
}

async fn fetch_address_snapshot_live(
    shared: &SharedState,
    api_key: &str,
    address: &str,
) -> Result<AddressSnapshot> {
    let helius_cooldown_remaining_ms = helius_snapshot_cooldown_remaining_ms(shared).await;
    if !api_key.trim().is_empty() && helius_cooldown_remaining_ms == 0 {
        match fetch_address_snapshot_helius(shared, api_key, address).await {
            Ok(snapshot) => return Ok(snapshot),
            Err(helius_err) => {
                warn!(
                    "address snapshot helius fallback | address={} | {}",
                    address, helius_err
                );
            }
        }
    } else if !api_key.trim().is_empty() && helius_cooldown_remaining_ms > 0 {
        warn!(
            "address snapshot helius cooldown active | address={} | cooldown_remaining_ms={}",
            address, helius_cooldown_remaining_ms
        );
    }

    let mut snapshot = fetch_address_snapshot_rpc(shared, address).await?;
    if !api_key.trim().is_empty() && helius_cooldown_remaining_ms > 0 {
        snapshot.source = format!("rpc:helius_cooldown:{}ms", helius_cooldown_remaining_ms);
    } else if !api_key.trim().is_empty() {
        snapshot.source = "rpc:helius_fallback".to_string();
    }
    warn!(
        "address snapshot rpc fallback | address={} | source={}",
        address, snapshot.source
    );
    Ok(snapshot)
}

async fn maybe_spawn_address_snapshot_refresh(
    shared: &SharedState,
    address: String,
    api_key: String,
) {
    {
        let mut inflight = shared.address_snapshot_refreshes.write().await;
        if !inflight.insert(address.clone()) {
            return;
        }
    }

    let shared = shared.clone();
    tokio::spawn(async move {
        let refresh_result = fetch_address_snapshot_live(&shared, &api_key, &address).await;
        match refresh_result {
            Ok(snapshot) => {
                let source = snapshot.source.clone();
                cache_address_snapshot(&shared, &address, snapshot, &source).await;
                info!(
                    "address snapshot refresh complete | address={} | source={}",
                    address, source
                );
            }
            Err(err) => {
                warn!(
                    "address snapshot refresh failed | address={} | {}",
                    address, err
                );
            }
        }

        let mut inflight = shared.address_snapshot_refreshes.write().await;
        inflight.remove(&address);
    });
}

async fn fetch_address_snapshot_blocking(
    shared: &SharedState,
    api_key: &str,
    address: &str,
) -> Result<AddressSnapshot> {
    let snapshot = fetch_address_snapshot_live(shared, api_key, address).await?;
    let source = snapshot.source.clone();
    cache_address_snapshot(shared, address, snapshot.clone(), &source).await;
    Ok(snapshot)
}

async fn fetch_address_snapshot_helius(
    shared: &SharedState,
    api_key: &str,
    address: &str,
) -> Result<AddressSnapshot> {
    let url = format!(
        "https://api-mainnet.helius-rpc.com/v0/addresses/{}/transactions",
        address
    );
    let retry_limit = shared.config.address_snapshot_helius_retry_limit.max(1);
    let retry_delay_ms = shared.config.address_snapshot_helius_retry_delay_ms.max(50);
    let mut last_error = None;

    for attempt in 0..retry_limit {
        let response = shared
            .http
            .get(url.clone())
            .query(&[
                ("api-key", api_key),
                ("commitment", "confirmed"),
                ("limit", "1"),
                ("sort-order", "asc"),
            ])
            .send()
            .await
            .with_context(|| format!("Helius oldest tx query failed: {}", address))?;
        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS {
            let cooldown_ms = retry_after_to_ms(response.headers()).unwrap_or(
                shared
                    .config
                    .address_snapshot_provider_cooldown_ms
                    .max(1_000),
            );
            set_helius_snapshot_cooldown(shared, cooldown_ms).await;
            anyhow::bail!(
                "Helius oldest tx rate limited: {} | cooldown_ms={}",
                address,
                cooldown_ms
            );
        }
        if status.is_server_error() && attempt + 1 < retry_limit {
            last_error = Some(anyhow::anyhow!(
                "Helius oldest tx server error: {} | status={}",
                address,
                status
            ));
            tokio::time::sleep(Duration::from_millis(retry_delay_ms)).await;
            continue;
        }
        let items: Vec<Value> = response
            .error_for_status()
            .with_context(|| format!("Helius oldest tx response invalid: {}", address))?
            .json()
            .await
            .context("Helius oldest tx json decode failed")?;

        clear_helius_snapshot_cooldown(shared).await;

        let oldest_tx_ms = items
            .first()
            .and_then(|item| item.get("timestamp"))
            .and_then(Value::as_u64)
            .map(|ts| ts * 1000)
            .unwrap_or_default();
        let wallet_age_days = if oldest_tx_ms == 0 {
            0
        } else {
            now_ms()
                .saturating_sub(oldest_tx_ms)
                .checked_div(DAY_MS)
                .unwrap_or_default() as u32
        };
        let first_funder = items
            .first()
            .and_then(|item| extract_first_funder(item, address));

        return Ok(AddressSnapshot {
            oldest_tx_ms,
            wallet_age_days,
            first_funder,
            source: "helius".to_string(),
        });
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!(
            "Helius oldest tx failed without detailed error: {}",
            address
        )
    }))
}

async fn fetch_address_snapshot_rpc(
    shared: &SharedState,
    address: &str,
) -> Result<AddressSnapshot> {
    let address = Pubkey::from_str(address).context("invalid snapshot address")?;
    let rpc = shared.rpc_client.clone();
    tokio::task::spawn_blocking(move || {
        let signatures = rpc.get_signatures_for_address_with_config(
            &address,
            GetConfirmedSignaturesForAddress2Config {
                before: None,
                until: None,
                limit: Some(1_000),
                commitment: Some(CommitmentConfig::confirmed()),
            },
        )?;
        let oldest_tx_ms = signatures
            .last()
            .and_then(|entry| entry.block_time)
            .map(|ts| (ts as u64).saturating_mul(1000))
            .unwrap_or_default();
        let wallet_age_days = if oldest_tx_ms == 0 {
            0
        } else {
            now_ms()
                .saturating_sub(oldest_tx_ms)
                .checked_div(DAY_MS)
                .unwrap_or_default() as u32
        };
        Ok::<AddressSnapshot, anyhow::Error>(AddressSnapshot {
            oldest_tx_ms,
            wallet_age_days,
            first_funder: None,
            source: "rpc".to_string(),
        })
    })
    .await
    .context("address snapshot rpc task failed")?
}

async fn get_cached_address_snapshot(
    shared: &SharedState,
    address: &str,
    max_age_ms: u64,
) -> Option<AddressSnapshot> {
    let cache = shared.address_snapshot_cache.read().await;
    let entry = cache.get(address)?;
    if now_ms().saturating_sub(entry.fetched_at_ms) > max_age_ms {
        return None;
    }
    Some(entry.snapshot.clone())
}

async fn get_persisted_address_snapshot(
    shared: &SharedState,
    address: &str,
    max_age_ms: u64,
) -> Option<AddressSnapshot> {
    let record = shared
        .db
        .get_address_snapshot(address)
        .await
        .ok()
        .flatten()?;
    if now_ms().saturating_sub(record.fetched_at_ms) > max_age_ms {
        return None;
    }

    let snapshot = AddressSnapshot {
        oldest_tx_ms: record.oldest_tx_ms,
        wallet_age_days: record.wallet_age_days,
        first_funder: record.first_funder,
        source: format!("sqlite:{}", record.source),
    };
    cache_address_snapshot_memory(shared, address, snapshot.clone(), record.fetched_at_ms).await;
    Some(snapshot)
}

async fn cache_address_snapshot(
    shared: &SharedState,
    address: &str,
    snapshot: AddressSnapshot,
    source: &str,
) {
    let fetched_at_ms = now_ms();
    cache_address_snapshot_memory(shared, address, snapshot.clone(), fetched_at_ms).await;
    if let Err(err) = shared
        .db
        .upsert_address_snapshot(&AddressSnapshotRecord {
            address: address.to_string(),
            oldest_tx_ms: snapshot.oldest_tx_ms,
            wallet_age_days: snapshot.wallet_age_days,
            first_funder: snapshot.first_funder.clone(),
            source: source.to_string(),
            fetched_at_ms,
        })
        .await
    {
        warn!(
            "address snapshot persist failed | address={} | {}",
            address, err
        );
    }
}

async fn cache_address_snapshot_memory(
    shared: &SharedState,
    address: &str,
    snapshot: AddressSnapshot,
    fetched_at_ms: u64,
) {
    let mut cache = shared.address_snapshot_cache.write().await;
    cache.insert(
        address.to_string(),
        CachedAddressSnapshot {
            snapshot,
            fetched_at_ms,
        },
    );
}

async fn helius_snapshot_cooldown_remaining_ms(shared: &SharedState) -> u64 {
    let guard = shared
        .address_snapshot_helius_cooldown_until_ms
        .read()
        .await;
    guard.saturating_sub(now_ms())
}

async fn set_helius_snapshot_cooldown(shared: &SharedState, cooldown_ms: u64) {
    let until_ms = now_ms().saturating_add(cooldown_ms);
    let mut guard = shared
        .address_snapshot_helius_cooldown_until_ms
        .write()
        .await;
    *guard = (*guard).max(until_ms);
}

async fn clear_helius_snapshot_cooldown(shared: &SharedState) {
    let mut guard = shared
        .address_snapshot_helius_cooldown_until_ms
        .write()
        .await;
    *guard = 0;
}

fn retry_after_to_ms(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let value = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    value
        .parse::<u64>()
        .ok()
        .map(|seconds| seconds.saturating_mul(1000))
}

fn extract_first_funder(item: &Value, address: &str) -> Option<String> {
    item.get("nativeTransfers")
        .and_then(Value::as_array)
        .and_then(|transfers| {
            transfers.iter().find_map(|transfer| {
                let to = transfer.get("toUserAccount").and_then(Value::as_str)?;
                let from = transfer.get("fromUserAccount").and_then(Value::as_str)?;
                if to == address && !from.is_empty() {
                    Some(from.to_string())
                } else {
                    None
                }
            })
        })
        .or_else(|| {
            item.get("tokenTransfers")
                .and_then(Value::as_array)
                .and_then(|transfers| {
                    transfers.iter().find_map(|transfer| {
                        let to = transfer
                            .get("toUserAccount")
                            .or_else(|| transfer.get("toTokenAccount"))
                            .and_then(Value::as_str)?;
                        let from = transfer
                            .get("fromUserAccount")
                            .or_else(|| transfer.get("fromTokenAccount"))
                            .and_then(Value::as_str)?;
                        if to == address && !from.is_empty() {
                            Some(from.to_string())
                        } else {
                            None
                        }
                    })
                })
        })
}

async fn refresh_dynamic_hot_keywords(shared: &SharedState) -> Result<()> {
    let now = now_ms();
    let market_keywords = match fetch_dynamic_hot_keywords(shared).await {
        Ok(keywords) => keywords,
        Err(err) => {
            warn!("dynamic keywords: base market refresh failed: {}", err);
            Vec::new()
        }
    };
    let social_keywords = normalize_seed_keywords(
        load_plaintext_set(&shared.config.narrative_social_keywords_file).await?,
    );
    let telegram_keywords = normalize_seed_keywords(
        load_plaintext_set(&shared.config.narrative_telegram_keywords_file).await?,
    );
    let event_keywords = normalize_seed_keywords(
        load_plaintext_set(&shared.config.narrative_event_keywords_file).await?,
    );
    let confirmed_keywords = match fetch_onchain_confirmed_keywords(shared).await {
        Ok(keywords) => keywords,
        Err(err) => {
            warn!(
                "dynamic keywords: onchain confirmation refresh failed: {}",
                err
            );
            Vec::new()
        }
    };

    let source_records = vec![
        (
            NARRATIVE_SOURCE_BASE_MARKET,
            build_ranked_keyword_records(
                NARRATIVE_SOURCE_BASE_MARKET,
                market_keywords,
                now,
                shared.config.narrative_base_ttl_secs,
            ),
        ),
        (
            NARRATIVE_SOURCE_PREHEAT_SOCIAL,
            build_ranked_keyword_records(
                NARRATIVE_SOURCE_PREHEAT_SOCIAL,
                social_keywords,
                now,
                shared.config.narrative_preheat_ttl_secs,
            ),
        ),
        (
            NARRATIVE_SOURCE_PREHEAT_TELEGRAM,
            build_ranked_keyword_records(
                NARRATIVE_SOURCE_PREHEAT_TELEGRAM,
                telegram_keywords,
                now,
                shared.config.narrative_preheat_ttl_secs,
            ),
        ),
        (
            NARRATIVE_SOURCE_PREHEAT_EVENT,
            build_ranked_keyword_records(
                NARRATIVE_SOURCE_PREHEAT_EVENT,
                event_keywords,
                now,
                shared.config.narrative_preheat_ttl_secs,
            ),
        ),
        (
            NARRATIVE_SOURCE_CONFIRMED_ONCHAIN,
            build_counted_keyword_records(
                NARRATIVE_SOURCE_CONFIRMED_ONCHAIN,
                confirmed_keywords,
                now,
                shared.config.narrative_confirmed_ttl_secs,
            ),
        ),
    ];

    shared
        .db
        .replace_dynamic_keywords("dynamic_hot_refresh", &[])
        .await?;
    let mut merged = HashMap::new();
    for (source, records) in &source_records {
        shared.db.replace_dynamic_keywords(source, records).await?;
        for record in records {
            merge_narrative_keyword_record(&mut merged, record);
        }
    }

    if merged.is_empty() {
        anyhow::bail!("all narrative keyword sources returned no usable keywords");
    }

    let snapshot = sort_narrative_snapshot(&merged, shared.config.dynamic_hot_keywords_limit);
    let snapshot_keywords: Vec<String> = snapshot
        .iter()
        .map(|(keyword, _)| keyword.clone())
        .collect();
    let preheat_count = snapshot
        .iter()
        .filter(|(_, term)| term.tier == NarrativeTier::Preheat)
        .count();
    let base_count = snapshot
        .iter()
        .filter(|(_, term)| term.tier == NarrativeTier::Base)
        .count();
    let confirmed_count = snapshot
        .iter()
        .filter(|(_, term)| term.tier == NarrativeTier::Confirmed)
        .count();

    write_plaintext_lines(&shared.config.dynamic_hot_keywords_file, &snapshot_keywords).await?;
    info!(
        "Dynamic narrative terms refreshed | total={} | preheat={} | base={} | confirmed={} | sample={}",
        snapshot_keywords.len(),
        preheat_count,
        base_count,
        confirmed_count,
        snapshot_keywords
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join("|")
    );
    Ok(())
}

async fn fetch_dynamic_hot_keywords(shared: &SharedState) -> Result<Vec<String>> {
    let mut keyword_scores: HashMap<String, u32> = HashMap::new();
    let mut source_count = 0usize;

    match fetch_coingecko_trending_search_keywords(shared).await {
        Ok(keywords) => {
            score_dynamic_keywords(&mut keyword_scores, keywords, 4);
            source_count += 1;
        }
        Err(err) => warn!(
            "dynamic keywords: coingecko search trending failed: {}",
            err
        ),
    }

    if let Some(api_key) = shared.config.coingecko_api_key.as_deref() {
        match fetch_coingecko_solana_trending_pool_keywords(shared, api_key).await {
            Ok(keywords) => {
                score_dynamic_keywords(&mut keyword_scores, keywords, 5);
                source_count += 1;
            }
            Err(err) => warn!(
                "dynamic keywords: coingecko solana trending pools failed: {}",
                err
            ),
        }
    }

    match fetch_dex_boosted_keywords(shared).await {
        Ok(keywords) => {
            score_dynamic_keywords(&mut keyword_scores, keywords, 3);
            source_count += 1;
        }
        Err(err) => warn!(
            "dynamic keywords: dexscreener boosted tokens failed: {}",
            err
        ),
    }

    if source_count == 0 {
        anyhow::bail!("all dynamic keyword sources failed");
    }

    let mut ranked: Vec<(String, u32)> = keyword_scores.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Ok(ranked
        .into_iter()
        .take(shared.config.dynamic_hot_keywords_limit.max(1))
        .map(|(keyword, _)| keyword)
        .collect())
}

async fn fetch_coingecko_trending_search_keywords(shared: &SharedState) -> Result<Vec<String>> {
    let payload: Value = shared
        .http
        .get("https://api.coingecko.com/api/v3/search/trending")
        .send()
        .await
        .context("CoinGecko trending search request failed")?
        .error_for_status()
        .context("CoinGecko trending search response invalid")?
        .json()
        .await
        .context("CoinGecko trending search json decode failed")?;

    let mut texts = Vec::new();
    if let Some(coins) = payload.get("coins").and_then(Value::as_array) {
        for coin in coins {
            let item = coin.get("item").unwrap_or(coin);
            collect_keyword_source_texts(item, &mut texts);
        }
    }
    if let Some(categories) = payload.get("categories").and_then(Value::as_array) {
        for category in categories {
            collect_keyword_source_texts(category, &mut texts);
        }
    }
    Ok(extract_dynamic_keywords_from_texts(texts))
}

async fn fetch_coingecko_solana_trending_pool_keywords(
    shared: &SharedState,
    api_key: &str,
) -> Result<Vec<String>> {
    let payload: Value = shared
        .http
        .get("https://pro-api.coingecko.com/api/v3/onchain/networks/solana/trending_pools")
        .header("x-cg-pro-api-key", api_key)
        .query(&[("include", "base_token"), ("duration", "1h")])
        .send()
        .await
        .context("CoinGecko solana trending pools request failed")?
        .error_for_status()
        .context("CoinGecko solana trending pools response invalid")?
        .json()
        .await
        .context("CoinGecko solana trending pools json decode failed")?;

    let mut texts = Vec::new();
    if let Some(data) = payload.get("data").and_then(Value::as_array) {
        for item in data {
            collect_keyword_source_texts(item, &mut texts);
            if let Some(attrs) = item.get("attributes") {
                collect_keyword_source_texts(attrs, &mut texts);
            }
        }
    }
    if let Some(included) = payload.get("included").and_then(Value::as_array) {
        for item in included {
            collect_keyword_source_texts(item, &mut texts);
            if let Some(attrs) = item.get("attributes") {
                collect_keyword_source_texts(attrs, &mut texts);
            }
        }
    }
    Ok(extract_dynamic_keywords_from_texts(texts))
}

async fn fetch_dex_boosted_keywords(shared: &SharedState) -> Result<Vec<String>> {
    let mut token_addresses = Vec::new();
    for endpoint in [
        "https://api.dexscreener.com/token-boosts/latest/v1",
        "https://api.dexscreener.com/token-boosts/top/v1",
    ] {
        let payload: Value = shared
            .http
            .get(endpoint)
            .send()
            .await
            .with_context(|| format!("DexScreener request failed: {}", endpoint))?
            .error_for_status()
            .with_context(|| format!("DexScreener response invalid: {}", endpoint))?
            .json()
            .await
            .with_context(|| format!("DexScreener json decode failed: {}", endpoint))?;

        collect_solana_token_addresses(&payload, &mut token_addresses);
    }

    token_addresses.sort();
    token_addresses.dedup();
    token_addresses.truncate(30);
    if token_addresses.is_empty() {
        anyhow::bail!("DexScreener returned no boosted Solana tokens");
    }

    let token_url = format!(
        "https://api.dexscreener.com/tokens/v1/solana/{}",
        token_addresses.join(",")
    );
    let payload: Value = shared
        .http
        .get(token_url)
        .send()
        .await
        .context("DexScreener token metadata request failed")?
        .error_for_status()
        .context("DexScreener token metadata response invalid")?
        .json()
        .await
        .context("DexScreener token metadata json decode failed")?;

    let mut texts = Vec::new();
    if let Some(items) = payload
        .as_array()
        .or_else(|| payload.get("pairs").and_then(Value::as_array))
        .or_else(|| payload.get("data").and_then(Value::as_array))
    {
        for item in items {
            if let Some(base_token) = item.get("baseToken") {
                collect_keyword_source_texts(base_token, &mut texts);
            }
            collect_keyword_source_texts(item, &mut texts);
        }
    }
    Ok(extract_dynamic_keywords_from_texts(texts))
}

fn collect_solana_token_addresses(payload: &Value, out: &mut Vec<String>) {
    if let Some(items) = payload
        .as_array()
        .or_else(|| payload.get("data").and_then(Value::as_array))
    {
        for item in items {
            let Some(chain_id) = item.get("chainId").and_then(Value::as_str) else {
                continue;
            };
            if chain_id != "solana" {
                continue;
            }
            if let Some(address) = item.get("tokenAddress").and_then(Value::as_str) {
                out.push(address.to_string());
            }
        }
    }
}

fn collect_keyword_source_texts(value: &Value, out: &mut Vec<String>) {
    for key in ["name", "symbol", "token_name", "token_symbol"] {
        if let Some(text) = value.get(key).and_then(Value::as_str) {
            out.push(text.to_string());
        }
    }
}

fn extract_dynamic_keywords_from_texts(texts: Vec<String>) -> Vec<String> {
    let mut keywords = Vec::new();
    for text in texts {
        for token in tokenize_keyword_text(&text) {
            if should_keep_dynamic_keyword(&token) {
                keywords.push(token);
            }
        }
    }
    keywords
}

fn should_keep_dynamic_keyword(token: &str) -> bool {
    let len = token.chars().count();
    if len < 2 || len > 24 {
        return false;
    }
    if token.chars().all(|ch| ch.is_ascii_digit()) {
        return false;
    }
    !DYNAMIC_KEYWORD_STOPWORDS.contains(&token)
}

fn score_dynamic_keywords(scores: &mut HashMap<String, u32>, keywords: Vec<String>, weight: u32) {
    let mut seen = HashSet::new();
    for keyword in keywords {
        if seen.insert(keyword.clone()) {
            *scores.entry(keyword).or_default() += weight;
        }
    }
}

async fn reload_hotlists(shared: &SharedState) -> Result<()> {
    let now = now_ms();
    let blacklist = load_plaintext_set(&shared.config.creator_blacklist_file).await?;
    let smart_money = load_plaintext_set(&shared.config.smart_money_file).await?;
    let smart_money_funders = load_plaintext_set(&shared.config.smart_money_funder_file).await?;
    let blocked_buyers = load_plaintext_set(&shared.config.blocked_buyers_file).await?;
    let dynamic_hot_keywords = load_plaintext_set(&shared.config.dynamic_hot_keywords_file).await?;
    let active_dynamic_keywords = shared.db.list_active_dynamic_keywords(now).await?;
    let mut narrative_terms = HashMap::new();
    for record in &active_dynamic_keywords {
        merge_narrative_keyword_record(&mut narrative_terms, record);
    }
    if narrative_terms.is_empty() {
        for keyword in &dynamic_hot_keywords {
            narrative_terms.insert(
                keyword.clone(),
                NarrativeHotTerm {
                    tier: NarrativeTier::Preheat,
                    score: 1,
                    source_mask: 0,
                },
            );
        }
    }
    {
        let mut hotlists = shared.hotlists.write().await;
        hotlists.creator_blacklist = blacklist.iter().cloned().collect();
        hotlists.smart_money = smart_money.iter().cloned().collect();
        hotlists.smart_money_funders = smart_money_funders.iter().cloned().collect();
        hotlists.blocked_buyers = blocked_buyers.iter().cloned().collect();
        hotlists.dynamic_hot_keywords = narrative_terms.keys().cloned().collect();
        hotlists.narrative_terms = narrative_terms.clone();
    }
    shared.db.sync_blacklist(&blacklist).await?;
    shared.db.sync_smart_money(&smart_money).await?;
    info!(
        "Filter hotlists loaded | blacklist={} | smart_money={} | smart_money_funders={} | blocked_buyers={} | dynamic_hot_keywords={} | narrative_terms={}",
        blacklist.len(),
        smart_money.len(),
        smart_money_funders.len(),
        blocked_buyers.len(),
        dynamic_hot_keywords.len(),
        narrative_terms.len(),
    );
    if shared.config.disable_smart_money_filter {
        warn!(
            "smart_money filter disabled, forcing early-buyer fallback | fast_threshold={} | soft_threshold={} | fast_window_ms={} | soft_window_ms={}",
            effective_fast_threshold(&shared.config, SmartMoneyMode::EarlyBuyerFallback),
            effective_soft_threshold(&shared.config, SmartMoneyMode::EarlyBuyerFallback),
            effective_fast_window_ms(&shared.config),
            effective_soft_window_ms(&shared.config),
        );
    } else if smart_money.is_empty() && smart_money_funders.is_empty() {
        warn!(
            "smart_money lists empty, enabling early-buyer fallback | fast_threshold={} | soft_threshold={} | fast_window_ms={} | soft_window_ms={}",
            effective_fast_threshold(&shared.config, SmartMoneyMode::EarlyBuyerFallback),
            effective_soft_threshold(&shared.config, SmartMoneyMode::EarlyBuyerFallback),
            effective_fast_window_ms(&shared.config),
            effective_soft_window_ms(&shared.config),
        );
    }
    Ok(())
}

async fn load_plaintext_set(path: &str) -> Result<Vec<String>> {
    let path_ref = std::path::Path::new(path);
    if let Some(parent) = path_ref.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if !path_ref.exists() {
        tokio::fs::write(path_ref, b"").await?;
    }
    let content = tokio::fs::read_to_string(path_ref).await?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToOwned::to_owned)
        .collect())
}

async fn write_plaintext_lines(path: &str, lines: &[String]) -> Result<()> {
    let path_ref = std::path::Path::new(path);
    if let Some(parent) = path_ref.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut content = lines.join("\n");
    if !content.is_empty() {
        content.push('\n');
    }
    tokio::fs::write(path_ref, content)
        .await
        .with_context(|| format!("write output file failed: {}", path_ref.display()))?;
    Ok(())
}

async fn append_jsonl(path: &str, value: &Value) -> Result<()> {
    let path_ref = std::path::Path::new(path);
    if let Some(parent) = path_ref.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path_ref)
        .await
        .with_context(|| format!("open output file failed: {}", path_ref.display()))?;

    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    file.write_all(&line).await?;
    Ok(())
}

async fn record_candidate_outcome(
    shared: &SharedState,
    candidate: &Candidate,
    passed: bool,
    reject_gate: Option<String>,
    score: Option<u32>,
    reason: String,
    mode: String,
    path: String,
    early_buy_count: usize,
    matched_buyers: usize,
) {
    let reject_gate_ref = reject_gate.clone();
    let score_ref = score;
    let reason_ref = reason.clone();
    let mode_ref = mode.clone();
    let path_ref = path.clone();
    let analytics_candidate = candidate.clone();
    record_token_outcome(
        shared,
        &candidate.token,
        &candidate.trace,
        passed,
        reject_gate,
        score,
        reason,
        mode,
        path,
        early_buy_count,
        matched_buyers,
        None,
    )
    .await;
    enqueue_persist_task(
        shared,
        FilterPersistTask::CandidateAnalytics {
            candidate: analytics_candidate,
            passed,
            reject_gate: reject_gate_ref,
            score: score_ref,
            reason: reason_ref,
            mode: mode_ref,
            path: path_ref.clone(),
        },
    );

    if passed {
        spawn_post_trade_outcome_tracking(
            shared,
            candidate.token.clone(),
            path_ref,
            score_ref.unwrap_or_default(),
        );
    }
}

async fn record_token_outcome(
    shared: &SharedState,
    token: &NewToken,
    trace: &CandidateTrace,
    passed: bool,
    reject_gate: Option<String>,
    score: Option<u32>,
    reason: String,
    mode: String,
    path: String,
    early_buy_count: usize,
    matched_buyers: usize,
    risk_signals: Option<Vec<RiskSignalRecord>>,
) {
    let final_at_ms = now_ms();
    let latency_ms = final_at_ms.saturating_sub(token.detected_at_ms);

    let record = FilterResultRecord {
        mint: token.mint.clone(),
        creator: token.creator.clone(),
        symbol: token.symbol.clone(),
        passed,
        reject_gate: reject_gate.clone(),
        score,
        reason: reason.clone(),
        ts: final_at_ms,
    };
    if let Err(err) = shared.db.insert_filter_result(&record).await {
        error!("filter: insert filter_results failed: {}", err);
    }

    let timing = FilterTimingRecord {
        mint: token.mint.clone(),
        decision: if passed {
            "pass".to_string()
        } else {
            reject_gate.clone().unwrap_or_else(|| "reject".to_string())
        },
        mode: mode.clone(),
        path: path.clone(),
        detected_at_ms: token.detected_at_ms,
        gate1_at_ms: trace.gate1_at_ms,
        gate2_at_ms: trace.gate2_at_ms,
        gate3_open_at_ms: trace.gate3_open_at_ms,
        gate3_trigger_at_ms: trace.gate3_trigger_at_ms,
        gate4_at_ms: trace.gate4_at_ms,
        final_at_ms,
        latency_ms,
        early_buy_count,
        matched_buyers,
    };
    if let Err(err) = shared.db.insert_filter_timing(&timing).await {
        error!("filter: insert filter_timelines failed: {}", err);
    }

    if let Err(err) = append_jsonl(
        &shared.config.latency_metrics_file,
        &json!({
            "mint": &token.mint,
            "decision": &timing.decision,
            "mode": &timing.mode,
            "path": &timing.path,
            "detected_at_ms": timing.detected_at_ms,
            "gate1_at_ms": timing.gate1_at_ms,
            "gate2_at_ms": timing.gate2_at_ms,
            "gate3_open_at_ms": timing.gate3_open_at_ms,
            "gate3_trigger_at_ms": timing.gate3_trigger_at_ms,
            "gate4_at_ms": timing.gate4_at_ms,
            "final_at_ms": timing.final_at_ms,
            "latency_ms": timing.latency_ms,
            "early_buy_count": timing.early_buy_count,
            "matched_buyers": timing.matched_buyers,
            "score": score,
        }),
    )
    .await
    {
        warn!("filter latency append failed: {}", err);
    }

    if let Some(risk_signals) = risk_signals {
        if !risk_signals.is_empty() {
            if let Err(err) = shared
                .db
                .replace_risk_signals(&token.mint, &risk_signals)
                .await
            {
                warn!(
                    "replace risk signals failed | mint={} | {}",
                    token.mint, err
                );
            }
        }
    }

    if passed {
        info!(
            "filter: pass | mint={} | score={:?} | mode={} | path={} | {}",
            token.mint, score, mode, path, reason
        );
    } else {
        info!(
            "filter: reject | mint={} | gate={} | mode={} | path={} | {}",
            token.mint,
            reject_gate.unwrap_or_else(|| "-".to_string()),
            mode,
            path,
            reason
        );
    }
}

fn spawn_filter_persist_worker(
    shared: SharedState,
    mut persist_rx: mpsc::UnboundedReceiver<FilterPersistTask>,
) {
    tokio::spawn(async move {
        while let Some(task) = persist_rx.recv().await {
            match task {
                FilterPersistTask::Gate1Artifacts(artifacts) => {
                    if let Err(err) = shared
                        .db
                        .upsert_creator_template_count(
                            &artifacts.creator,
                            &artifacts.template_hash,
                            artifacts.template_repeat_count,
                            Some(&artifacts.mint),
                            artifacts.updated_at_ms,
                        )
                        .await
                    {
                        warn!(
                            "upsert creator template failed | creator={} | mint={} | {}",
                            artifacts.creator, artifacts.mint, err
                        );
                    }
                    if let Some(record) = artifacts.uri_pattern_record.as_ref() {
                        if let Err(err) = shared.db.upsert_uri_pattern(record).await {
                            warn!(
                                "upsert uri pattern failed | mint={} | {}",
                                artifacts.mint, err
                            );
                        }
                    }
                }
                FilterPersistTask::CreatorLinks { token, profile } => {
                    persist_entity_links_for_creator(&shared, &token, &profile).await;
                }
                FilterPersistTask::BuyerLinks { mint, profile } => {
                    persist_entity_links_for_buyer(&shared, &mint, &profile).await;
                }
                FilterPersistTask::CandidateAnalytics {
                    candidate,
                    passed,
                    reject_gate,
                    score,
                    reason,
                    mode,
                    path,
                } => {
                    persist_candidate_analytics(
                        &shared,
                        &candidate,
                        passed,
                        reject_gate.as_deref(),
                        score,
                        &reason,
                        &mode,
                        &path,
                    )
                    .await;
                }
            }
        }
        info!("filter: persist worker stopped");
    });
}

fn enqueue_persist_task(shared: &SharedState, task: FilterPersistTask) {
    if shared.persist_tx.send(task).is_err() {
        warn!("filter persist queue closed; dropping background task");
    }
}

fn spawn_post_trade_outcome_tracking(
    shared: &SharedState,
    token: NewToken,
    path: String,
    score: u32,
) {
    let shared = shared.clone();
    tokio::spawn(async move {
        let start = Instant::now();
        let checkpoints = [10u64, 30u64, 60u64];
        let mut metrics = [None, None, None];

        for (idx, seconds) in checkpoints.into_iter().enumerate() {
            let elapsed_secs = start.elapsed().as_secs();
            if seconds > elapsed_secs {
                tokio::time::sleep(Duration::from_secs(seconds - elapsed_secs)).await;
            }
            metrics[idx] = fetch_curve_progress_pct(&shared, &token).await.ok();
        }

        let observed: Vec<f64> = metrics.iter().flatten().copied().collect();
        let peak_metric = observed.iter().copied().reduce(f64::max);
        let trough_metric = observed.iter().copied().reduce(f64::min);
        let drawdown_metric = peak_metric
            .zip(trough_metric)
            .map(|(peak, trough)| (peak - trough).max(0.0));
        let record = PostTradeOutcomeRecord {
            mint: token.mint.clone(),
            path,
            score,
            metric_type: "curve_progress_pct".to_string(),
            metric_10s: metrics[0],
            metric_30s: metrics[1],
            metric_60s: metrics[2],
            peak_metric,
            drawdown_metric,
            recorded_at_ms: now_ms(),
        };
        if let Err(err) = shared.db.upsert_post_trade_outcome(&record).await {
            warn!(
                "post-trade outcome persist failed | mint={} | {}",
                token.mint, err
            );
        }
    });
}

async fn persist_raw_new_token_event(shared: &SharedState, token: &NewToken) {
    if !shared.config.persist_raw_scanner_events {
        return;
    }
    let meta = token.meta();
    let payload = json!({
        "mint": token.mint,
        "creator": token.creator,
        "name": token.name,
        "symbol": token.symbol,
        "uri": token.uri,
        "bonding_curve": token.bonding_curve,
        "signature": token.signature,
        "slot": token.slot,
        "feed_source": token.feed_source,
        "is_v2": token.is_v2,
        "detected_at_ms": token.detected_at_ms,
        "instruction_data_b58": meta.raw_meta.instruction_data_b58,
        "instruction_accounts": meta.raw_meta.instruction_accounts,
        "raw_meta": meta.raw_meta,
    });
    let record = RawEventRecord {
        feed_source: token.feed_source.clone(),
        event_type: "new_token".to_string(),
        slot: token.slot,
        signature: token.signature.clone(),
        mint: token.mint.clone(),
        actor: Some(token.creator.clone()),
        recorded_at_ms: token.detected_at_ms,
        payload_json: payload.to_string(),
    };
    if let Err(err) = shared.db.insert_raw_event(&record).await {
        warn!("raw new_token insert failed: {}", err);
    }
}

fn spawn_raw_new_token_persist_worker(
    shared: SharedState,
    mut raw_new_token_persist_rx: mpsc::Receiver<NewToken>,
) {
    tokio::spawn(async move {
        while let Some(token) = raw_new_token_persist_rx.recv().await {
            persist_raw_new_token_event(&shared, &token).await;
        }
        info!("filter: raw new_token persist worker stopped");
    });
}

fn enqueue_raw_new_token_persist(shared: &SharedState, token: &NewToken) {
    if !shared.config.persist_raw_scanner_events {
        return;
    }
    match shared.raw_new_token_persist_tx.try_send(token.clone()) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            warn!(
                "raw new_token persist queue full; dropping event | mint={}",
                token.mint
            );
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            warn!(
                "raw new_token persist queue closed; dropping event | mint={}",
                token.mint
            );
        }
    }
}

fn spawn_raw_buy_persist_worker(
    shared: SharedState,
    mut raw_buy_persist_rx: mpsc::Receiver<PumpBuyEvent>,
) {
    tokio::spawn(async move {
        while let Some(buy) = raw_buy_persist_rx.recv().await {
            persist_raw_buy_event(&shared, &buy).await;
        }
        info!("filter: raw buy persist worker stopped");
    });
}

fn enqueue_raw_buy_persist(shared: &SharedState, buy: &PumpBuyEvent) {
    if !shared.config.persist_raw_scanner_events {
        return;
    }
    match shared.raw_buy_persist_tx.try_send(buy.clone()) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            warn!(
                "raw buy persist queue full; dropping event | mint={}",
                buy.mint
            );
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            warn!(
                "raw buy persist queue closed; dropping event | mint={}",
                buy.mint
            );
        }
    }
}

async fn persist_raw_buy_event(shared: &SharedState, buy: &PumpBuyEvent) {
    if !shared.config.persist_raw_scanner_events {
        return;
    }
    let meta = buy.meta();
    let payload = json!({
        "mint": buy.mint,
        "buyer": buy.buyer.to_string(),
        "sol_amount_lamports": buy.sol_amount_lamports,
        "feed_source": buy.feed_source,
        "signature": buy.signature,
        "slot": buy.slot,
        "token_program": buy.token_program.to_string(),
        "detected_at_ms": buy.detected_at_ms,
        "instruction_data_b58": meta.raw_meta.instruction_data_b58,
        "instruction_accounts": meta.raw_meta.instruction_accounts,
        "raw_meta": meta.raw_meta,
    });
    let record = RawEventRecord {
        feed_source: buy.feed_source.clone(),
        event_type: "buy".to_string(),
        slot: buy.slot,
        signature: buy.signature.clone(),
        mint: buy.mint.clone(),
        actor: Some(buy.buyer.to_string()),
        recorded_at_ms: buy.detected_at_ms,
        payload_json: payload.to_string(),
    };
    if let Err(err) = shared.db.insert_raw_event(&record).await {
        warn!("raw buy insert failed: {}", err);
    }
}

async fn persist_entity_links_for_creator(
    shared: &SharedState,
    token: &NewToken,
    profile: &CreatorProfile,
) {
    let Some(funder) = profile.first_funder.clone() else {
        return;
    };
    let funder_profile = FunderProfile {
        address: funder.clone(),
        wallet_count: 1,
        rug_exposure: profile.rug_count,
        last_seen_ms: profile.fetched_at_ms,
    };
    if let Err(err) = shared.db.upsert_funder_profile(&funder_profile).await {
        warn!("upsert funder profile failed | funder={} | {}", funder, err);
    }
    let cluster_id = cluster_id_for_funder(&funder);
    let member = ClusterMemberRecord {
        cluster_id: cluster_id.clone(),
        address: token.creator.clone(),
        cluster_type: "creator_funder".to_string(),
        score: 100,
    };
    if let Err(err) = shared.db.upsert_cluster_member(&member).await {
        warn!("upsert creator cluster member failed | {}", err);
    }
    let edge = ClusterEdgeRecord {
        src: funder,
        dst: token.creator.clone(),
        edge_type: "funds_creator".to_string(),
        weight: profile.total_tokens.max(1) as i32,
    };
    if let Err(err) = shared.db.upsert_cluster_edge(&edge).await {
        warn!("upsert creator cluster edge failed | {}", err);
    }
}

async fn persist_entity_links_for_buyer(shared: &SharedState, mint: &str, profile: &BuyerProfile) {
    let Some(funder) = profile.first_funder.clone() else {
        return;
    };
    let current = shared
        .db
        .get_funder_profile(&funder)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let funder_profile = FunderProfile {
        address: funder.clone(),
        wallet_count: current.wallet_count.saturating_add(1),
        rug_exposure: current.rug_exposure,
        last_seen_ms: profile.fetched_at_ms.max(current.last_seen_ms),
    };
    if let Err(err) = shared.db.upsert_funder_profile(&funder_profile).await {
        warn!(
            "upsert buyer funder profile failed | funder={} | {}",
            funder, err
        );
    }
    let cluster_id = cluster_id_for_funder(&funder);
    let member = ClusterMemberRecord {
        cluster_id: cluster_id.clone(),
        address: profile.address.clone(),
        cluster_type: "buyer_funder".to_string(),
        score: 50,
    };
    if let Err(err) = shared.db.upsert_cluster_member(&member).await {
        warn!("upsert buyer cluster member failed | {}", err);
    }
    let edge = ClusterEdgeRecord {
        src: funder,
        dst: profile.address.clone(),
        edge_type: format!("funds_buyer:{}", mint),
        weight: 1,
    };
    if let Err(err) = shared.db.upsert_cluster_edge(&edge).await {
        warn!("upsert buyer cluster edge failed | {}", err);
    }
}

fn ordered_edge_endpoints(left: &str, right: &str) -> (String, String) {
    if left <= right {
        (left.to_string(), right.to_string())
    } else {
        (right.to_string(), left.to_string())
    }
}

fn cluster_id_for_funder(funder: &str) -> String {
    format!("funder:{}", funder)
}

async fn persist_launch_participation_edges(shared: &SharedState, candidate: &Candidate) {
    let creator_funder = candidate
        .creator_profile
        .as_ref()
        .and_then(|profile| profile.first_funder.clone());
    let mut participants = Vec::new();
    let mut seen_buyers = HashSet::new();

    for (idx, buy) in candidate.early_buys.iter().enumerate() {
        let buyer = buy.buyer.to_string();
        if !seen_buyers.insert(buyer.clone()) {
            continue;
        }
        let funder = candidate
            .buyer_profiles
            .get(&buyer)
            .and_then(|profile| profile.first_funder.clone());
        participants.push((idx, buyer, funder));
        if participants.len() >= shared.config.smart_money_max_buys {
            break;
        }
    }

    for (idx, buyer, funder) in &participants {
        let launch_edge = ClusterEdgeRecord {
            src: candidate.token.creator.clone(),
            dst: buyer.clone(),
            edge_type: format!("launch_participation:{}", candidate.token.mint),
            weight: (shared.config.smart_money_max_buys.saturating_sub(*idx)).max(1) as i32,
        };
        if let Err(err) = shared.db.upsert_cluster_edge(&launch_edge).await {
            warn!("upsert launch participation edge failed | {}", err);
        }

        if creator_funder
            .as_deref()
            .zip(funder.as_deref())
            .is_some_and(|(creator_funder, buyer_funder)| creator_funder == buyer_funder)
        {
            let member = ClusterMemberRecord {
                cluster_id: cluster_id_for_funder(creator_funder.as_deref().unwrap_or_default()),
                address: buyer.clone(),
                cluster_type: "creator_launch_peer".to_string(),
                score: 70,
            };
            if let Err(err) = shared.db.upsert_cluster_member(&member).await {
                warn!("upsert creator launch cluster member failed | {}", err);
            }
        }
    }

    for (left_idx, (_, left_buyer, left_funder)) in participants.iter().enumerate() {
        for (_, right_buyer, right_funder) in participants.iter().skip(left_idx + 1) {
            let (src, dst) = ordered_edge_endpoints(left_buyer, right_buyer);
            let peer_edge = ClusterEdgeRecord {
                src,
                dst,
                edge_type: format!("launch_peer:{}", candidate.token.mint),
                weight: 1,
            };
            if let Err(err) = shared.db.upsert_cluster_edge(&peer_edge).await {
                warn!("upsert launch peer edge failed | {}", err);
            }

            if let (Some(left_funder), Some(right_funder)) =
                (left_funder.as_deref(), right_funder.as_deref())
            {
                if left_funder != right_funder {
                    let (src, dst) = ordered_edge_endpoints(
                        &cluster_id_for_funder(left_funder),
                        &cluster_id_for_funder(right_funder),
                    );
                    let peer_edge = ClusterEdgeRecord {
                        src,
                        dst,
                        edge_type: format!("launch_funder_peer:{}", candidate.token.mint),
                        weight: 1,
                    };
                    if let Err(err) = shared.db.upsert_cluster_edge(&peer_edge).await {
                        warn!("upsert launch funder peer edge failed | {}", err);
                    }
                }
            }
        }
    }
}

async fn persist_candidate_analytics(
    shared: &SharedState,
    candidate: &Candidate,
    passed: bool,
    reject_gate: Option<&str>,
    score: Option<u32>,
    reason: &str,
    mode: &str,
    path: &str,
) {
    let stats = smart_money_stats(candidate, shared).await;
    persist_launch_participation_edges(shared, candidate).await;
    let first_buy_ms = candidate
        .early_buys
        .iter()
        .map(|buy| buy.detected_at_ms.saturating_sub(candidate.detected_at_ms))
        .min();
    let threshold_hit_ms = match path {
        "fast" => stats.fast_reached_at_ms,
        "soft" => stats.soft_reached_at_ms,
        _ => None,
    };

    let snapshot = Gate3SnapshotRecord {
        mint: candidate.token.mint.clone(),
        mode: mode.to_string(),
        path: path.to_string(),
        buy_count: stats.buy_count,
        unique_buyers: stats.eligible_buyers,
        unique_funders: stats.unique_funders,
        matched_buyers: stats.unique_sm_wallets.len(),
        total_sol: stats.total_eligible_sol,
        matched_sol: stats.sm_sol_total,
        creator_buy_sol: stats.creator_buy_sol,
        max_single_buyer_share: stats.max_single_buyer_share,
        first_buy_ms,
        threshold_hit_ms,
        recorded_at_ms: now_ms(),
    };
    if let Err(err) = shared.db.insert_gate3_snapshot(&snapshot).await {
        warn!(
            "insert gate3 snapshot failed | mint={} | {}",
            candidate.token.mint, err
        );
    }

    if shared.config.persist_gate3_sequences {
        let sequences: Vec<Gate3SequenceRecord> = candidate
            .early_buys
            .iter()
            .enumerate()
            .map(|(idx, buy)| {
                let buyer = buy.buyer.to_string();
                let funder = candidate
                    .buyer_profiles
                    .get(&buyer)
                    .and_then(|profile| profile.first_funder.clone());
                Gate3SequenceRecord {
                    mint: candidate.token.mint.clone(),
                    seq_no: idx,
                    buyer: buyer.clone(),
                    funder: funder.clone(),
                    cluster_id: funder.as_deref().map(cluster_id_for_funder),
                    sol_amount: buy.sol_amount_lamports as f64 / 1e9,
                    detected_at_ms: buy.detected_at_ms,
                    is_creator: buyer == candidate.token.creator,
                    feed_source: buy.feed_source.clone(),
                }
            })
            .collect();
        if let Err(err) = shared
            .db
            .replace_gate3_sequences(&candidate.token.mint, &sequences)
            .await
        {
            warn!(
                "replace gate3 sequences failed | mint={} | {}",
                candidate.token.mint, err
            );
        }
    }

    if shared.config.persist_scoring_breakdowns {
        let scoring = build_scoring_context(
            shared,
            candidate,
            &stats,
            if path == "soft" {
                Gate3Path::Soft
            } else {
                Gate3Path::Fast
            },
        )
        .await;
        let details = json!({
            "participants_score": scoring.participants_score,
            "capital_score": scoring.capital_score,
            "momentum_score": scoring.momentum_score,
            "curve_score": scoring.curve_score,
            "buyer_quality_score": scoring.buyer_quality_score,
            "funder_diversity_penalty": scoring.funder_diversity_penalty,
            "risk_penalty": scoring.runtime_risk.penalty_score,
            "risk_signals": scoring.runtime_risk
                .signals
                .iter()
                .map(|signal| json!({
                    "type": signal.signal_type,
                    "value": signal.signal_value,
                    "score": signal.score,
                    "severity": format!("{:?}", signal.severity),
                }))
                .collect::<Vec<_>>(),
            "creator_funder_match_count": stats.creator_funder_match_count,
            "creator_funder_match_sol": stats.creator_funder_match_sol,
            "largest_funder_cluster_size": scoring.cluster.largest_funder_cluster_size,
            "largest_funder_cluster_share": scoring.cluster.largest_funder_cluster_share,
            "hotlist_funder_hits": scoring.cluster.hotlist_funder_hits,
            "hotlist_funder_diversity": scoring.cluster.hotlist_funder_diversity,
            "quality_cluster": scoring.cluster.quality_cluster,
            "quality_cluster_bonus": scoring.cluster.quality_cluster_bonus,
            "fast_required_score_relief": scoring.cluster.fast_required_score_relief,
            "suspicious_funder_penalty": scoring.cluster.suspicious_funder_penalty,
            "same_cluster_first_buy_penalty": scoring.cluster.same_cluster_first_buy_penalty,
            "creator_cluster_funder": scoring.cluster.creator_funder,
            "creator_cluster_wallet_count": scoring.cluster.creator_cluster_wallet_count,
            "creator_cluster_rug_exposure": scoring.cluster.creator_cluster_rug_exposure,
            "dynamic_narrative_bonus": scoring.dynamic_narrative_bonus,
            "narrative_preheat_hits": scoring.narrative.preheat_hits,
            "narrative_base_hits": scoring.narrative.base_hits,
            "narrative_confirmed_hits": scoring.narrative.confirmed_hits,
            "narrative_preheat_bonus": scoring.narrative.preheat_bonus,
            "narrative_base_bonus": scoring.narrative.base_bonus,
            "narrative_confirmed_bonus": scoring.narrative.confirmed_bonus,
            "narrative_fast_required_score_relief": scoring.narrative.fast_required_score_relief,
            "dynamic_narrative_keywords": &candidate.dynamic_narrative_keywords,
            "dynamic_narrative_tiers": candidate
                .narrative_keyword_matches
                .iter()
                .map(|item| json!({
                    "keyword": item.keyword,
                    "tier": format!("{:?}", item.tier),
                    "score": item.score,
                    "source_mask": item.source_mask,
                }))
                .collect::<Vec<_>>(),
            "curve_progress_pct": scoring.curve_progress_pct,
            "buyer_quality_pct": scoring.buyer_quality_pct,
            "reason": reason,
        });
        let breakdown = ScoringBreakdownRecord {
            mint: candidate.token.mint.clone(),
            path: path.to_string(),
            quality_score: scoring.quality_score,
            urgency_score: scoring.urgency_score,
            execution_confidence: if passed {
                scoring.execution_confidence
            } else {
                scoring.quality_score.min(100)
            },
            total_score: scoring.total_score,
            required_score: scoring.required_score,
            details_json: details.to_string(),
            recorded_at_ms: now_ms(),
        };
        if let Err(err) = shared.db.insert_scoring_breakdown(&breakdown).await {
            warn!(
                "insert scoring breakdown failed | mint={} | {}",
                candidate.token.mint, err
            );
        }
    }

    let risk_signals = build_risk_signals(
        candidate,
        &stats,
        reject_gate,
        reason,
        shared.config.as_ref(),
    );
    if !risk_signals.is_empty() {
        if let Err(err) = shared
            .db
            .replace_risk_signals(&candidate.token.mint, &risk_signals)
            .await
        {
            warn!(
                "replace risk signals failed | mint={} | {}",
                candidate.token.mint, err
            );
        }
    }

    if shared.config.persist_label_suggestions {
        for suggestion in build_label_suggestions(candidate, passed, reject_gate, score, reason) {
            if let Err(err) = shared.db.insert_label_suggestion(&suggestion).await {
                warn!(
                    "insert label suggestion failed | subject={} | {}",
                    suggestion.subject, err
                );
            }
        }
    }
}

fn build_risk_signals(
    candidate: &Candidate,
    stats: &WindowStats,
    reject_gate: Option<&str>,
    reason: &str,
    config: &AppConfig,
) -> Vec<RiskSignalRecord> {
    let detected_at_ms = now_ms();
    let runtime_risk = analyze_runtime_risk(
        &candidate.gate1_risk,
        RuntimeRiskInput {
            eligible_buyers: stats.eligible_buyers,
            unique_funders: stats.unique_funders,
            total_eligible_sol: stats.total_eligible_sol,
            creator_funder_match_count: stats.creator_funder_match_count,
            creator_funder_match_sol: stats.creator_funder_match_sol,
            max_single_buyer_share: stats.max_single_buyer_share,
        },
        config,
    );
    let mut signals = risk_signal_records_from_seeds(
        &candidate.token.mint,
        &candidate.gate1_risk.signals,
        detected_at_ms,
    );
    signals.extend(risk_signal_records_from_seeds(
        &candidate.token.mint,
        &runtime_risk.signals,
        detected_at_ms,
    ));
    let mut push = |signal_type: &str, signal_value: &str, score: i32| {
        signals.push(RiskSignalRecord {
            mint: candidate.token.mint.clone(),
            signal_type: signal_type.to_string(),
            signal_value: signal_value.to_string(),
            score,
            detected_at_ms,
        });
    };
    if let Some(gate) = reject_gate {
        push("reject_gate", gate, 20);
    }
    if reason.contains("factory creator pattern") {
        push("factory_creator", candidate.token.creator.as_str(), 90);
    }
    if reason.contains("creator self-buy") {
        push("creator_self_buy", candidate.token.creator.as_str(), 80);
    }
    if reason.contains("blacklist keyword") {
        push("blacklist_keyword", reason, 70);
    }
    if reason.contains("symbol too long") {
        push("symbol_shape", candidate.token.symbol.as_str(), 35);
    }
    if reason.contains("early concentration") {
        push("buy_concentration", reason, 60);
    }
    if stats.largest_funder_cluster_size >= SAME_FUNDER_CLUSTER_PENALTY_MIN_BUYS
        && stats.largest_funder_cluster_share >= SAME_FUNDER_CLUSTER_PENALTY_SHARE
    {
        push(
            "same_funder_cluster_pressure",
            &format!(
                "size={} share={:.4}",
                stats.largest_funder_cluster_size, stats.largest_funder_cluster_share
            ),
            SAME_FUNDER_CLUSTER_PENALTY_SCORE as i32,
        );
    }
    if let Some(funder) = candidate
        .creator_profile
        .as_ref()
        .and_then(|profile| profile.first_funder.as_deref())
    {
        push("creator_funder_cluster", funder, 15);
    }
    if let Some(host) = candidate.gate1_risk.uri_host.as_deref() {
        push("uri_host", host, 10);
    }
    if let Some(pattern) = candidate.gate1_risk.uri_pattern.as_deref() {
        push("uri_pattern", pattern, 10);
    }
    signals
}

fn risk_signal_records_from_seeds(
    mint: &str,
    seeds: &[RiskSignalSeed],
    detected_at_ms: u64,
) -> Vec<RiskSignalRecord> {
    seeds
        .iter()
        .map(|seed| RiskSignalRecord {
            mint: mint.to_string(),
            signal_type: seed.signal_type.clone(),
            signal_value: seed.signal_value.clone(),
            score: seed.score,
            detected_at_ms,
        })
        .collect()
}

fn build_label_suggestions(
    candidate: &Candidate,
    passed: bool,
    reject_gate: Option<&str>,
    score: Option<u32>,
    reason: &str,
) -> Vec<LabelSuggestionRecord> {
    let now = now_ms();
    let mut out = Vec::new();
    if passed && score.unwrap_or_default() >= 60 {
        out.push(LabelSuggestionRecord {
            label_type: "watch_creator".to_string(),
            subject: candidate.token.creator.clone(),
            reason: format!("passed filter with score {}", score.unwrap_or_default()),
            score: score.unwrap_or_default() as i32,
            mint: Some(candidate.token.mint.clone()),
            created_at_ms: now,
        });
    }
    if reason.contains("factory creator pattern") || reason.contains("blacklist keyword") {
        out.push(LabelSuggestionRecord {
            label_type: "creator_blacklist_candidate".to_string(),
            subject: candidate.token.creator.clone(),
            reason: reason.to_string(),
            score: 90,
            mint: Some(candidate.token.mint.clone()),
            created_at_ms: now,
        });
    } else if reject_gate == Some("gate3") && reason.contains("creator self-buy") {
        out.push(LabelSuggestionRecord {
            label_type: "creator_review_candidate".to_string(),
            subject: candidate.token.creator.clone(),
            reason: reason.to_string(),
            score: 70,
            mint: Some(candidate.token.mint.clone()),
            created_at_ms: now,
        });
    } else if reject_gate == Some("gate2") && reason.contains("funder") {
        if let Some(funder) = candidate
            .creator_profile
            .as_ref()
            .and_then(|profile| profile.first_funder.clone())
        {
            out.push(LabelSuggestionRecord {
                label_type: "funder_blacklist_candidate".to_string(),
                subject: funder,
                reason: reason.to_string(),
                score: 80,
                mint: Some(candidate.token.mint.clone()),
                created_at_ms: now,
            });
        }
    }
    out
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::feed::ScannerMode;
    use solana_sdk::signature::{Keypair, Signer};

    fn base_config() -> AppConfig {
        let keypair = Keypair::new();
        AppConfig {
            rpc_url: String::new(),
            secondary_rpc_url: None,
            grpc_url: String::new(),
            grpc_token: None,
            grpc_account_url: String::new(),
            grpc_account_token: None,
            scanner_grpc_url: String::new(),
            scanner_grpc_token: None,
            scanner_secondary_grpc_url: None,
            scanner_secondary_grpc_token: None,
            scanner_secondary_auto_inferred: false,
            scanner_deshred_grpc_url: None,
            scanner_deshred_grpc_token: None,
            scanner_mode: ScannerMode::ProcessedOnly,
            scanner_primary_feed_label: "primary_processed".to_string(),
            scanner_secondary_feed_label: "secondary_processed".to_string(),
            scanner_deshred_feed_label: "deshred_pre_exec".to_string(),
            helius_api_key: None,
            coingecko_api_key: None,
            filter_db_path: String::new(),
            replay_db_path: String::new(),
            replay_mode_enabled: false,
            replay_pipeline_enabled: false,
            replay_from_ms: None,
            replay_to_ms: None,
            replay_window_minutes: 60,
            replay_speedup: 50.0,
            replay_report_file: String::new(),
            runtime_report_enabled: true,
            runtime_report_interval_secs: 60,
            runtime_report_window_secs: 300,
            runtime_report_file: String::new(),
            smart_money_file: String::new(),
            smart_money_funder_file: String::new(),
            blocked_buyers_file: String::new(),
            creator_blacklist_file: String::new(),
            dynamic_hot_keywords_file: String::new(),
            narrative_social_keywords_file: String::new(),
            narrative_telegram_keywords_file: String::new(),
            narrative_event_keywords_file: String::new(),
            latency_metrics_file: String::new(),
            filter_hot_reload_secs: 0,
            dynamic_hot_refresh_secs: 60,
            dynamic_hot_keywords_enabled: true,
            dynamic_hot_keywords_limit: 40,
            narrative_preheat_ttl_secs: 7_200,
            narrative_base_ttl_secs: 86_400,
            narrative_confirmed_ttl_secs: 21_600,
            narrative_onchain_confirm_window_secs: 900,
            narrative_onchain_confirm_min_mints: 2,
            persist_raw_scanner_events: true,
            persist_gate3_sequences: true,
            persist_scoring_breakdowns: true,
            persist_label_suggestions: true,
            persist_feed_health: true,
            smart_money_window_secs: 60,
            smart_money_fast_window_ms: 650,
            smart_money_soft_window_ms: 1_500,
            gate3_hard_reject_ms: 1_800,
            smart_money_fast_threshold: 2,
            smart_money_threshold: 2,
            smart_money_max_buys: 20,
            gate3_fast_min_sol: 0.35,
            gate3_soft_min_sol: 0.90,
            gate3_max_single_buyer_share: 0.85,
            gate3_creator_self_buy_block: true,
            gate3_creator_self_buy_max_sol: 0.75,
            gate3_creator_self_buy_max_share: 0.40,
            gate3_creator_self_buy_hard_sol: 4.00,
            gate3_creator_self_buy_hard_share: 0.55,
            gate3_creator_self_buy_min_external_buyers: 3,
            gate3_creator_self_buy_min_external_sol: 0.75,
            gate3_early_concentration_reject: true,
            gate3_early_concentration_min_buys: 8,
            disable_smart_money_filter: false,
            filter_min_score: 60,
            filter_fast_min_score: 48,
            filter_soft_min_score: 58,
            narrative_preheat_bonus_per_hit: 1,
            narrative_base_bonus_per_hit: 1,
            narrative_confirmed_bonus_per_hit: 2,
            narrative_confirmed_fast_score_relief: 2,
            dynamic_narrative_bonus_per_hit: 3,
            dynamic_narrative_bonus_cap: 6,
            risk_template_repeat_threshold: 3,
            risk_template_hard_reject_threshold: 6,
            risk_template_penalty_score: 8,
            risk_uri_penalty_score: 8,
            risk_concentration_penalty_score: 8,
            risk_liquidity_penalty_score: 6,
            risk_creator_funder_penalty_score: 7,
            risk_penalty_cap: 18,
            scanner_idle_timeout_secs: 0,
            scanner_catchup_window_ms: 120_000,
            scanner_catchup_max_events: 1024,
            scanner_failover_stale_ms: 15_000,
            execution_feedback_window_secs: 300,
            execution_feedback_refresh_secs: 15,
            creator_gate_timeout_ms: 1_500,
            creator_min_wallet_age_days: 1,
            creator_fresh_wallet_token_limit: 2,
            execution_enabled: false,
            scanner_live_tokens_file: String::new(),
            scanned_tokens_file: String::new(),
            passed_tokens_file: String::new(),
            keypair: std::sync::Arc::new(keypair.insecure_clone()),
            pubkey: keypair.pubkey(),
            target_wallets: Vec::new(),
            consensus_min_wallets: 0,
            consensus_timeout_secs: 0,
            buy_sol_amount: 0.0,
            slippage_bps: 0,
            sell_slippage_bps: 0,
            compute_units: 0,
            priority_fee_micro_lamport: 0,
            min_target_buy_sol: 0.0,
            jito_enabled: false,
            jito_block_engine_urls: Vec::new(),
            jito_buy_tip_lamports: 0,
            jito_sell_tip_lamports: 0,
            jito_auth_uuid: None,
            zero_slot_urls: Vec::new(),
            zero_slot_tip_lamports: 0,
            confirm_timeout_secs: 0,
            auto_sell_enabled: false,
            take_profit_percent: 0.0,
            stop_loss_percent: 0.0,
            trailing_stop_percent: 0.0,
            max_hold_seconds: 0,
            price_check_interval_secs: 0,
            default_sol_usd_price: 0.0,
            telegram_bot_token: None,
            telegram_chat_id: None,
        }
    }

    #[test]
    fn gate3_prefers_fast_path_inside_micro_window() {
        let cfg = base_config();
        let stats = WindowStats {
            mode: SmartMoneyMode::EarlyBuyerFallback,
            fast_threshold: 2,
            soft_threshold: 4,
            unique_sm_wallets: ["a".to_string(), "b".to_string()].into_iter().collect(),
            sm_sol_total: 1.2,
            total_eligible_sol: 1.2,
            fastest_sm_ms: Some(100),
            fast_reached_at_ms: Some(900),
            soft_reached_at_ms: None,
            buy_count: 2,
            eligible_buyers: 2,
            unique_funders: 0,
            creator_funder_match_count: 0,
            creator_funder_match_sol: 0.0,
            max_single_buyer_share: 0.55,
            max_single_buyer: Some("a".to_string()),
            creator_buy_count: 0,
            creator_buy_sol: 0.0,
            largest_funder_cluster_size: 0,
            largest_funder_cluster_share: 0.0,
            hotlist_funder_hits: 0,
            hotlist_funder_diversity: 0,
            elapsed_ms: 900,
        };
        let trigger = gate3_trigger_from_stats(&cfg, &stats).expect("fast trigger");
        assert_eq!(trigger.path, Gate3Path::Fast);
        assert_eq!(trigger.threshold, 2);
    }

    #[test]
    fn gate3_uses_soft_threshold_after_fast_window() {
        let cfg = base_config();
        let stats = WindowStats {
            mode: SmartMoneyMode::EarlyBuyerFallback,
            fast_threshold: 2,
            soft_threshold: 3,
            unique_sm_wallets: [
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ]
            .into_iter()
            .collect(),
            sm_sol_total: 2.1,
            total_eligible_sol: 2.1,
            fastest_sm_ms: Some(100),
            fast_reached_at_ms: Some(900),
            soft_reached_at_ms: Some(2_000),
            buy_count: 4,
            eligible_buyers: 4,
            unique_funders: 0,
            creator_funder_match_count: 0,
            creator_funder_match_sol: 0.0,
            max_single_buyer_share: 0.35,
            max_single_buyer: Some("a".to_string()),
            creator_buy_count: 0,
            creator_buy_sol: 0.0,
            largest_funder_cluster_size: 0,
            largest_funder_cluster_share: 0.0,
            hotlist_funder_hits: 0,
            hotlist_funder_diversity: 0,
            elapsed_ms: 2_000,
        };
        let trigger = gate3_trigger_from_stats(&cfg, &stats).expect("soft trigger");
        assert_eq!(trigger.path, Gate3Path::Soft);
        assert_eq!(trigger.threshold, 3);
    }

    #[test]
    fn gate3_requires_sol_threshold_for_fast_path() {
        let cfg = base_config();
        let stats = WindowStats {
            mode: SmartMoneyMode::EarlyBuyerFallback,
            fast_threshold: 2,
            soft_threshold: 3,
            unique_sm_wallets: ["a".to_string(), "b".to_string()].into_iter().collect(),
            sm_sol_total: 0.6,
            total_eligible_sol: 0.6,
            fastest_sm_ms: Some(90),
            fast_reached_at_ms: None,
            soft_reached_at_ms: None,
            buy_count: 2,
            eligible_buyers: 2,
            unique_funders: 0,
            creator_funder_match_count: 0,
            creator_funder_match_sol: 0.0,
            max_single_buyer_share: 0.55,
            max_single_buyer: Some("a".to_string()),
            creator_buy_count: 0,
            creator_buy_sol: 0.0,
            largest_funder_cluster_size: 0,
            largest_funder_cluster_share: 0.0,
            hotlist_funder_hits: 0,
            hotlist_funder_diversity: 0,
            elapsed_ms: 700,
        };
        assert!(!gate3_fast_ready(&cfg, &stats));
        assert!(gate3_trigger_from_stats(&cfg, &stats).is_none());
    }

    #[test]
    fn gate3_quality_cluster_relaxes_fast_threshold() {
        let cfg = base_config();
        let stats = WindowStats {
            mode: SmartMoneyMode::Hotlist,
            fast_threshold: 3,
            soft_threshold: 3,
            unique_sm_wallets: ["a".to_string(), "b".to_string()].into_iter().collect(),
            sm_sol_total: 0.40,
            total_eligible_sol: 0.40,
            fastest_sm_ms: Some(80),
            fast_reached_at_ms: Some(320),
            soft_reached_at_ms: None,
            buy_count: 2,
            eligible_buyers: 2,
            unique_funders: 3,
            creator_funder_match_count: 0,
            creator_funder_match_sol: 0.0,
            max_single_buyer_share: 0.52,
            max_single_buyer: Some("a".to_string()),
            creator_buy_count: 0,
            creator_buy_sol: 0.0,
            largest_funder_cluster_size: 1,
            largest_funder_cluster_share: 0.50,
            hotlist_funder_hits: 2,
            hotlist_funder_diversity: 2,
            elapsed_ms: 320,
        };
        let trigger = gate3_trigger_from_stats(&cfg, &stats).expect("quality cluster fast trigger");
        assert_eq!(trigger.path, Gate3Path::Fast);
        assert_eq!(trigger.threshold, 2);
    }

    #[test]
    fn gate3_same_cluster_pressure_blocks_fast_path() {
        let cfg = base_config();
        let stats = WindowStats {
            mode: SmartMoneyMode::EarlyBuyerFallback,
            fast_threshold: 2,
            soft_threshold: 3,
            unique_sm_wallets: ["a".to_string(), "b".to_string(), "c".to_string()]
                .into_iter()
                .collect(),
            sm_sol_total: 1.40,
            total_eligible_sol: 1.40,
            fastest_sm_ms: Some(50),
            fast_reached_at_ms: Some(120),
            soft_reached_at_ms: Some(220),
            buy_count: 3,
            eligible_buyers: 3,
            unique_funders: 1,
            creator_funder_match_count: 0,
            creator_funder_match_sol: 0.0,
            max_single_buyer_share: 0.45,
            max_single_buyer: Some("a".to_string()),
            creator_buy_count: 0,
            creator_buy_sol: 0.0,
            largest_funder_cluster_size: 3,
            largest_funder_cluster_share: 1.0,
            hotlist_funder_hits: 0,
            hotlist_funder_diversity: 0,
            elapsed_ms: 220,
        };
        let trigger = gate3_trigger_from_stats(&cfg, &stats).expect("soft trigger");
        assert_eq!(trigger.path, Gate3Path::Soft);
        assert_eq!(trigger.threshold, 3);
    }

    #[test]
    fn gate3_rejects_creator_self_buy() {
        let cfg = base_config();
        let candidate = Candidate {
            token: NewToken {
                mint: "mint".to_string(),
                name: "name".to_string(),
                symbol: "sym".to_string(),
                uri: String::new(),
                creator: "creator".to_string(),
                bonding_curve: String::new(),
                signature: String::new(),
                slot: 0,
                detected_at_ms: 0,
                instruction_data: Vec::new(),
                instruction_accounts: Vec::new(),
                feed_source: "test".to_string(),
                is_v2: true,
            },
            created_at: Instant::now(),
            detected_at_ms: 0,
            status: CandidateStatus::Active,
            gate1_risk: Gate1RiskProfile::default(),
            narrative_keywords: Vec::new(),
            dynamic_narrative_keywords: Vec::new(),
            narrative_keyword_matches: Vec::new(),
            early_buys: Vec::new(),
            buy_signatures: HashSet::new(),
            creator_profile: None,
            buyer_profiles: HashMap::new(),
            pending_buyer_profiles: HashSet::new(),
            trace: CandidateTrace::default(),
        };
        let stats = WindowStats {
            mode: SmartMoneyMode::EarlyBuyerFallback,
            fast_threshold: 2,
            soft_threshold: 2,
            unique_sm_wallets: ["creator".to_string()].into_iter().collect(),
            sm_sol_total: 3.00,
            total_eligible_sol: 3.00,
            fastest_sm_ms: Some(30),
            fast_reached_at_ms: None,
            soft_reached_at_ms: None,
            buy_count: 1,
            eligible_buyers: 1,
            unique_funders: 0,
            creator_funder_match_count: 0,
            creator_funder_match_sol: 0.0,
            max_single_buyer_share: 1.0,
            max_single_buyer: Some("creator".to_string()),
            creator_buy_count: 1,
            creator_buy_sol: 3.00,
            largest_funder_cluster_size: 0,
            largest_funder_cluster_share: 0.0,
            hotlist_funder_hits: 0,
            hotlist_funder_diversity: 0,
            elapsed_ms: 100,
        };
        let reason =
            gate3_reject_reason(&candidate, &stats, &cfg).expect("creator self-buy reject");
        assert!(reason.contains("creator self-buy"));
    }

    #[test]
    fn gate3_allows_small_creator_bootstrap_buy() {
        let cfg = base_config();
        let candidate = Candidate {
            token: NewToken {
                mint: "mint".to_string(),
                name: "name".to_string(),
                symbol: "sym".to_string(),
                uri: String::new(),
                creator: "creator".to_string(),
                bonding_curve: String::new(),
                signature: String::new(),
                slot: 0,
                detected_at_ms: 0,
                instruction_data: Vec::new(),
                instruction_accounts: Vec::new(),
                feed_source: "test".to_string(),
                is_v2: true,
            },
            created_at: Instant::now(),
            detected_at_ms: 0,
            status: CandidateStatus::Active,
            gate1_risk: Gate1RiskProfile::default(),
            narrative_keywords: Vec::new(),
            dynamic_narrative_keywords: Vec::new(),
            narrative_keyword_matches: Vec::new(),
            early_buys: Vec::new(),
            buy_signatures: HashSet::new(),
            creator_profile: None,
            buyer_profiles: HashMap::new(),
            pending_buyer_profiles: HashSet::new(),
            trace: CandidateTrace::default(),
        };
        let stats = WindowStats {
            mode: SmartMoneyMode::EarlyBuyerFallback,
            fast_threshold: 2,
            soft_threshold: 2,
            unique_sm_wallets: ["creator".to_string(), "other".to_string()]
                .into_iter()
                .collect(),
            sm_sol_total: 0.60,
            total_eligible_sol: 0.60,
            fastest_sm_ms: Some(30),
            fast_reached_at_ms: Some(300),
            soft_reached_at_ms: Some(300),
            buy_count: 2,
            eligible_buyers: 2,
            unique_funders: 0,
            creator_funder_match_count: 0,
            creator_funder_match_sol: 0.0,
            max_single_buyer_share: 0.66,
            max_single_buyer: Some("other".to_string()),
            creator_buy_count: 1,
            creator_buy_sol: 0.10,
            largest_funder_cluster_size: 0,
            largest_funder_cluster_share: 0.0,
            hotlist_funder_hits: 0,
            hotlist_funder_diversity: 0,
            elapsed_ms: 300,
        };
        assert!(gate3_reject_reason(&candidate, &stats, &cfg).is_none());
    }

    #[test]
    fn gate3_allows_creator_seed_with_strong_external_support() {
        let cfg = base_config();
        let candidate = Candidate {
            token: NewToken {
                mint: "mint".to_string(),
                name: "name".to_string(),
                symbol: "sym".to_string(),
                uri: String::new(),
                creator: "creator".to_string(),
                bonding_curve: String::new(),
                signature: String::new(),
                slot: 0,
                detected_at_ms: 0,
                instruction_data: Vec::new(),
                instruction_accounts: Vec::new(),
                feed_source: "test".to_string(),
                is_v2: true,
            },
            created_at: Instant::now(),
            detected_at_ms: 0,
            status: CandidateStatus::Active,
            gate1_risk: Gate1RiskProfile::default(),
            narrative_keywords: Vec::new(),
            dynamic_narrative_keywords: Vec::new(),
            narrative_keyword_matches: Vec::new(),
            early_buys: Vec::new(),
            buy_signatures: HashSet::new(),
            creator_profile: None,
            buyer_profiles: HashMap::new(),
            pending_buyer_profiles: HashSet::new(),
            trace: CandidateTrace::default(),
        };
        let stats = WindowStats {
            mode: SmartMoneyMode::EarlyBuyerFallback,
            fast_threshold: 2,
            soft_threshold: 2,
            unique_sm_wallets: [
                "creator".to_string(),
                "other1".to_string(),
                "other2".to_string(),
                "other3".to_string(),
            ]
            .into_iter()
            .collect(),
            sm_sol_total: 4.10,
            total_eligible_sol: 4.10,
            fastest_sm_ms: Some(20),
            fast_reached_at_ms: Some(80),
            soft_reached_at_ms: Some(80),
            buy_count: 4,
            eligible_buyers: 4,
            unique_funders: 0,
            creator_funder_match_count: 0,
            creator_funder_match_sol: 0.0,
            max_single_buyer_share: 0.36,
            max_single_buyer: Some("creator".to_string()),
            creator_buy_count: 1,
            creator_buy_sol: 1.10,
            largest_funder_cluster_size: 0,
            largest_funder_cluster_share: 0.0,
            hotlist_funder_hits: 0,
            hotlist_funder_diversity: 0,
            elapsed_ms: 100,
        };
        assert!(gate3_reject_reason(&candidate, &stats, &cfg).is_none());
    }

    #[test]
    fn gate3_reject_path_marks_creator_self_buy() {
        assert_eq!(
            gate3_reject_path("gate3 reject: creator self-buy detected"),
            "creator_self_buy"
        );
    }

    #[test]
    fn gate3_rejects_creator_linked_cluster_concentration() {
        let cfg = base_config();
        let candidate = Candidate {
            token: NewToken {
                mint: "mint".to_string(),
                name: "name".to_string(),
                symbol: "sym".to_string(),
                uri: String::new(),
                creator: "creator".to_string(),
                bonding_curve: String::new(),
                signature: String::new(),
                slot: 0,
                detected_at_ms: 0,
                instruction_data: Vec::new(),
                instruction_accounts: Vec::new(),
                feed_source: "test".to_string(),
                is_v2: true,
            },
            created_at: Instant::now(),
            detected_at_ms: 0,
            status: CandidateStatus::Active,
            gate1_risk: Gate1RiskProfile::default(),
            narrative_keywords: Vec::new(),
            dynamic_narrative_keywords: Vec::new(),
            narrative_keyword_matches: Vec::new(),
            early_buys: Vec::new(),
            buy_signatures: HashSet::new(),
            creator_profile: Some(CreatorProfile {
                address: "creator".to_string(),
                total_tokens: 1,
                graduated: 0,
                rug_count: 0,
                oldest_tx_ms: 0,
                wallet_age_days: 0,
                first_funder: Some("funder-a".to_string()),
                fetched_at_ms: 0,
            }),
            buyer_profiles: HashMap::new(),
            pending_buyer_profiles: HashSet::new(),
            trace: CandidateTrace::default(),
        };
        let stats = WindowStats {
            mode: SmartMoneyMode::EarlyBuyerFallback,
            fast_threshold: 2,
            soft_threshold: 2,
            unique_sm_wallets: ["buyer-1".to_string(), "buyer-2".to_string()]
                .into_iter()
                .collect(),
            sm_sol_total: 1.40,
            total_eligible_sol: 1.50,
            fastest_sm_ms: Some(20),
            fast_reached_at_ms: Some(120),
            soft_reached_at_ms: Some(120),
            buy_count: 2,
            eligible_buyers: 2,
            unique_funders: 1,
            creator_funder_match_count: 2,
            creator_funder_match_sol: 1.35,
            max_single_buyer_share: 0.51,
            max_single_buyer: Some("buyer-1".to_string()),
            creator_buy_count: 0,
            creator_buy_sol: 0.0,
            largest_funder_cluster_size: 2,
            largest_funder_cluster_share: 0.9,
            hotlist_funder_hits: 0,
            hotlist_funder_diversity: 0,
            elapsed_ms: 120,
        };
        let reason =
            gate3_reject_reason(&candidate, &stats, &cfg).expect("creator-linked cluster reject");
        assert!(reason.contains("creator-linked cluster"));
    }

    #[test]
    fn narrative_keywords_use_token_boundaries() {
        let token = NewToken {
            mint: "mint".to_string(),
            name: "Paid in Full".to_string(),
            symbol: "PAID".to_string(),
            uri: String::new(),
            creator: "creator".to_string(),
            bonding_curve: String::new(),
            signature: String::new(),
            slot: 0,
            detected_at_ms: 0,
            instruction_data: Vec::new(),
            instruction_accounts: Vec::new(),
            feed_source: "test".to_string(),
            is_v2: true,
        };
        let mut dynamic = HashMap::new();
        dynamic.insert(
            "full".to_string(),
            NarrativeHotTerm {
                tier: NarrativeTier::Confirmed,
                score: 5,
                source_mask: NARRATIVE_MASK_ONCHAIN,
            },
        );
        let (all_keywords, dynamic_keywords, matches) =
            collect_narrative_keywords(&token, &dynamic);
        assert!(!all_keywords.iter().any(|kw| kw == "ai"));
        assert!(dynamic_keywords.iter().any(|kw| kw == "full"));
        assert!(matches
            .iter()
            .any(|item| item.keyword == "full" && item.tier == NarrativeTier::Confirmed));
    }

    #[test]
    fn dynamic_keyword_stopwords_are_filtered() {
        let keywords = extract_dynamic_keywords_from_texts(vec![
            "The Official Solana Coin".to_string(),
            "Agent Pepe AI".to_string(),
        ]);
        assert!(!keywords.iter().any(|kw| kw == "the"));
        assert!(!keywords.iter().any(|kw| kw == "official"));
        assert!(keywords.iter().any(|kw| kw == "agent"));
        assert!(keywords.iter().any(|kw| kw == "pepe"));
        assert!(keywords.iter().any(|kw| kw == "ai"));
    }

    #[test]
    fn buyer_matches_hotlist_by_address_or_funder() {
        let mut hotlists = HotLists::default();
        hotlists.smart_money.insert("smart".to_string());
        hotlists.smart_money_funders.insert("funder".to_string());

        assert!(buyer_matches_hotlist("smart", None, &hotlists));
        assert!(buyer_matches_hotlist(
            "fresh",
            Some(&BuyerProfile {
                address: "fresh".to_string(),
                oldest_tx_ms: 0,
                wallet_age_days: 0,
                first_funder: Some("funder".to_string()),
                fetched_at_ms: 0,
            }),
            &hotlists,
        ));
        assert!(!buyer_matches_hotlist("other", None, &hotlists));
    }

    #[test]
    fn upgrade_existing_buy_prefers_processed_signal_strength() {
        let buyer = Pubkey::new_unique();
        let token_program = Pubkey::new_unique();
        let mut existing = PumpBuyEvent {
            mint: "mint".to_string(),
            buyer,
            feed_source: "deshred_pre_exec".to_string(),
            token_program,
            sol_amount_lamports: 100_000_000,
            instruction_data: vec![1],
            instruction_accounts: vec![buyer],
            signature: "sig".to_string(),
            slot: 1,
            detected_at_ms: 1,
            detected_at: Instant::now(),
        };
        let incoming = PumpBuyEvent {
            mint: "mint".to_string(),
            buyer,
            feed_source: "primary_processed".to_string(),
            token_program,
            sol_amount_lamports: 350_000_000,
            instruction_data: vec![2],
            instruction_accounts: vec![buyer, token_program],
            signature: "sig".to_string(),
            slot: 1,
            detected_at_ms: 2,
            detected_at: Instant::now(),
        };

        assert!(upgrade_existing_buy_event(&mut existing, &incoming));
        assert_eq!(existing.feed_source, "primary_processed");
        assert_eq!(existing.sol_amount_lamports, 350_000_000);
        assert_eq!(existing.instruction_accounts.len(), 2);
    }
}
