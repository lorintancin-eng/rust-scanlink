use crate::config::AppConfig;
use crate::filter::db::{
    BuyerProfile, CreatorProfile, FilterDb, FilterResultRecord, FilterTimingRecord,
};
use crate::processor::pumpfun::BondingCurveState;
use crate::scanner::{
    NewToken, PumpBuyEvent, ScannerEvent, DISC_CREATE, DISC_CREATE_V2, PUMP_PROGRAM_ID,
};
use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use solana_client::rpc_client::RpcClient;
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
const FALLBACK_SM_THRESHOLD: usize = 4;
const DAY_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Clone)]
pub struct BuySignal {
    pub token: NewToken,
    pub score: u32,
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
    discovered_at_ms: u64,
    status: CandidateStatus,
    narrative_keywords: Vec<String>,
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
    fastest_sm_ms: Option<u64>,
    buy_count: usize,
    eligible_buyers: usize,
    elapsed_ms: u64,
}

#[derive(Debug, Clone, Default)]
struct AddressSnapshot {
    oldest_tx_ms: u64,
    wallet_age_days: u32,
    first_funder: Option<String>,
}

#[derive(Clone)]
struct SharedState {
    config: Arc<AppConfig>,
    rpc_client: Arc<RpcClient>,
    http: reqwest::Client,
    db: FilterDb,
    hotlists: Arc<RwLock<HotLists>>,
}

pub async fn run(
    config: Arc<AppConfig>,
    rpc_client: Arc<RpcClient>,
    mut scanner_rx: mpsc::Receiver<ScannerEvent>,
    buy_signal_tx: mpsc::Sender<BuySignal>,
) -> Result<()> {
    let db = FilterDb::new(&config.filter_db_path).await?;
    let shared = SharedState {
        config: config.clone(),
        rpc_client,
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .context("filter http client init failed")?,
        db,
        hotlists: Arc::new(RwLock::new(HotLists::default())),
    };
    reload_hotlists(&shared).await?;

    let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<InternalMessage>();
    let mut candidates: HashMap<String, Candidate> = HashMap::new();
    let mut creator_window: HashMap<String, VecDeque<u64>> = HashMap::new();

    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut hot_reload = tokio::time::interval(Duration::from_secs(config.filter_hot_reload_secs));
    hot_reload.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe_event = scanner_rx.recv() => {
                let Some(event) = maybe_event else { break; };
                match event {
                    ScannerEvent::NewToken(token) => {
                        handle_new_token(&shared, &internal_tx, &mut candidates, &mut creator_window, token).await?;
                    }
                    ScannerEvent::Buy(buy) => {
                        handle_buy_event(&shared, &internal_tx, &mut candidates, buy).await;
                    }
                }
            }
            maybe_msg = internal_rx.recv() => {
                let Some(msg) = maybe_msg else { break; };
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
                                    "detected_at_ms": signal.token.discovered_at_ms,
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
            _ = tick.tick() => {
                let mut expired = Vec::new();
                for (mint, candidate) in &candidates {
                    if candidate.status != CandidateStatus::Active {
                        continue;
                    }
                    let stats = smart_money_stats(candidate, &shared).await;
                    if let Some(reason) = gate3_reject_reason(candidate, &stats, &shared.config) {
                        expired.push((
                            mint.clone(),
                            reason,
                            smart_money_mode_label(stats.mode).to_string(),
                            "timeout".to_string(),
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
            _ = hot_reload.tick() => {
                if let Err(err) = reload_hotlists(&shared).await {
                    warn!("filter: hot reload failed: {}", err);
                }
            }
        }
    }

    Ok(())
}

struct Gate1Decision {
    passed: bool,
    reason: String,
    narrative_keywords: Vec<String>,
}

async fn handle_new_token(
    shared: &SharedState,
    internal_tx: &mpsc::UnboundedSender<InternalMessage>,
    candidates: &mut HashMap<String, Candidate>,
    creator_window: &mut HashMap<String, VecDeque<u64>>,
    token: NewToken,
) -> Result<()> {
    if candidates.contains_key(&token.mint) {
        return Ok(());
    }

    if let Err(err) = append_jsonl(
        &shared.config.scanned_tokens_file,
        &json!({
            "detected_at_ms": token.discovered_at_ms,
            "mint": &token.mint,
            "symbol": &token.symbol,
            "name": &token.name,
            "creator": &token.creator,
            "bonding_curve": &token.bonding_curve,
            "signature": &token.signature,
            "slot": token.slot,
            "uri": &token.uri,
            "is_v2": token.is_v2,
        }),
    )
    .await
    {
        warn!("scanned_tokens append failed: {}", err);
    }

    let gate1 = gate1_check(shared, creator_window, &token).await;
    if !gate1.passed {
        let trace = CandidateTrace {
            gate1_at_ms: Some(now_ms()),
            ..Default::default()
        };
        record_token_outcome(
            shared,
            &token,
            &trace,
            false,
            Some("gate1".to_string()),
            None,
            gate1.reason,
            "gate1".to_string(),
            "immediate".to_string(),
            0,
            0,
        )
        .await;
        return Ok(());
    }

    let gate1_at_ms = now_ms();
    candidates.insert(
        token.mint.clone(),
        Candidate {
            token: token.clone(),
            created_at: Instant::now(),
            discovered_at_ms: token.discovered_at_ms,
            status: CandidateStatus::CreatorPending,
            narrative_keywords: gate1.narrative_keywords,
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
    let Some(candidate) = candidates.get_mut(&buy.mint) else {
        return;
    };
    if candidate.status == CandidateStatus::Finalizing
        || candidate.buy_signatures.contains(&buy.signature)
    {
        return;
    }
    if candidate.early_buys.len() >= shared.config.smart_money_max_buys {
        return;
    }
    if buy
        .detected_at
        .duration_since(candidate.created_at)
        .as_millis() as u64
        > hard_window_ms(&shared.config)
    {
        return;
    }

    candidate.buy_signatures.insert(buy.signature.clone());
    candidate.early_buys.push(buy.clone());
    candidate.trace.gate3_open_at_ms.get_or_insert_with(now_ms);

    maybe_spawn_buyer_profile_fetch(shared, internal_tx, candidate, &buy);

    if candidate.status == CandidateStatus::Active {
        if let Some(trigger) = should_finalize(candidate, shared).await {
            candidate.status = CandidateStatus::Finalizing;
            candidate.trace.gate3_trigger_at_ms = Some(now_ms());
            candidate.trace.path = Some(gate3_path_label(trigger.path).to_string());
            spawn_score_task(shared, internal_tx, candidate.clone());
        }
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

async fn gate1_check(
    shared: &SharedState,
    creator_window: &mut HashMap<String, VecDeque<u64>>,
    token: &NewToken,
) -> Gate1Decision {
    let hotlists = shared.hotlists.read().await;
    if hotlists.creator_blacklist.contains(&token.creator) {
        return Gate1Decision {
            passed: false,
            reason: "gate1 reject: creator blacklist hit".to_string(),
            narrative_keywords: Vec::new(),
        };
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
        return Gate1Decision {
            passed: false,
            reason: format!(
                "gate1 reject: factory creator pattern ({} launches in 5m)",
                window.len()
            ),
            narrative_keywords: Vec::new(),
        };
    }

    if token.name.trim().is_empty() || token.symbol.trim().is_empty() {
        return Gate1Decision {
            passed: false,
            reason: "gate1 reject: empty name or symbol".to_string(),
            narrative_keywords: Vec::new(),
        };
    }
    if token.symbol.chars().count() > 10 {
        return Gate1Decision {
            passed: false,
            reason: format!(
                "gate1 reject: symbol too long ({})",
                token.symbol.chars().count()
            ),
            narrative_keywords: Vec::new(),
        };
    }

    let lower = format!(
        "{} {}",
        token.name.to_lowercase(),
        token.symbol.to_lowercase()
    );
    if let Some(keyword) = GATE1_BLACK_KEYWORDS.iter().find(|kw| lower.contains(**kw)) {
        return Gate1Decision {
            passed: false,
            reason: format!("gate1 reject: blacklist keyword {}", keyword),
            narrative_keywords: Vec::new(),
        };
    }

    let narrative_keywords = GATE1_WHITE_KEYWORDS
        .iter()
        .filter(|kw| lower.contains(**kw))
        .map(|kw| (*kw).to_string())
        .collect();

    Gate1Decision {
        passed: true,
        reason: "gate1 pass".to_string(),
        narrative_keywords,
    }
}

async fn creator_gate(shared: &SharedState, token: &NewToken) -> Result<CreatorGateResult> {
    let cached = shared.db.get_creator_profile(&token.creator).await?;
    if let Some(profile) = cached.as_ref() {
        if now_ms().saturating_sub(profile.fetched_at_ms) <= CREATOR_CACHE_TTL_MS {
            return Ok(apply_creator_rules(shared.config.as_ref(), profile.clone()));
        }
    }

    let Some(api_key) = shared.config.helius_api_key.as_deref() else {
        return Ok(CreatorGateResult {
            passed: true,
            reason: "gate2 pass: HELIUS_API_KEY not configured".to_string(),
            profile: cached,
        });
    };

    let mints = fetch_creator_mints(shared, api_key, &token.creator).await?;
    let stale_rug_count = cached.as_ref().map(|p| p.rug_count).unwrap_or_default();
    let graduated = fetch_graduated_count(shared, &mints).await?;
    let snapshot = fetch_address_snapshot(shared, api_key, &token.creator).await?;
    let profile = CreatorProfile {
        address: token.creator.clone(),
        total_tokens: mints.len() as u32,
        graduated,
        rug_count: stale_rug_count,
        oldest_tx_ms: snapshot.oldest_tx_ms,
        wallet_age_days: snapshot.wallet_age_days,
        first_funder: snapshot.first_funder,
        fetched_at_ms: now_ms(),
    };
    shared.db.upsert_creator_profile(&profile).await?;
    Ok(apply_creator_rules(shared.config.as_ref(), profile))
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
    let hard = hard_window_ms(config);
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

fn hard_window_ms(config: &AppConfig) -> u64 {
    config.smart_money_window_secs.saturating_mul(1000)
}

fn smart_money_mode_label(mode: SmartMoneyMode) -> &'static str {
    match mode {
        SmartMoneyMode::Hotlist => "address_or_funder_hotlist",
        SmartMoneyMode::EarlyBuyerFallback => "early_buyers_fallback",
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
    gate3_trigger_from_stats(shared.config.as_ref(), &stats)
}

fn gate3_trigger_from_stats(config: &AppConfig, stats: &WindowStats) -> Option<Gate3Trigger> {
    if stats.unique_sm_wallets.len() >= stats.fast_threshold
        && stats.elapsed_ms <= effective_fast_window_ms(config)
    {
        return Some(Gate3Trigger {
            path: Gate3Path::Fast,
            threshold: stats.fast_threshold,
        });
    }
    if stats.unique_sm_wallets.len() >= stats.soft_threshold
        && stats.elapsed_ms <= effective_soft_window_ms(config)
    {
        return Some(Gate3Trigger {
            path: Gate3Path::Soft,
            threshold: stats.soft_threshold,
        });
    }
    None
}

fn gate3_reject_reason(
    candidate: &Candidate,
    stats: &WindowStats,
    config: &AppConfig,
) -> Option<String> {
    if stats.elapsed_ms > effective_soft_window_ms(config) {
        return Some(format!(
            "gate3 reject: window closed | mode={} | matched={} | threshold={} | first_buys={} | elapsed_ms={}",
            smart_money_mode_label(stats.mode),
            stats.unique_sm_wallets.len(),
            stats.soft_threshold,
            stats.buy_count,
            stats.elapsed_ms,
        ));
    }
    if candidate.early_buys.len() >= config.smart_money_max_buys
        && stats.unique_sm_wallets.len() < stats.soft_threshold
    {
        return Some(format!(
            "gate3 reject: max buys reached | mode={} | matched={} | threshold={} | first_buys={}",
            smart_money_mode_label(stats.mode),
            stats.unique_sm_wallets.len(),
            stats.soft_threshold,
            stats.buy_count,
        ));
    }
    None
}

async fn smart_money_stats(candidate: &Candidate, shared: &SharedState) -> WindowStats {
    let hotlists = shared.hotlists.read().await;
    let mode = if hotlists.smart_money.is_empty() && hotlists.smart_money_funders.is_empty() {
        SmartMoneyMode::EarlyBuyerFallback
    } else {
        SmartMoneyMode::Hotlist
    };
    let fast_threshold = effective_fast_threshold(&shared.config, mode);
    let soft_threshold = effective_soft_threshold(&shared.config, mode);
    let mut unique_sm_wallets = HashSet::new();
    let mut eligible_buyers = HashSet::new();
    let mut sm_sol_total = 0.0f64;
    let mut fastest_sm_ms: Option<u64> = None;

    for buy in &candidate.early_buys {
        let buyer = buy.buyer.to_string();
        if hotlists.blocked_buyers.contains(&buyer) {
            continue;
        }
        eligible_buyers.insert(buyer.clone());

        let matched = match mode {
            SmartMoneyMode::Hotlist => {
                buyer_matches_hotlist(&buyer, candidate.buyer_profiles.get(&buyer), &hotlists)
            }
            SmartMoneyMode::EarlyBuyerFallback => unique_sm_wallets.insert(buyer.clone()),
        };
        if !matched {
            continue;
        }

        unique_sm_wallets.insert(buyer);
        sm_sol_total += buy.sol_amount_lamports as f64 / 1e9;
        let elapsed_ms = buy
            .detected_at
            .saturating_duration_since(candidate.created_at)
            .as_millis() as u64;
        fastest_sm_ms = Some(match fastest_sm_ms {
            Some(current) => current.min(elapsed_ms),
            None => elapsed_ms,
        });
    }

    WindowStats {
        mode,
        fast_threshold,
        soft_threshold,
        unique_sm_wallets,
        sm_sol_total,
        fastest_sm_ms,
        buy_count: candidate.early_buys.len(),
        eligible_buyers: eligible_buyers.len(),
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

async fn score_candidate(shared: &SharedState, mut candidate: Candidate) -> ScoreDecision {
    let stats = smart_money_stats(&candidate, shared).await;
    let Some(trigger) = gate3_trigger_from_stats(shared.config.as_ref(), &stats) else {
        return ScoreDecision {
            passed: false,
            gate: "gate3".to_string(),
            score: 0,
            reason: format!(
                "gate3 reject: below threshold | mode={} | matched={} | fast_threshold={} | soft_threshold={} | first_buys={}",
                smart_money_mode_label(stats.mode),
                stats.unique_sm_wallets.len(),
                stats.fast_threshold,
                stats.soft_threshold,
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

    let sm_count_score = match stats.unique_sm_wallets.len() {
        0 => 0,
        1 => 10,
        2 => 20,
        _ => 30,
    };
    let sm_sol_score = if stats.sm_sol_total >= 2.0 {
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
    let buyer_quality_pct = fetch_buyer_quality_pct(shared, &candidate)
        .await
        .unwrap_or(0.0);
    let buyer_quality_score = (buyer_quality_pct * 15.0).round().clamp(0.0, 15.0) as u32;
    let total_score =
        sm_count_score + sm_sol_score + momentum_score + curve_score + buyer_quality_score;
    let reason = format!(
        "mode={} path={} participants={} capital={} momentum={} curve={} buyer_quality={} total={} | matched={} eligible={} sol={:.2} fastest={}ms narrative={}",
        smart_money_mode_label(stats.mode),
        gate3_path_label(trigger.path),
        sm_count_score,
        sm_sol_score,
        momentum_score,
        curve_score,
        buyer_quality_score,
        total_score,
        stats.unique_sm_wallets.len(),
        stats.eligible_buyers,
        stats.sm_sol_total,
        stats.fastest_sm_ms.unwrap_or_default(),
        if candidate.narrative_keywords.is_empty() {
            "-".to_string()
        } else {
            candidate.narrative_keywords.join("|")
        }
    );

    if total_score < shared.config.filter_min_score {
        return ScoreDecision {
            passed: false,
            gate: "gate4".to_string(),
            score: total_score,
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
            score: total_score,
            reason: format!("{} | missing trigger buy context", reason),
            signal: None,
            mode: smart_money_mode_label(stats.mode).to_string(),
            path: gate3_path_label(trigger.path).to_string(),
            matched_buyers: stats.unique_sm_wallets.len(),
            early_buy_count: stats.buy_count,
            gate4_at_ms: candidate.trace.gate4_at_ms,
        };
    };

    let latency_ms = now_ms().saturating_sub(candidate.discovered_at_ms);
    ScoreDecision {
        passed: true,
        gate: "pass".to_string(),
        score: total_score,
        reason: reason.clone(),
        signal: Some(BuySignal {
            token: candidate.token,
            score: total_score,
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
        !(hotlists.smart_money.is_empty() && hotlists.smart_money_funders.is_empty());
    if !hotlist_mode {
        return candidate
            .early_buys
            .iter()
            .find(|buy| !hotlists.blocked_buyers.contains(&buy.buyer.to_string()))
            .cloned();
    }

    candidate
        .early_buys
        .iter()
        .find(|buy| {
            let buyer = buy.buyer.to_string();
            buyer_matches_hotlist(&buyer, candidate.buyer_profiles.get(&buyer), &hotlists)
        })
        .cloned()
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
    let url = format!(
        "https://api-mainnet.helius-rpc.com/v0/addresses/{}/transactions",
        address
    );
    let items: Vec<Value> = shared
        .http
        .get(url)
        .query(&[
            ("api-key", api_key),
            ("commitment", "confirmed"),
            ("limit", "1"),
            ("sort-order", "asc"),
        ])
        .send()
        .await
        .with_context(|| format!("Helius oldest tx query failed: {}", address))?
        .error_for_status()
        .with_context(|| format!("Helius oldest tx response invalid: {}", address))?
        .json()
        .await
        .context("Helius oldest tx json decode failed")?;

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

    Ok(AddressSnapshot {
        oldest_tx_ms,
        wallet_age_days,
        first_funder,
    })
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

async fn reload_hotlists(shared: &SharedState) -> Result<()> {
    let blacklist = load_plaintext_set(&shared.config.creator_blacklist_file).await?;
    let smart_money = load_plaintext_set(&shared.config.smart_money_file).await?;
    let smart_money_funders = load_plaintext_set(&shared.config.smart_money_funder_file).await?;
    let blocked_buyers = load_plaintext_set(&shared.config.blocked_buyers_file).await?;
    {
        let mut hotlists = shared.hotlists.write().await;
        hotlists.creator_blacklist = blacklist.iter().cloned().collect();
        hotlists.smart_money = smart_money.iter().cloned().collect();
        hotlists.smart_money_funders = smart_money_funders.iter().cloned().collect();
        hotlists.blocked_buyers = blocked_buyers.iter().cloned().collect();
    }
    shared.db.sync_blacklist(&blacklist).await?;
    shared.db.sync_smart_money(&smart_money).await?;
    info!(
        "Filter hotlists loaded | blacklist={} | smart_money={} | smart_money_funders={} | blocked_buyers={}",
        blacklist.len(),
        smart_money.len(),
        smart_money_funders.len(),
        blocked_buyers.len(),
    );
    if smart_money.is_empty() && smart_money_funders.is_empty() {
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
    )
    .await;
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
) {
    let final_at_ms = now_ms();
    let latency_ms = final_at_ms.saturating_sub(token.discovered_at_ms);

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
        detected_at_ms: token.discovered_at_ms,
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
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
            helius_api_key: None,
            filter_db_path: String::new(),
            smart_money_file: String::new(),
            smart_money_funder_file: String::new(),
            blocked_buyers_file: String::new(),
            creator_blacklist_file: String::new(),
            latency_metrics_file: String::new(),
            filter_hot_reload_secs: 0,
            smart_money_window_secs: 60,
            smart_money_fast_window_ms: 1_200,
            smart_money_soft_window_ms: 5_000,
            smart_money_fast_threshold: 2,
            smart_money_threshold: 2,
            smart_money_max_buys: 20,
            filter_min_score: 60,
            scanner_idle_timeout_secs: 0,
            creator_min_wallet_age_days: 1,
            creator_fresh_wallet_token_limit: 2,
            execution_enabled: false,
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
            sm_sol_total: 0.0,
            fastest_sm_ms: Some(100),
            buy_count: 2,
            eligible_buyers: 2,
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
            soft_threshold: 4,
            unique_sm_wallets: [
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ]
            .into_iter()
            .collect(),
            sm_sol_total: 0.0,
            fastest_sm_ms: Some(100),
            buy_count: 4,
            eligible_buyers: 4,
            elapsed_ms: 2_500,
        };
        let trigger = gate3_trigger_from_stats(&cfg, &stats).expect("soft trigger");
        assert_eq!(trigger.path, Gate3Path::Soft);
        assert_eq!(trigger.threshold, 4);
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
}
