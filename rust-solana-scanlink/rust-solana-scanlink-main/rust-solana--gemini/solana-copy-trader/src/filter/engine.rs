use crate::config::AppConfig;
use crate::filter::db::{CreatorProfile, FilterDb, FilterResultRecord};
use crate::processor::pumpfun::BondingCurveState;
use crate::scanner::{NewToken, PumpBuyEvent, ScannerEvent, DISC_CREATE, DISC_CREATE_V2, PUMP_PROGRAM_ID};
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

const GATE1_BLACK_KEYWORDS: &[&str] = &["test", "rug", "scam", "fake", "honeypot", "rugpull", "ponzi"];
const GATE1_WHITE_KEYWORDS: &[&str] = &["ai", "agent", "trump", "pepe", "maga"];
const CREATOR_CACHE_TTL_MS: u64 = 24 * 60 * 60 * 1000;
const FACTORY_WINDOW_MS: u64 = 5 * 60 * 1000;
const FACTORY_THRESHOLD: usize = 3;
const CREATOR_TOTAL_TOKEN_LIMIT: u32 = 100;
const CREATOR_RUG_LIMIT: u32 = 3;
const CURVE_TOTAL_TARGET_SOL: f64 = 69.0;
const CURVE_INITIAL_VIRTUAL_SOL: u64 = 30_000_000_000;
const OLD_WALLET_DAYS: u64 = 7;
const HELIUS_PAGE_LIMIT: usize = 100;
const HELIUS_MAX_PAGES: usize = 5;

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
}

#[derive(Debug, Clone)]
struct HotLists {
    creator_blacklist: HashSet<String>,
    smart_money: HashSet<String>,
}

#[derive(Debug)]
enum InternalMessage {
    CreatorGateResolved {
        mint: String,
        token: NewToken,
        result: CreatorGateResult,
    },
    Scored {
        mint: String,
        decision: ScoreDecision,
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
}

#[derive(Debug, Clone)]
struct WindowStats {
    unique_sm_wallets: HashSet<String>,
    sm_sol_total: f64,
    fastest_sm_ms: Option<u64>,
    buy_count: usize,
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
            .context("过滤层 HTTP 客户端初始化失败")?,
        db,
        hotlists: Arc::new(RwLock::new(HotLists {
            creator_blacklist: HashSet::new(),
            smart_money: HashSet::new(),
        })),
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
                        if !result.passed {
                            candidates.remove(&mint);
                            record_filter_result(&shared, &token, false, Some("gate2".to_string()), None, result.reason).await;
                            continue;
                        }
                        if let Some(candidate) = candidates.get_mut(&mint) {
                            candidate.status = CandidateStatus::Active;
                            candidate.creator_profile = result.profile;
                            if should_finalize(candidate, &shared).await {
                                candidate.status = CandidateStatus::Finalizing;
                                spawn_score_task(&shared, &internal_tx, candidate.clone());
                            }
                        }
                    }
                    InternalMessage::Scored { mint, decision } => {
                        if let Some(candidate) = candidates.remove(&mint) {
                            record_filter_result(
                                &shared,
                                &candidate.token,
                                decision.passed,
                                if decision.passed { None } else { Some(decision.gate.clone()) },
                                if decision.score > 0 { Some(decision.score) } else { None },
                                decision.reason.clone(),
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
                                warn!("过滤层：执行层通道已关闭");
                                break;
                            }
                        }
                    }
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                let mut expired = Vec::new();
                for (mint, candidate) in &candidates {
                    if candidate.status == CandidateStatus::Finalizing {
                        continue;
                    }
                    if now.duration_since(candidate.created_at).as_secs() >= config.smart_money_window_secs {
                        expired.push(mint.clone());
                    } else if candidate.status == CandidateStatus::Active
                        && candidate.early_buys.len() >= config.smart_money_max_buys
                        && smart_money_stats(candidate, &shared).await.unique_sm_wallets.len() < config.smart_money_threshold
                    {
                        expired.push(mint.clone());
                    }
                }

                for mint in expired {
                    if let Some(candidate) = candidates.remove(&mint) {
                        let stats = smart_money_stats(&candidate, &shared).await;
                        let reason = format!(
                            "关卡3拒绝：窗口结束 | 前{}笔买入内聪明钱={} | 阈值={}",
                            stats.buy_count,
                            stats.unique_sm_wallets.len(),
                            shared.config.smart_money_threshold
                        );
                        record_filter_result(&shared, &candidate.token, false, Some("gate3".to_string()), None, reason).await;
                    }
                }
            }
            _ = hot_reload.tick() => {
                if let Err(err) = reload_hotlists(&shared).await {
                    warn!("过滤层：热重载名单失败: {}", err);
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
    ).await {
        warn!("scanned_tokens append failed: {}", err);
    }

    let gate1 = gate1_check(shared, creator_window, &token).await;
    if !gate1.passed {
        record_filter_result(shared, &token, false, Some("gate1".to_string()), None, gate1.reason).await;
        return Ok(());
    }

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
        },
    );

    let shared_clone = shared.clone();
    let tx_clone = internal_tx.clone();
    tokio::spawn(async move {
        let result = creator_gate(&shared_clone, &token).await.unwrap_or_else(|err| CreatorGateResult {
            passed: true,
            reason: format!("关卡2降级放行：{}", err),
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

async fn handle_buy_event(
    shared: &SharedState,
    internal_tx: &mpsc::UnboundedSender<InternalMessage>,
    candidates: &mut HashMap<String, Candidate>,
    buy: PumpBuyEvent,
) {
    let Some(candidate) = candidates.get_mut(&buy.mint) else {
        return;
    };
    if candidate.status == CandidateStatus::Finalizing || candidate.buy_signatures.contains(&buy.signature) {
        return;
    }
    if candidate.early_buys.len() >= shared.config.smart_money_max_buys {
        return;
    }
    if buy.detected_at.duration_since(candidate.created_at).as_secs() > shared.config.smart_money_window_secs {
        return;
    }

    candidate.buy_signatures.insert(buy.signature.clone());
    candidate.early_buys.push(buy);

    if candidate.status == CandidateStatus::Active && should_finalize(candidate, shared).await {
        let snapshot = candidate.clone();
        candidate.status = CandidateStatus::Finalizing;
        spawn_score_task(shared, internal_tx, snapshot);
    }
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
        let _ = tx.send(InternalMessage::Scored { mint, decision });
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
            reason: "关卡1拒绝：Creator 黑名单命中".to_string(),
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
            reason: format!("关卡1拒绝：工厂号行为（5分钟内发币 {} 次）", window.len()),
            narrative_keywords: Vec::new(),
        };
    }

    if token.name.trim().is_empty() || token.symbol.trim().is_empty() {
        return Gate1Decision {
            passed: false,
            reason: "关卡1拒绝：name/symbol 为空".to_string(),
            narrative_keywords: Vec::new(),
        };
    }
    if token.symbol.chars().count() > 10 {
        return Gate1Decision {
            passed: false,
            reason: format!("关卡1拒绝：symbol 长度异常 ({})", token.symbol.chars().count()),
            narrative_keywords: Vec::new(),
        };
    }

    let lower = format!("{} {}", token.name.to_lowercase(), token.symbol.to_lowercase());
    if let Some(keyword) = GATE1_BLACK_KEYWORDS.iter().find(|kw| lower.contains(**kw)) {
        return Gate1Decision {
            passed: false,
            reason: format!("关卡1拒绝：黑名单关键词命中 {}", keyword),
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
        reason: "关卡1通过".to_string(),
        narrative_keywords,
    }
}

async fn creator_gate(shared: &SharedState, token: &NewToken) -> Result<CreatorGateResult> {
    let cached = shared.db.get_creator_profile(&token.creator).await?;
    if let Some(profile) = cached.as_ref() {
        if now_ms().saturating_sub(profile.fetched_at_ms) <= CREATOR_CACHE_TTL_MS {
            return Ok(apply_creator_rules(profile.clone()));
        }
    }

    let Some(api_key) = shared.config.helius_api_key.as_deref() else {
        return Ok(CreatorGateResult {
            passed: true,
            reason: "关卡2放行：未配置 HELIUS_API_KEY".to_string(),
            profile: cached,
        });
    };

    let mints = fetch_creator_mints(shared, api_key, &token.creator).await?;
    let stale_rug_count = cached.as_ref().map(|p| p.rug_count).unwrap_or_default();
    let graduated = fetch_graduated_count(shared, &mints).await?;
    let profile = CreatorProfile {
        address: token.creator.clone(),
        total_tokens: mints.len() as u32,
        graduated,
        rug_count: stale_rug_count,
        fetched_at_ms: now_ms(),
    };
    shared.db.upsert_creator_profile(&profile).await?;
    Ok(apply_creator_rules(profile))
}

fn apply_creator_rules(profile: CreatorProfile) -> CreatorGateResult {
    if profile.total_tokens > CREATOR_TOTAL_TOKEN_LIMIT {
        return CreatorGateResult {
            passed: false,
            reason: format!("关卡2拒绝：历史发币总数过高 ({})", profile.total_tokens),
            profile: Some(profile),
        };
    }
    if profile.total_tokens > 0 && profile.graduated == 0 {
        return CreatorGateResult {
            passed: false,
            reason: format!("关卡2拒绝：历史发币 {} 个但毕业数为 0", profile.total_tokens),
            profile: Some(profile),
        };
    }
    if profile.rug_count >= CREATOR_RUG_LIMIT {
        return CreatorGateResult {
            passed: false,
            reason: format!("关卡2拒绝：历史 Rug 次数 {}", profile.rug_count),
            profile: Some(profile),
        };
    }
    CreatorGateResult {
        passed: true,
        reason: format!(
            "关卡2通过：creator={} total={} graduated={} rug={}",
            profile.address, profile.total_tokens, profile.graduated, profile.rug_count
        ),
        profile: Some(profile),
    }
}

async fn should_finalize(candidate: &Candidate, shared: &SharedState) -> bool {
    smart_money_stats(candidate, shared).await.unique_sm_wallets.len() >= shared.config.smart_money_threshold
}

async fn smart_money_stats(candidate: &Candidate, shared: &SharedState) -> WindowStats {
    let hotlists = shared.hotlists.read().await;
    let mut unique_sm_wallets = HashSet::new();
    let mut sm_sol_total = 0.0f64;
    let mut fastest_sm_ms: Option<u64> = None;
    for buy in &candidate.early_buys {
        let buyer = buy.buyer.to_string();
        if hotlists.smart_money.contains(&buyer) {
            unique_sm_wallets.insert(buyer);
            sm_sol_total += buy.sol_amount_lamports as f64 / 1e9;
            let elapsed_ms = buy.detected_at.saturating_duration_since(candidate.created_at).as_millis() as u64;
            fastest_sm_ms = Some(match fastest_sm_ms {
                Some(current) => current.min(elapsed_ms),
                None => elapsed_ms,
            });
        }
    }
    WindowStats {
        unique_sm_wallets,
        sm_sol_total,
        fastest_sm_ms,
        buy_count: candidate.early_buys.len(),
    }
}

async fn score_candidate(shared: &SharedState, candidate: Candidate) -> ScoreDecision {
    let stats = smart_money_stats(&candidate, shared).await;
    if stats.unique_sm_wallets.len() < shared.config.smart_money_threshold {
        return ScoreDecision {
            passed: false,
            gate: "gate3".to_string(),
            score: 0,
            reason: format!(
                "关卡3拒绝：聪明钱不足 | sm={} | 阈值={} | 前{}笔买入",
                stats.unique_sm_wallets.len(),
                shared.config.smart_money_threshold,
                stats.buy_count
            ),
            signal: None,
        };
    }

    let sm_count_score = match stats.unique_sm_wallets.len() {
        0 => 0,
        1 => 10,
        2 => 20,
        _ => 30,
    };
    let sm_sol_score = if stats.sm_sol_total >= 2.0 { 20 } else if stats.sm_sol_total >= 0.5 { 10 } else { 0 };
    let momentum_score = if stats.buy_count >= 15 {
        20
    } else if stats.buy_count >= 8 {
        13
    } else if stats.buy_count >= 4 {
        6
    } else {
        0
    };
    let curve_progress_pct = fetch_curve_progress_pct(shared, &candidate.token).await.unwrap_or(0.0);
    let curve_score = if curve_progress_pct > 5.0 { 15 } else if curve_progress_pct > 2.0 { 10 } else if curve_progress_pct > 0.5 { 5 } else { 0 };
    let buyer_quality_pct = fetch_buyer_quality_pct(shared, &candidate).await.unwrap_or(0.0);
    let buyer_quality_score = (buyer_quality_pct * 15.0).round().clamp(0.0, 15.0) as u32;
    let total_score = sm_count_score + sm_sol_score + momentum_score + curve_score + buyer_quality_score;
    let reason = format!(
        "SM数量={} SM买入={} 动量={} 曲线={} 买家质量={} 总分={} | sm={} sol={:.2} fastest={}ms narrative={}",
        sm_count_score,
        sm_sol_score,
        momentum_score,
        curve_score,
        buyer_quality_score,
        total_score,
        stats.unique_sm_wallets.len(),
        stats.sm_sol_total,
        stats.fastest_sm_ms.unwrap_or_default(),
        if candidate.narrative_keywords.is_empty() { "-".to_string() } else { candidate.narrative_keywords.join("|") }
    );

    if total_score < shared.config.filter_min_score {
        return ScoreDecision {
            passed: false,
            gate: "gate4".to_string(),
            score: total_score,
            reason,
            signal: None,
        };
    }

    let Some(trigger_trade) = select_trigger_trade(&candidate, shared).await else {
        return ScoreDecision {
            passed: false,
            gate: "gate4".to_string(),
            score: total_score,
            reason: format!("{} | 缺少 Smart Money 买入上下文", reason),
            signal: None,
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
    }
}

async fn select_trigger_trade(candidate: &Candidate, shared: &SharedState) -> Option<PumpBuyEvent> {
    let hotlists = shared.hotlists.read().await;
    candidate
        .early_buys
        .iter()
        .find(|buy| hotlists.smart_money.contains(&buy.buyer.to_string()))
        .cloned()
}

async fn fetch_creator_mints(shared: &SharedState, api_key: &str, creator: &str) -> Result<Vec<String>> {
    let mut mints = HashSet::new();
    let mut before_signature: Option<String> = None;

    for _ in 0..HELIUS_MAX_PAGES {
        let url = format!("https://api-mainnet.helius-rpc.com/v0/addresses/{}/transactions", creator);
        let mut request = shared.http.get(url).query(&[
            ("api-key", api_key),
            ("commitment", "confirmed"),
            ("limit", "100"),
            ("sort-order", "desc"),
        ]);
        if let Some(before) = before_signature.as_deref() {
            request = request.query(&[("before-signature", before)]);
        }
        let items: Vec<Value> = request.send().await
            .with_context(|| format!("Helius Creator 查询失败: {}", creator))?
            .error_for_status()
            .with_context(|| format!("Helius Creator 响应异常: {}", creator))?
            .json()
            .await
            .context("Helius Creator JSON 解析失败")?;
        if items.is_empty() {
            break;
        }
        for item in &items {
            for mint in extract_pump_create_mints(item) {
                mints.insert(mint);
            }
        }
        before_signature = items.last()
            .and_then(|item| item.get("signature"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        if before_signature.is_none() || items.len() < HELIUS_PAGE_LIMIT || mints.len() > CREATOR_TOTAL_TOKEN_LIMIT as usize {
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
    let curves: Vec<Pubkey> = mints.iter()
        .filter_map(|mint| Pubkey::from_str(mint).ok())
        .map(|mint| Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &pump_program).0)
        .collect();
    let rpc = shared.rpc_client.clone();
    tokio::task::spawn_blocking(move || {
        let mut graduated = 0u32;
        for chunk in curves.chunks(100) {
            let accounts = rpc.get_multiple_accounts_with_commitment(chunk, CommitmentConfig::confirmed())?.value;
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
    }).await.context("Creator 毕业率统计任务失败")?
}

async fn fetch_curve_progress_pct(shared: &SharedState, token: &NewToken) -> Result<f64> {
    let bonding_curve = Pubkey::from_str(&token.bonding_curve)?;
    let rpc = shared.rpc_client.clone();
    tokio::task::spawn_blocking(move || {
        let account = rpc.get_account_with_commitment(&bonding_curve, CommitmentConfig::confirmed())?;
        let Some(account) = account.value else {
            anyhow::bail!("BondingCurve 不存在");
        };
        let state = BondingCurveState::from_account_data(&account.data)?;
        let progressed = state.virtual_sol_reserves.saturating_sub(CURVE_INITIAL_VIRTUAL_SOL);
        Ok::<f64, anyhow::Error>(((progressed as f64 / 1e9) / CURVE_TOTAL_TARGET_SOL * 100.0).clamp(0.0, 100.0))
    }).await.context("曲线推进查询任务失败")?
}

async fn fetch_buyer_quality_pct(shared: &SharedState, candidate: &Candidate) -> Result<f64> {
    let Some(api_key) = shared.config.helius_api_key.as_deref() else {
        return Ok(0.0);
    };

    let mut unique_buyers = Vec::new();
    let mut seen = HashSet::new();
    for buy in &candidate.early_buys {
        if seen.insert(buy.buyer) {
            unique_buyers.push(buy.buyer.to_string());
        }
        if unique_buyers.len() >= shared.config.smart_money_max_buys {
            break;
        }
    }
    if unique_buyers.is_empty() {
        return Ok(0.0);
    }
    let unique_buyer_count = unique_buyers.len();

    let cutoff = now_ms().saturating_sub(OLD_WALLET_DAYS * 24 * 60 * 60 * 1000);
    let shared_clone = shared.clone();
    let old_count = stream::iter(unique_buyers)
        .map(|address| {
            let shared = shared_clone.clone();
            async move { wallet_is_old(&shared, api_key, &address, cutoff).await.unwrap_or(false) }
        })
        .buffer_unordered(8)
        .fold(0usize, |acc, is_old| async move { acc + usize::from(is_old) })
        .await;

    Ok(old_count as f64 / unique_buyer_count.max(1) as f64)
}

async fn wallet_is_old(shared: &SharedState, api_key: &str, address: &str, cutoff_ms: u64) -> Result<bool> {
    let url = format!("https://api-mainnet.helius-rpc.com/v0/addresses/{}/transactions", address);
    let items: Vec<Value> = shared.http.get(url)
        .query(&[
            ("api-key", api_key),
            ("commitment", "confirmed"),
            ("limit", "1"),
            ("sort-order", "asc"),
        ])
        .send()
        .await
        .with_context(|| format!("Helius 钱包年龄查询失败: {}", address))?
        .error_for_status()
        .with_context(|| format!("Helius 钱包年龄响应异常: {}", address))?
        .json()
        .await
        .context("Helius 钱包年龄 JSON 解析失败")?;
    let oldest_ms = items.first()
        .and_then(|item| item.get("timestamp"))
        .and_then(Value::as_u64)
        .map(|ts| ts * 1000);
    Ok(oldest_ms.map(|ts| ts <= cutoff_ms).unwrap_or(false))
}

async fn reload_hotlists(shared: &SharedState) -> Result<()> {
    let blacklist = load_plaintext_set(&shared.config.creator_blacklist_file).await?;
    let smart_money = load_plaintext_set(&shared.config.smart_money_file).await?;
    {
        let mut hotlists = shared.hotlists.write().await;
        hotlists.creator_blacklist = blacklist.iter().cloned().collect();
        hotlists.smart_money = smart_money.iter().cloned().collect();
    }
    shared.db.sync_blacklist(&blacklist).await?;
    shared.db.sync_smart_money(&smart_money).await?;
    info!("过滤层：名单已加载 | blacklist={} | smart_money={}", blacklist.len(), smart_money.len());
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
    Ok(content.lines()
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
        .with_context(|| format!("打开输出文件失败: {}", path_ref.display()))?;

    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    file.write_all(&line).await?;
    Ok(())
}

async fn record_filter_result(
    shared: &SharedState,
    token: &NewToken,
    passed: bool,
    reject_gate: Option<String>,
    score: Option<u32>,
    reason: String,
) {
    let record = FilterResultRecord {
        mint: token.mint.clone(),
        creator: token.creator.clone(),
        symbol: token.symbol.clone(),
        passed,
        reject_gate: reject_gate.clone(),
        score,
        reason: reason.clone(),
        ts: now_ms(),
    };
    if let Err(err) = shared.db.insert_filter_result(&record).await {
        error!("过滤层：写入 filter_results 失败: {}", err);
    }
    if passed {
        info!("过滤层：通过 | mint={} | score={:?} | {}", token.mint, score, reason);
    } else {
        info!("过滤层：拒绝 | mint={} | gate={} | {}", token.mint, reject_gate.unwrap_or_else(|| "-".to_string()), reason);
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
