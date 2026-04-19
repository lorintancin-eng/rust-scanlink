mod analytics;
mod autosell;
mod config;
mod consensus;
mod filter;
mod groups;
mod grpc;
mod processor;
mod replay;
mod scanner;
mod telegram;
mod tx;
mod utils;

use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::{lookup_host, TcpStream};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{error, info, warn};

use analytics::runtime::{build_runtime_report, log_runtime_report, persist_runtime_report};
use autosell::{AutoSellManager, Position, SellAccountSnapshot, SellSignal};
use config::{
    classify_stream_endpoint, infer_stream_region, is_rabbitstream_url, same_stream_endpoint,
    stream_provider, AppConfig,
};
use filter::{BuySignal as SniperBuySignal, ExecutionReceiptRecord, FilterDb};
use groups::CopyGroup;
use grpc::{AccountSubscriber, AccountUpdate, AtaBalanceCache, BondingCurveCache};
use processor::prefetch::PrefetchCache;
use processor::pumpfun::PumpfunProcessor;
use scanner::{
    NewToken, PumpBuyEvent, SCANNER_BUY_CHANNEL_CAPACITY, SCANNER_NEW_TOKEN_CHANNEL_CAPACITY,
};
use telegram::{TgEvent, TgNotifier, TgStats};
use tx::{
    blockhash,
    builder::TxBuilder,
    confirm::{format_mcap_usd, format_price_gmgn, BuyConfirmer, BuyExecutionReceiptContext},
    execution_router::{ExecutionFeedback, ExecutionPlan, ExecutionProfile},
    sell_executor::SellExecutor,
    sender::TxSender,
};
use utils::sol_price::SolUsdPrice;

const BLOCKHASH_REFRESH_MS: u64 = 120;
const PREFETCH_WAIT_MS: u64 = 8;
const BUY_EXECUTOR_PARALLELISM: usize = 4;

#[derive(Debug, Clone, Copy, Default)]
struct BuyPathTimings {
    queue: Duration,
    prefetch_wait: Duration,
    quote_build: Duration,
    tx_build: Duration,
    send_call: Duration,
}

struct RuntimeGuard {
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct EndpointProbeTarget {
    role: &'static str,
    url: String,
}

fn endpoint_profile(url: &str) -> String {
    format!(
        "{}:{}@{}",
        stream_provider(url),
        classify_stream_endpoint(url),
        infer_stream_region(url).unwrap_or("-")
    )
}

fn log_scanner_topology(config: &AppConfig) {
    let primary_profile = endpoint_profile(&config.scanner_grpc_url);
    let secondary_profile = config
        .scanner_secondary_grpc_url
        .as_deref()
        .map(endpoint_profile)
        .unwrap_or_else(|| "-".to_string());
    let topology = if is_rabbitstream_url(&config.scanner_grpc_url) {
        if config.scanner_secondary_grpc_url.is_some() {
            "rabbit_primary_with_yellowstone_fallback"
        } else {
            "rabbit_primary_only"
        }
    } else {
        "yellowstone_primary"
    };
    info!(
        "Shyft topology | profile={} | scanner_primary={} ({}) | scanner_secondary={} ({}) | secondary_auto={} | account_stream={} ({}) | catchup=raw_event_replay",
        topology,
        config.scanner_primary_feed_label,
        primary_profile,
        config.scanner_secondary_feed_label,
        secondary_profile,
        config.scanner_secondary_auto_inferred,
        config.grpc_account_url,
        endpoint_profile(&config.grpc_account_url),
    );
    if is_rabbitstream_url(&config.scanner_grpc_url) && config.scanner_secondary_grpc_url.is_none()
    {
        warn!("Shyft topology: RabbitStream is primary but no Yellowstone fallback is configured");
    }
    if !is_rabbitstream_url(&config.scanner_grpc_url) && is_rabbitstream_url(&config.grpc_url) {
        warn!(
            "Shyft topology: scanner primary is not RabbitStream even though a RabbitStream URL is available | scanner_primary={} | tx_stream={}",
            config.scanner_grpc_url,
            config.grpc_url,
        );
    }
}

fn collect_endpoint_probe_targets(config: &AppConfig) -> Vec<EndpointProbeTarget> {
    let mut targets = Vec::new();
    targets.push(EndpointProbeTarget {
        role: "scanner_primary",
        url: config.scanner_grpc_url.clone(),
    });
    if let Some(url) = config.scanner_secondary_grpc_url.clone() {
        if !same_stream_endpoint(&url, &config.scanner_grpc_url) {
            targets.push(EndpointProbeTarget {
                role: "scanner_secondary",
                url,
            });
        }
    }
    if !same_stream_endpoint(&config.grpc_account_url, &config.scanner_grpc_url)
        && config
            .scanner_secondary_grpc_url
            .as_deref()
            .map(|url| !same_stream_endpoint(url, &config.grpc_account_url))
            .unwrap_or(true)
    {
        targets.push(EndpointProbeTarget {
            role: "account_stream",
            url: config.grpc_account_url.clone(),
        });
    }
    targets
}

async fn probe_endpoint_connect_ms(url: &str) -> Result<u128> {
    let parsed =
        reqwest::Url::parse(url).with_context(|| format!("invalid endpoint url: {url}"))?;
    let host = parsed
        .host_str()
        .map(str::to_string)
        .with_context(|| format!("missing endpoint host: {url}"))?;
    let port = parsed.port_or_known_default().unwrap_or(443);
    let mut best_ms: Option<u128> = None;

    for _ in 0..2 {
        let started = Instant::now();
        let addrs = tokio::time::timeout(
            Duration::from_millis(1_500),
            lookup_host((host.as_str(), port)),
        )
        .await
        .with_context(|| format!("dns lookup timeout for {host}:{port}"))?
        .with_context(|| format!("dns lookup failed for {host}:{port}"))?;
        for addr in addrs {
            match tokio::time::timeout(Duration::from_millis(1_500), TcpStream::connect(addr)).await
            {
                Ok(Ok(stream)) => {
                    let elapsed_ms = started.elapsed().as_millis();
                    best_ms = Some(best_ms.map_or(elapsed_ms, |best| best.min(elapsed_ms)));
                    drop(stream);
                    break;
                }
                Ok(Err(_)) | Err(_) => {}
            }
        }
    }

    best_ms.with_context(|| format!("tcp connect failed for {host}:{port}"))
}

fn spawn_endpoint_probe_task(config: &AppConfig) {
    let targets = collect_endpoint_probe_targets(config);
    if targets.is_empty() {
        return;
    }
    tokio::spawn(async move {
        for target in targets {
            match probe_endpoint_connect_ms(&target.url).await {
                Ok(connect_ms) => info!(
                    "Endpoint probe | role={} | url={} | profile={} | tcp_connect_ms={}",
                    target.role,
                    target.url,
                    endpoint_profile(&target.url),
                    connect_ms,
                ),
                Err(err) => warn!(
                    "Endpoint probe failed | role={} | url={} | {}",
                    target.role, target.url, err,
                ),
            }
        }
    });
}

fn execution_feedback_from_receipts(
    execution_plan: &ExecutionPlan,
    receipts: &[ExecutionReceiptRecord],
) -> ExecutionFeedback {
    let mut sample_count = 0usize;
    let mut success_count = 0usize;

    for receipt in receipts {
        if matches!(receipt.status.as_str(), "dry_run" | "submitted") {
            continue;
        }
        sample_count = sample_count.saturating_add(1);
        if execution_status_is_success(&receipt.status) {
            success_count = success_count.saturating_add(1);
        }
    }

    let success_rate_bps = if sample_count == 0 {
        10_000
    } else {
        ((success_count as u128) * 10_000u128 / sample_count as u128) as u32
    };
    let recent_failure_streak = receipts
        .iter()
        .rev()
        .filter(|receipt| !matches!(receipt.status.as_str(), "dry_run" | "submitted"))
        .take_while(|receipt| execution_status_is_failure(&receipt.status))
        .count();

    let prefer_fanout = execution_plan.send_jito && sample_count >= 4 && success_rate_bps < 8_000;
    let prefer_zero_slot = execution_plan.send_zero_slot
        && sample_count >= 4
        && (success_rate_bps < 7_000 || recent_failure_streak >= 2);

    let fee_boost_bps = if sample_count < 4 {
        10_000
    } else if success_rate_bps < 5_000 {
        13_500
    } else if success_rate_bps < 7_500 {
        12_000
    } else if recent_failure_streak >= 2 {
        11_500
    } else {
        10_000
    };
    let tip_boost_bps = if sample_count < 4 {
        10_000
    } else if success_rate_bps < 5_000 {
        14_000
    } else if success_rate_bps < 7_500 {
        12_500
    } else if recent_failure_streak >= 2 {
        11_500
    } else {
        10_000
    };

    ExecutionFeedback {
        sample_count,
        success_rate_bps,
        recent_failure_streak,
        prefer_fanout,
        prefer_zero_slot,
        fee_boost_bps,
        tip_boost_bps,
    }
}

fn execution_status_is_success(status: &str) -> bool {
    matches!(
        status,
        "confirmed" | "landed" | "bundle_accepted" | "success"
    )
}

fn execution_status_is_failure(status: &str) -> bool {
    status.ends_with("_failed")
        || matches!(
            status,
            "dropped" | "expired" | "rejected" | "replaced" | "not_landed" | "bundle_rejected"
        )
}

fn format_latency(duration: Duration) -> String {
    if duration.as_millis() > 0 {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{}us", duration.as_micros())
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

impl RuntimeGuard {
    fn acquire(config: &AppConfig) -> Result<Self> {
        let path = runtime_guard_path(config);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        for _ in 0..2 {
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(mut file) => {
                    writeln!(file, "{}", process::id())?;
                    info!("Runtime guard acquired: {}", path.display());
                    return Ok(Self { path });
                }
                Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                    if runtime_guard_is_stale(&path)? {
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                    anyhow::bail!(
                        "another rust-scanlink-trader instance is already running | pid_file={}",
                        path.display()
                    );
                }
                Err(err) => return Err(err.into()),
            }
        }

        anyhow::bail!(
            "failed to acquire runtime pid file after stale cleanup | pid_file={}",
            path.display()
        );
    }
}

impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn runtime_guard_path(config: &AppConfig) -> PathBuf {
    Path::new(&config.filter_db_path)
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("data"))
        .join("runtime.pid")
}

fn runtime_guard_is_stale(path: &Path) -> Result<bool> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(true),
        Err(err) => return Err(err.into()),
    };
    let Some(pid) = content.trim().parse::<u32>().ok() else {
        return Ok(true);
    };

    #[cfg(target_os = "linux")]
    {
        let cmdline = PathBuf::from(format!("/proc/{pid}/cmdline"));
        match fs::read(&cmdline) {
            Ok(bytes) => {
                let command = String::from_utf8_lossy(&bytes);
                Ok(!command.contains("rust-scanlink-trader"))
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(true),
            Err(_) => Ok(false),
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        Ok(false)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    /*

    info!("==============================================");
    */
    info!("==============================================");
    info!(
        "   Solana Pump.fun Scanner System v{}",
        env!("CARGO_PKG_VERSION")
    );
    info!("   Yellowstone Scanner + Multi-Gate Filter + Execution Router");
    info!("==============================================");
    /*
    info!(
        "   Solana Pump.fun Scanner System v{}",
        env!("CARGO_PKG_VERSION")
    );
    info!("   Yellowstone Scanner + 鍥涘眰杩囨护 + 鐜版湁鎵ц灞?);
    info!("==============================================");

    */
    let config = Arc::new(AppConfig::from_env()?);
    if config.replay_mode_enabled {
        info!(
            "Mode: replay | pipeline={} | from_ms={:?} | to_ms={:?} | window_minutes={} | speedup={:.1} | replay_db={} | report={}",
            config.replay_pipeline_enabled,
            config.replay_from_ms,
            config.replay_to_ms,
            config.replay_window_minutes,
            config.replay_speedup,
            config.replay_db_path,
            config.replay_report_file,
        );
        replay::run(config.as_ref()).await?;
        return Ok(());
    }
    let _runtime_guard = RuntimeGuard::acquire(config.as_ref())?;
    let execution_plan = ExecutionPlan::from_config(config.as_ref());
    let sniper_group = CopyGroup::from_app_config(config.as_ref());
    if config.execution_enabled {
        info!("Mode: scanner + filter + execution");
    } else {
        info!(
            "Mode: scanner + filter only | buy/sell disabled | scanner_live={} | scanned={} | passed={}",
            config.scanner_live_tokens_file, config.scanned_tokens_file, config.passed_tokens_file,
        );
    }

    info!("浜ゆ槗閽卞寘: {}", config.pubkey);
    info!(
        "鎵摼鑺傜偣: {} | SmartMoney 闃堝€? {} | 鎵撳垎闃堝€? {}",
        config.scanner_grpc_url, config.smart_money_threshold, config.filter_min_score,
    );

    info!("Execution plan: {}", execution_plan.summary());
    info!(
        "Gate3 windows: fast={}ms soft={}ms hard={}ms | thresholds: buyers_fast={} buyers_soft={} sol_fast={:.2} sol_soft={:.2} max_share={:.2} self_buy_max_sol={:.2} self_buy_max_share={:.2} self_buy_hard_sol={:.2} self_buy_hard_share={:.2} self_buy_min_external_buyers={} self_buy_min_external_sol={:.2} | gate4 fast_min={} soft_min={} global_min={} | smart_money_disabled={} | helius={} | creator_gate_timeout_ms={} sniper_fast_timeout_ms={} creator_gate_remote_concurrency={} creator_gate_cache_miss_retry_ms={} | hotlists: wallets={} funders={} blocked={}",
        config.smart_money_fast_window_ms,
        config.smart_money_soft_window_ms,
        config.gate3_hard_reject_ms,
        config.smart_money_fast_threshold,
        config.smart_money_threshold,
        config.gate3_fast_min_sol,
        config.gate3_soft_min_sol,
        config.gate3_max_single_buyer_share,
        config.gate3_creator_self_buy_max_sol,
        config.gate3_creator_self_buy_max_share,
        config.gate3_creator_self_buy_hard_sol,
        config.gate3_creator_self_buy_hard_share,
        config.gate3_creator_self_buy_min_external_buyers,
        config.gate3_creator_self_buy_min_external_sol,
        config.filter_fast_min_score,
        config.filter_soft_min_score,
        config.filter_min_score,
        config.disable_smart_money_filter,
        config.helius_api_key.is_some(),
        config.creator_gate_timeout_ms,
        config.creator_gate_sniper_fast_timeout_ms,
        config.creator_gate_remote_concurrency,
        config.creator_gate_cache_miss_retry_ms,
        config.smart_money_file,
        config.smart_money_funder_file,
        config.blocked_buyers_file,
    );
    info!(
        "Dynamic hot keywords: enabled={} refresh_secs={} limit={} snapshot_file={} | social_file={} telegram_file={} event_file={} | preheat_ttl={}s base_ttl={}s confirmed_ttl={}s confirm_window={}s confirm_min_mints={} | bonuses preheat={} base={} confirmed={} fast_relief={} cap={} | coingecko_pro={}",
        config.dynamic_hot_keywords_enabled,
        config.dynamic_hot_refresh_secs,
        config.dynamic_hot_keywords_limit,
        config.dynamic_hot_keywords_file,
        config.narrative_social_keywords_file,
        config.narrative_telegram_keywords_file,
        config.narrative_event_keywords_file,
        config.narrative_preheat_ttl_secs,
        config.narrative_base_ttl_secs,
        config.narrative_confirmed_ttl_secs,
        config.narrative_onchain_confirm_window_secs,
        config.narrative_onchain_confirm_min_mints,
        config.narrative_preheat_bonus_per_hit,
        config.narrative_base_bonus_per_hit,
        config.narrative_confirmed_bonus_per_hit,
        config.narrative_confirmed_fast_score_relief,
        config.dynamic_narrative_bonus_cap,
        config.coingecko_api_key.is_some(),
    );
    info!(
        "Scanner feeds: mode={} | primary_label={} primary_url={} | secondary_label={} secondary_url={} | secondary_auto={} | account_url={} | deshred_label={} deshred_url={} | scanner_live_file={} scanned_file={} | runtime_db={} analytics_db={} | persist_raw_events={} gate3_sequences={} scoring_breakdowns={} labels={} feed_health={} | analytics_retention_secs raw={} metrics={} exec={} | catchup_window_ms={} catchup_max_events={} failover_stale_ms={} health_snapshot_secs={} replay_db={} replay_report={}",
        config.scanner_mode,
        config.scanner_primary_feed_label,
        config.scanner_grpc_url,
        config.scanner_secondary_feed_label,
        config
            .scanner_secondary_grpc_url
            .as_deref()
            .unwrap_or("-"),
        config.scanner_secondary_auto_inferred,
        config.grpc_account_url,
        config.scanner_deshred_feed_label,
        config
            .scanner_deshred_grpc_url
            .as_deref()
            .unwrap_or("-"),
        config.scanner_live_tokens_file,
        config.scanned_tokens_file,
        config.filter_db_path,
        config.analytics_db_path,
        config.persist_raw_scanner_events,
        config.persist_gate3_sequences,
        config.persist_scoring_breakdowns,
        config.persist_label_suggestions,
        config.persist_feed_health,
        config.analytics_raw_event_retention_secs,
        config.analytics_metrics_retention_secs,
        config.analytics_execution_retention_secs,
        config.scanner_catchup_window_ms,
        config.scanner_catchup_max_events,
        config.scanner_failover_stale_ms,
        config.scanner_health_snapshot_secs,
        config.replay_db_path,
        config.replay_report_file,
    );
    log_scanner_topology(config.as_ref());
    spawn_endpoint_probe_task(config.as_ref());
    info!(
        "Execution feedback: window_secs={} refresh_secs={}",
        config.execution_feedback_window_secs, config.execution_feedback_refresh_secs,
    );
    info!(
        "Runtime report: enabled={} interval_secs={} window_secs={} file={}",
        config.runtime_report_enabled,
        config.runtime_report_interval_secs,
        config.runtime_report_window_secs,
        config.runtime_report_file,
    );

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        config.rpc_url.clone(),
        solana_sdk::commitment_config::CommitmentConfig::confirmed(),
    ));
    let runtime_db = FilterDb::new(&config.filter_db_path).await?;
    let analytics_db = FilterDb::new(&config.analytics_db_path).await?;
    let execution_db = Arc::new(analytics_db.clone());
    let execution_feedback = Arc::new(RwLock::new(ExecutionFeedback::default()));

    if config.execution_feedback_refresh_secs > 0 {
        let execution_db_task = execution_db.clone();
        let execution_plan_task = execution_plan.clone();
        let feedback_state = execution_feedback.clone();
        let feedback_cfg = config.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                feedback_cfg.execution_feedback_refresh_secs.max(5),
            ));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let to_ms = current_time_ms();
                let from_ms = to_ms.saturating_sub(
                    feedback_cfg
                        .execution_feedback_window_secs
                        .saturating_mul(1000),
                );
                match execution_db_task
                    .list_execution_receipts_window(from_ms, to_ms)
                    .await
                {
                    Ok(receipts) => {
                        let feedback =
                            execution_feedback_from_receipts(&execution_plan_task, &receipts);
                        info!(
                            "Execution feedback refreshed | samples={} success_bps={} failure_streak={} fanout={} zero_slot={} fee_boost_bps={} tip_boost_bps={}",
                            feedback.sample_count,
                            feedback.success_rate_bps,
                            feedback.recent_failure_streak,
                            feedback.prefer_fanout,
                            feedback.prefer_zero_slot,
                            feedback.fee_boost_bps,
                            feedback.tip_boost_bps,
                        );
                        *feedback_state.write().await = feedback;
                    }
                    Err(err) => warn!("execution feedback refresh failed | {}", err),
                }
            }
        });
    }

    if config.runtime_report_enabled && config.runtime_report_interval_secs > 0 {
        let runtime_db = Arc::new(runtime_db.clone());
        let analytics_runtime_db = Arc::new(analytics_db.clone());
        let runtime_cfg = config.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                runtime_cfg.runtime_report_interval_secs.max(10),
            ));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let to_ms = current_time_ms();
                let from_ms = to_ms
                    .saturating_sub(runtime_cfg.runtime_report_window_secs.saturating_mul(1000));
                match build_runtime_report(
                    runtime_db.as_ref(),
                    analytics_runtime_db.as_ref(),
                    runtime_cfg.as_ref(),
                    from_ms,
                    to_ms,
                )
                .await
                {
                    Ok(report) => {
                        log_runtime_report(&report);
                        if let Err(err) =
                            persist_runtime_report(&report, &runtime_cfg.runtime_report_file).await
                        {
                            warn!("runtime report persist failed | {}", err);
                        }
                    }
                    Err(err) => warn!("runtime report refresh failed | {}", err),
                }
                interval.tick().await;
            }
        });
    }

    let balance = rpc_client.get_balance(&config.pubkey)?;
    info!("SOL balance: {:.4}", balance as f64 / 1e9);

    let blockhash_cache = blockhash::init_blockhash_cache(&rpc_client).await?;
    let _blockhash_task = blockhash_cache.start_refresh_task(
        rpc_client.clone(),
        Duration::from_millis(BLOCKHASH_REFRESH_MS),
    );

    let sol_usd = SolUsdPrice::new();
    sol_usd.init(config.default_sol_usd_price).await;
    let _sol_usd_task = sol_usd.start_refresh_task();

    let bc_cache = BondingCurveCache::new();
    let ata_cache = AtaBalanceCache::new();
    let prefetch_cache = Arc::new(PrefetchCache::new(bc_cache.clone()));

    let tx_sender = Arc::new(TxSender::new(
        config.rpc_url.clone(),
        config.secondary_rpc_url.clone(),
        config.jito_block_engine_urls.clone(),
        config.jito_enabled,
        config.jito_auth_uuid.clone(),
        config.zero_slot_urls.clone(),
    ));
    let buy_exec_limiter = Arc::new(Semaphore::new(BUY_EXECUTOR_PARALLELISM));
    let pumpfun = Arc::new(PumpfunProcessor::new(rpc_client.clone()));
    let auto_sell_manager = Arc::new(AutoSellManager::new(
        config.as_ref().clone(),
        bc_cache.clone(),
        rpc_client.clone(),
        sol_usd.clone(),
    ));
    let tg_stats = Arc::new(TgStats::new());
    let tg_notifier = TgNotifier::noop();

    let account_subscriber = Arc::new(AccountSubscriber::new(
        config.grpc_account_url.clone(),
        config.grpc_account_token.clone(),
        bc_cache.clone(),
        ata_cache.clone(),
    ));

    let (scanner_new_token_tx, scanner_new_token_rx) =
        mpsc::channel::<NewToken>(SCANNER_NEW_TOKEN_CHANNEL_CAPACITY);
    let (scanner_buy_tx, scanner_buy_rx) =
        mpsc::channel::<PumpBuyEvent>(SCANNER_BUY_CHANNEL_CAPACITY);
    let (buy_signal_tx, mut buy_signal_rx) = mpsc::channel::<SniperBuySignal>(256);
    let (sell_signal_tx, mut sell_signal_rx) = mpsc::unbounded_channel::<SellSignal>();
    let (account_update_tx, account_update_rx) = mpsc::unbounded_channel::<AccountUpdate>();

    let sell_executor = Arc::new(SellExecutor::new(
        config.as_ref().clone(),
        rpc_client.clone(),
        pumpfun.clone(),
        tx_sender.clone(),
        blockhash_cache.clone(),
        auto_sell_manager.clone(),
        bc_cache.clone(),
        ata_cache.clone(),
        prefetch_cache.clone(),
        account_subscriber.clone(),
        tg_notifier.clone(),
    ));

    if config.execution_enabled && config.auto_sell_enabled {
        let _grpc_monitor =
            auto_sell_manager.start_grpc_monitor(account_update_rx, sell_signal_tx.clone());
        let _fallback_monitor = auto_sell_manager.start_fallback_monitor(sell_signal_tx.clone());
    }

    let prefetch_cleanup = prefetch_cache.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            prefetch_cleanup.cleanup(300);
        }
    });

    if config.execution_enabled && config.auto_sell_enabled {
        let account_subscriber_task = account_subscriber.clone();
        let account_update_tx_task = account_update_tx.clone();
        tokio::spawn(async move {
            loop {
                match account_subscriber_task
                    .subscribe(account_update_tx_task.clone())
                    .await
                {
                    Ok(()) => warn!("璐︽埛璁㈤槄娴佸叧闂紝鍑嗗閲嶈繛"),
                    Err(err) => error!("璐︽埛璁㈤槄娴佸紓甯? {}", err),
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });

        let sell_exec = sell_executor.clone();
        tokio::spawn(async move {
            while let Some(signal) = sell_signal_rx.recv().await {
                sell_exec.handle_sell_signal(signal).await;
            }
        });
    }

    let scanner_cfg = config.clone();
    let scanner_runtime_db = runtime_db.clone();
    let scanner_analytics_db = analytics_db.clone();
    let scanner_new_token_tx_task = scanner_new_token_tx.clone();
    let scanner_buy_tx_task = scanner_buy_tx.clone();
    tokio::spawn(async move {
        if let Err(err) = scanner::geyser::start(
            scanner_cfg,
            Some(scanner_runtime_db),
            Some(scanner_analytics_db),
            scanner_new_token_tx_task,
            scanner_buy_tx_task,
        )
        .await
        {
            error!("鎵摼灞傞€€鍑? {}", err);
        }
    });

    let filter_cfg = config.clone();
    let filter_runtime_db = runtime_db.clone();
    let filter_analytics_db = analytics_db.clone();
    let filter_rpc = rpc_client.clone();
    tokio::spawn(async move {
        if let Err(err) = filter::run(
            filter_cfg,
            filter_runtime_db,
            filter_analytics_db,
            filter_rpc,
            scanner_new_token_rx,
            scanner_buy_rx,
            buy_signal_tx,
        )
        .await
        {
            error!("杩囨护灞傞€€鍑? {}", err);
        }
    });

    info!("涓绘祦绋嬪凡鍚姩锛岀瓑寰呮壂鎻忎簨浠朵笌 BuySignal");

    while let Some(signal) = buy_signal_rx.recv().await {
        let feedback = execution_feedback.read().await.clone();
        let execution_profile = execution_plan.profile_for_signal(
            &signal.path,
            signal.quality_score,
            signal.urgency_score,
            signal.execution_confidence,
            &feedback,
        );
        let Ok(token_mint) = Pubkey::from_str(&signal.token.mint) else {
            warn!("鎵ц灞傦細mint 鏃犳晥锛岃烦杩?{}", signal.token.mint);
            continue;
        };

        if !config.execution_enabled {
            info!(
                "Dry-run shortlist | mint={} | symbol={} | score={} | sm={} | sol={:.2} | exec={} | reason={}",
                signal.token.mint,
                signal.token.symbol,
                signal.score,
                signal.sm_count,
                signal.sm_sol_total,
                execution_profile.summary(),
                signal.reason,
            );
            if let Err(err) = execution_db
                .insert_execution_receipt(&ExecutionReceiptRecord {
                    mint: signal.token.mint.clone(),
                    signature: None,
                    route_label: execution_profile.route_label.clone(),
                    status: "dry_run".to_string(),
                    detail: signal.reason.clone(),
                    path: signal.path.clone(),
                    quality_score: signal.quality_score,
                    urgency_score: signal.urgency_score,
                    execution_confidence: signal.execution_confidence,
                    priority_fee_micro_lamport: execution_profile
                        .adjusted_priority_fee(config.priority_fee_micro_lamport),
                    jito_tip_lamports: execution_profile
                        .adjusted_tip_lamports(config.jito_buy_tip_lamports),
                    zero_slot_tip_lamports: execution_profile
                        .adjusted_tip_lamports(config.zero_slot_tip_lamports),
                    recorded_at_ms: current_time_ms(),
                })
                .await
            {
                warn!("execution dry-run receipt failed | {}", err);
            }
            continue;
        }

        tg_stats.buy_attempts.fetch_add(1, Ordering::Relaxed);

        info!(
            "鎵ц灞傦細鏀跺埌 BuySignal | mint={} | symbol={} | score={} | sm={} | sol={:.2} | latency={}ms",
            signal.token.mint,
            signal.token.symbol,
            signal.score,
            signal.sm_count,
            signal.sm_sol_total,
            signal.latency_ms,
        );

        let prefetched = prefetch_cache.prefetch_token(
            &token_mint,
            &signal.trigger_trade.token_program,
            &signal.trigger_trade.instruction_accounts,
            &signal.trigger_trade.buyer,
            config.as_ref(),
        );
        account_subscriber.track_bonding_curve(token_mint, prefetched.bonding_curve);
        account_subscriber.track_ata(token_mint, prefetched.user_ata);

        if bc_cache.get(&token_mint).is_none() {
            let pf = pumpfun.clone();
            let bonding_curve = prefetched.bonding_curve;
            let mint_copy = token_mint;
            let bc_cache_copy = bc_cache.clone();
            tokio::spawn(async move {
                if let Ok(state) = pf.prefetch_bonding_curve(&bonding_curve).await {
                    bc_cache_copy.update(&mint_copy, state);
                }
            });
        }

        let cfg = config.as_ref().clone();
        let rpc = rpc_client.clone();
        let pf = pumpfun.clone();
        let bh = blockhash_cache.clone();
        let sender = tx_sender.clone();
        let sol = sol_usd.clone();
        let auto_sell = auto_sell_manager.clone();
        let prefetch = prefetch_cache.clone();
        let bc = bc_cache.clone();
        let ata = ata_cache.clone();
        let acct_sub = account_subscriber.clone();
        let tg = tg_notifier.clone();
        let stats = tg_stats.clone();
        let limiter = buy_exec_limiter.clone();
        let group = sniper_group.clone();
        let execution_db = execution_db.clone();
        let wallets = vec![signal.trigger_trade.buyer];
        let target_instruction_data = signal.trigger_trade.instruction_data.clone();
        let detected_at = signal.trigger_trade.detected_at;

        tokio::spawn(async move {
            let _permit = limiter.acquire_owned().await.expect("buy semaphore closed");
            execute_buy(
                &group,
                &token_mint,
                &wallets,
                detected_at,
                &target_instruction_data,
                &cfg,
                &rpc,
                &pf,
                &bh,
                &sender,
                &sol,
                &auto_sell,
                &prefetch,
                &bc,
                &ata,
                &acct_sub,
                &tg,
                &stats,
                execution_profile,
                &execution_db,
            )
            .await;
        });
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_buy(
    group: &CopyGroup,
    mint: &Pubkey,
    wallets: &[Pubkey],
    detected_at: Instant,
    target_instruction_data: &[u8],
    base_config: &AppConfig,
    rpc_client: &Arc<RpcClient>,
    pumpfun: &Arc<PumpfunProcessor>,
    blockhash_cache: &blockhash::BlockhashCache,
    tx_sender: &Arc<TxSender>,
    sol_usd: &SolUsdPrice,
    auto_sell_manager: &Arc<AutoSellManager>,
    prefetch_cache: &Arc<PrefetchCache>,
    bc_cache: &BondingCurveCache,
    ata_cache: &AtaBalanceCache,
    _account_subscriber: &Arc<AccountSubscriber>,
    tg: &TgNotifier,
    tg_stats: &Arc<TgStats>,
    execution_profile: ExecutionProfile,
    execution_db: &Arc<FilterDb>,
) {
    let start = Instant::now();
    let detect_to_exec = detected_at.elapsed();
    let mut timings = BuyPathTimings {
        queue: detect_to_exec,
        ..Default::default()
    };
    let mut config = group.to_app_config(base_config);
    config.priority_fee_micro_lamport =
        execution_profile.adjusted_priority_fee(config.priority_fee_micro_lamport);
    let effective_tip_lamports = execution_profile.adjusted_tip_lamports(group.tip_buy_lamports);
    let execution_plan = ExecutionPlan::from_config(&config);

    let prefetch_wait_start = Instant::now();
    let prefetched = match prefetch_cache.get(mint) {
        Some(prefetched) => Some(prefetched),
        None => {
            prefetch_cache
                .get_or_wait(mint, Duration::from_millis(PREFETCH_WAIT_MS))
                .await
        }
    };
    timings.prefetch_wait = prefetch_wait_start.elapsed();

    let buy_sol = group.buy_sol_amount;
    let buy_lamports = group.buy_lamports();
    let sol_price = sol_usd.get();

    let quote_build_start = Instant::now();
    let buy_result: Result<(processor::MirrorInstruction, u64), anyhow::Error> =
        if let Some(ref pf) = prefetched {
            if pf.mirror_accounts.is_empty() {
                Err(anyhow::anyhow!("missing mirror accounts"))
            } else if let Some(bc_state) = bc_cache.get(mint) {
                let token_amount = bc_state.sol_to_token_quote(buy_lamports);
                pumpfun
                    .buy_from_cached_state(
                        mint,
                        &pf.user_ata,
                        &pf.token_program,
                        &pf.source_wallet,
                        &pf.mirror_accounts,
                        &bc_state,
                        &config,
                    )
                    .map(|mirror| (mirror, token_amount))
            } else if target_instruction_data.len() >= 24 {
                pumpfun.buy_from_target_instruction(
                    mint,
                    &pf.user_ata,
                    &pf.token_program,
                    &pf.source_wallet,
                    &pf.mirror_accounts,
                    target_instruction_data,
                    &config,
                )
            } else {
                Err(anyhow::anyhow!("missing bc cache and target instruction"))
            }
        } else {
            Err(anyhow::anyhow!("prefetch not ready"))
        };
    timings.quote_build = quote_build_start.elapsed();

    let (estimated_tokens_raw, entry_price_sol, entry_mcap_sol) = match &buy_result {
        Ok((_, estimated_tokens)) if *estimated_tokens > 0 => {
            let display_tokens = *estimated_tokens as f64 / 1e6;
            let price = if display_tokens > 0.0 {
                buy_sol / display_tokens
            } else {
                0.0
            };
            let mcap = if let Some(bc_state) = bc_cache.get(mint) {
                bc_state.market_cap_sol()
            } else {
                price * processor::pumpfun::PUMP_TOTAL_SUPPLY
            };
            (*estimated_tokens, price, mcap)
        }
        _ => (0, 0.0, 0.0),
    };

    let entry_price_usd = entry_price_sol * sol_price;
    let entry_mcap_usd = entry_mcap_sol * sol_price;
    let pre_buy_ata_balance = ata_cache.get(mint).unwrap_or(0);

    let mut position = Position::new(
        group.clone(),
        *mint,
        buy_lamports,
        entry_price_sol,
        wallets[0],
        pre_buy_ata_balance,
    );
    position.set_token_amount_estimate(estimated_tokens_raw);
    position.entry_mcap_sol = entry_mcap_sol;
    if let Some(ref pf) = prefetched {
        position.set_sell_snapshot(SellAccountSnapshot {
            bonding_curve: pf.bonding_curve,
            associated_bonding_curve: pf.associated_bonding_curve,
            user_ata: pf.user_ata,
            token_program: pf.token_program,
            mirror_accounts: pf.mirror_accounts.clone(),
            source_wallet: pf.source_wallet,
        });
    }
    let position_key = position.key();

    match buy_result {
        Ok((mirror, _)) => {
            let (blockhash, _) = blockhash_cache.get_sync();
            let tx_build_start = Instant::now();
            let tx_result = if execution_plan.prefers_zero_slot() {
                let fee_account = tx_sender.random_0slot_tip_account();
                TxBuilder::build_0slot_transaction(
                    &mirror,
                    &config,
                    &config.keypair,
                    blockhash,
                    &fee_account,
                    effective_tip_lamports,
                    &[],
                )
            } else if execution_plan.prefers_jito() {
                let tip = tx_sender.random_jito_tip_account();
                TxBuilder::build_jito_bundle_transaction(
                    &mirror,
                    &config,
                    &config.keypair,
                    blockhash,
                    &tip,
                    effective_tip_lamports,
                    &[],
                )
            } else {
                TxBuilder::build_transaction(&mirror, &config, &config.keypair, blockhash, &[])
            };
            timings.tx_build = tx_build_start.elapsed();

            match tx_result {
                Ok(transaction) => {
                    let send_call_start = Instant::now();
                    let send_result = if execution_profile.use_all_channels
                        && !execution_profile.allow_zero_slot
                    {
                        match tx_sender
                            .send_all_channels_with_opts(&transaction, None, true)
                            .await
                        {
                            Ok(result) if result.success => {
                                let signature = result.signature.unwrap_or_else(|| {
                                    transaction.signatures.first().copied().unwrap_or_default()
                                });
                                Ok((
                                    signature,
                                    format!(
                                        "fanout success channels={}/{}",
                                        result.channels_succeeded, result.channels_sent
                                    ),
                                    result.elapsed,
                                ))
                            }
                            Ok(result) => Err(anyhow::anyhow!(
                                "fanout send failed channels={}/{}",
                                result.channels_succeeded,
                                result.channels_sent
                            )),
                            Err(err) => Err(err),
                        }
                    } else {
                        tx_sender.fire_and_forget(&transaction, None).map(|sig| {
                            (
                                sig,
                                execution_profile.route_label.clone(),
                                Duration::default(),
                            )
                        })
                    };

                    match send_result {
                        Ok((sig, route_detail, send_elapsed)) => {
                            timings.send_call = if send_elapsed > Duration::default() {
                                send_elapsed
                            } else {
                                send_call_start.elapsed()
                            };
                            let total_latency = start.elapsed();
                            let sig_str = sig.to_string();
                            let buy_usd = sol_usd.sol_to_usd(buy_sol);

                            info!(
                                "Buy submitted: [{}] {} | {:.4} SOL (${:.2}) | est {:.0} tokens | price={} | mcap={} | route={} | detail={} | queue={} | prefetch={} | quote_build={} | tx_build={} | send_call={} | total={} | sig={}",
                                group.name,
                                &mint.to_string()[..12],
                                buy_sol,
                                buy_usd,
                                estimated_tokens_raw as f64 / 1e6,
                                format_price_gmgn(entry_price_usd),
                                format_mcap_usd(entry_mcap_usd),
                                execution_profile.summary(),
                                route_detail,
                                format_latency(timings.queue),
                                format_latency(timings.prefetch_wait),
                                format_latency(timings.quote_build),
                                format_latency(timings.tx_build),
                                format_latency(timings.send_call),
                                format_latency(total_latency),
                                &sig_str[..16.min(sig_str.len())],
                            );
                            if let Err(err) = execution_db
                                .insert_execution_receipt(&ExecutionReceiptRecord {
                                    mint: mint.to_string(),
                                    signature: Some(sig_str.clone()),
                                    route_label: execution_profile.route_label.clone(),
                                    status: "submitted".to_string(),
                                    detail: route_detail.clone(),
                                    path: execution_profile.signal_path.clone(),
                                    quality_score: execution_profile.quality_score,
                                    urgency_score: execution_profile.urgency_score,
                                    execution_confidence: execution_profile.execution_confidence,
                                    priority_fee_micro_lamport: config.priority_fee_micro_lamport,
                                    jito_tip_lamports: effective_tip_lamports,
                                    zero_slot_tip_lamports: effective_tip_lamports,
                                    recorded_at_ms: current_time_ms(),
                                })
                                .await
                            {
                                warn!("execution receipt insert failed | {}", err);
                            }
                            if execution_profile.route_label.contains("jito") {
                                if let Err(err) = execution_db
                                    .insert_execution_receipt(&ExecutionReceiptRecord {
                                        mint: mint.to_string(),
                                        signature: Some(sig_str.clone()),
                                        route_label: execution_profile.route_label.clone(),
                                        status: "bundle_accepted".to_string(),
                                        detail: route_detail,
                                        path: execution_profile.signal_path.clone(),
                                        quality_score: execution_profile.quality_score,
                                        urgency_score: execution_profile.urgency_score,
                                        execution_confidence: execution_profile
                                            .execution_confidence,
                                        priority_fee_micro_lamport: config
                                            .priority_fee_micro_lamport,
                                        jito_tip_lamports: effective_tip_lamports,
                                        zero_slot_tip_lamports: effective_tip_lamports,
                                        recorded_at_ms: current_time_ms(),
                                    })
                                    .await
                                {
                                    warn!("execution bundle_accepted receipt failed | {}", err);
                                }
                            }

                            tg.send(TgEvent::BuySubmitted {
                                group_name: group.name.clone(),
                                mint: *mint,
                                sol_amount: buy_sol,
                                latency_ms: total_latency.as_millis() as u64,
                            });

                            let user_ata = prefetched
                                .as_ref()
                                .map(|pf| pf.user_ata)
                                .unwrap_or_else(|| {
                                    get_associated_token_address(&config.pubkey, mint)
                                });
                            if config.auto_sell_enabled {
                                position.mark_submitted(sig_str.clone());
                                position.mark_confirming();
                                auto_sell_manager.add_position(position.clone());
                            }
                            BuyConfirmer::spawn_confirm_task(
                                rpc_client.clone(),
                                execution_db.clone(),
                                BuyExecutionReceiptContext {
                                    route_label: execution_profile.route_label.clone(),
                                    path: execution_profile.signal_path.clone(),
                                    quality_score: execution_profile.quality_score,
                                    urgency_score: execution_profile.urgency_score,
                                    execution_confidence: execution_profile.execution_confidence,
                                    priority_fee_micro_lamport: config.priority_fee_micro_lamport,
                                    jito_tip_lamports: effective_tip_lamports,
                                    zero_slot_tip_lamports: effective_tip_lamports,
                                },
                                auto_sell_manager.clone(),
                                bc_cache.clone(),
                                ata_cache.clone(),
                                sol_usd.clone(),
                                position_key,
                                group.name.clone(),
                                *mint,
                                sig,
                                config.pubkey,
                                buy_lamports,
                                user_ata,
                                estimated_tokens_raw,
                                pre_buy_ata_balance,
                                tg.clone(),
                            );
                        }
                        Err(err) => {
                            error!(
                                "Buy send failed [{}] {}: {}",
                                group.name,
                                &mint.to_string()[..12],
                                err
                            );
                            tg_stats.buy_failed.fetch_add(1, Ordering::Relaxed);
                            if let Err(receipt_err) = execution_db
                                .insert_execution_receipt(&ExecutionReceiptRecord {
                                    mint: mint.to_string(),
                                    signature: None,
                                    route_label: execution_profile.route_label.clone(),
                                    status: "send_failed".to_string(),
                                    detail: err.to_string(),
                                    path: execution_profile.signal_path.clone(),
                                    quality_score: execution_profile.quality_score,
                                    urgency_score: execution_profile.urgency_score,
                                    execution_confidence: execution_profile.execution_confidence,
                                    priority_fee_micro_lamport: config.priority_fee_micro_lamport,
                                    jito_tip_lamports: effective_tip_lamports,
                                    zero_slot_tip_lamports: effective_tip_lamports,
                                    recorded_at_ms: current_time_ms(),
                                })
                                .await
                            {
                                warn!("execution send_failed receipt failed | {}", receipt_err);
                            }
                            tg.send(TgEvent::BuyFailed {
                                group_name: group.name.clone(),
                                mint: *mint,
                                reason: err.to_string(),
                            });
                        }
                    }
                }
                Err(err) => {
                    error!(
                        "Buy tx build failed [{}] {}: {}",
                        group.name,
                        &mint.to_string()[..12],
                        err
                    );
                    tg_stats.buy_failed.fetch_add(1, Ordering::Relaxed);
                    if let Err(receipt_err) = execution_db
                        .insert_execution_receipt(&ExecutionReceiptRecord {
                            mint: mint.to_string(),
                            signature: None,
                            route_label: execution_profile.route_label.clone(),
                            status: "build_failed".to_string(),
                            detail: err.to_string(),
                            path: execution_profile.signal_path.clone(),
                            quality_score: execution_profile.quality_score,
                            urgency_score: execution_profile.urgency_score,
                            execution_confidence: execution_profile.execution_confidence,
                            priority_fee_micro_lamport: config.priority_fee_micro_lamport,
                            jito_tip_lamports: effective_tip_lamports,
                            zero_slot_tip_lamports: effective_tip_lamports,
                            recorded_at_ms: current_time_ms(),
                        })
                        .await
                    {
                        warn!("execution build_failed receipt failed | {}", receipt_err);
                    }
                }
            }
        }
        Err(err) => {
            warn!(
                "Buy skipped [{}] {}: {}",
                group.name,
                &mint.to_string()[..12],
                err
            );
            if let Err(receipt_err) = execution_db
                .insert_execution_receipt(&ExecutionReceiptRecord {
                    mint: mint.to_string(),
                    signature: None,
                    route_label: execution_profile.route_label.clone(),
                    status: "quote_failed".to_string(),
                    detail: err.to_string(),
                    path: execution_profile.signal_path.clone(),
                    quality_score: execution_profile.quality_score,
                    urgency_score: execution_profile.urgency_score,
                    execution_confidence: execution_profile.execution_confidence,
                    priority_fee_micro_lamport: config.priority_fee_micro_lamport,
                    jito_tip_lamports: effective_tip_lamports,
                    zero_slot_tip_lamports: effective_tip_lamports,
                    recorded_at_ms: current_time_ms(),
                })
                .await
            {
                warn!("execution quote_failed receipt failed | {}", receipt_err);
            }
        }
    }
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,solana_copy_trader=debug".into()),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_ansi(true)
        .init();
}
