use crate::config::{
    classify_stream_endpoint, infer_stream_region, same_stream_endpoint, stream_provider, AppConfig,
};
use crate::filter::{FeedFirstHitRecord, FeedHealthRecord, FilterDb};
use crate::scanner::failover::{
    FailoverController, FeedFirstHitEvent, FeedHealthEvent, FeedRuntimeSnapshot,
};
use crate::scanner::feed::{FeedEndpoint, FeedKind};
use crate::scanner::raw_event::raw_event_to_scanner_event;
use crate::scanner::{
    decoder, NewToken, PumpBuyEvent, ScannerEvent, PUMP_PROGRAM_ID, SCANNER_BUY_CHANNEL_CAPACITY,
    SCANNER_NEW_TOKEN_CHANNEL_CAPACITY,
};
use anyhow::{Context, Result};
use futures::{Sink, SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout, Instant};
use tonic::transport::ClientTlsConfig;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

const SCANNER_DEDUP_TTL_MS: u64 = 120_000;
const SCANNER_DEDUP_MAX_KEYS: usize = 16_384;
const SCANNER_FAST_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const SCANNER_KEEPALIVE_SECS: u64 = 10;
const SCANNER_LIVE_HEARTBEAT_SECS: u64 = 30;

#[derive(Debug, Clone)]
struct SeenSignal {
    event_type: &'static str,
    mint: String,
    signature: String,
    slot: u64,
    first_feed_source: String,
    first_seen_ms: u64,
    last_seen_ms: u64,
    sources: HashSet<String>,
}

#[derive(Default)]
struct LiveDispatchMetrics {
    new_token_count: AtomicU64,
    buy_count: AtomicU64,
    dropped_buy_count: AtomicU64,
}

pub async fn start(
    cfg: Arc<AppConfig>,
    _runtime_db: Option<FilterDb>,
    _analytics_db: Option<FilterDb>,
    new_token_tx: mpsc::Sender<NewToken>,
    buy_tx: mpsc::Sender<PumpBuyEvent>,
) -> Result<()> {
    let processed_endpoints = build_processed_endpoints(cfg.as_ref());
    let processed_labels = processed_endpoints
        .iter()
        .map(format_endpoint_descriptor)
        .collect::<Vec<_>>()
        .join(",");
    if processed_endpoints.is_empty() {
        anyhow::bail!("scanner processed feed list is empty");
    }

    info!(
        "scanner: starting fast processed feeds | mode={} | processed_feeds={} [{}] | dedup_ttl_ms={} | dedup_max_keys={} | live_path=direct_dispatch",
        cfg.scanner_mode,
        processed_endpoints.len(),
        if processed_labels.is_empty() { "-" } else { processed_labels.as_str() },
        SCANNER_DEDUP_TTL_MS,
        SCANNER_DEDUP_MAX_KEYS,
    );
    if cfg.scanner_mode != crate::scanner::feed::ScannerMode::ProcessedOnly {
        warn!(
            "scanner: fast sniper path is forcing processed feeds only | configured_mode={} | deshred_feed_ignored={}",
            cfg.scanner_mode,
            cfg.scanner_deshred_grpc_url.is_some(),
        );
    }

    let live_metrics = Arc::new(LiveDispatchMetrics::default());
    let seen = Arc::new(StdMutex::new(FastSeenCache::new(now_ms())));

    if SCANNER_LIVE_HEARTBEAT_SECS > 0 {
        let heartbeat_cfg = cfg.clone();
        let heartbeat_metrics = live_metrics.clone();
        let heartbeat_new_token_tx = new_token_tx.clone();
        let heartbeat_buy_tx = buy_tx.clone();
        tokio::spawn(async move {
            live_dispatch_heartbeat_loop(
                heartbeat_cfg,
                heartbeat_metrics,
                heartbeat_new_token_tx,
                heartbeat_buy_tx,
            )
            .await;
        });
    }

    let mut task_set = JoinSet::new();
    for endpoint in processed_endpoints {
        let cfg_clone = cfg.clone();
        let endpoint_seen = seen.clone();
        let endpoint_metrics = live_metrics.clone();
        let endpoint_new_token_tx = new_token_tx.clone();
        let endpoint_buy_tx = buy_tx.clone();
        task_set.spawn(async move {
            start_processed_feed_loop_fast(
                cfg_clone,
                endpoint,
                endpoint_seen,
                endpoint_metrics,
                endpoint_new_token_tx,
                endpoint_buy_tx,
            )
            .await
        });
    }

    while let Some(joined) = task_set.join_next().await {
        match joined {
            Ok(Ok(())) => {
                warn!("scanner: processed feed task exited because output channel closed");
                return Ok(());
            }
            Ok(Err(err)) => {
                error!("scanner: processed feed task exited | {}", err);
            }
            Err(err) => {
                error!("scanner: processed feed task panicked | {}", err);
            }
        }
    }

    Ok(())
}

struct FastSeenCache {
    entries: HashMap<String, SeenSignal>,
    last_cleanup_ms: u64,
}

impl FastSeenCache {
    fn new(now_ms: u64) -> Self {
        Self {
            entries: HashMap::new(),
            last_cleanup_ms: now_ms,
        }
    }
}

fn should_forward_live_event(
    cache: &StdMutex<FastSeenCache>,
    event: &ScannerEvent,
    now_ms: u64,
) -> bool {
    let (event_key, event_type, mint, signature, slot, feed_source) = event_identity(event);
    let mut guard = match cache.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };

    if now_ms.saturating_sub(guard.last_cleanup_ms) >= 30_000
        || guard.entries.len() > SCANNER_DEDUP_MAX_KEYS
    {
        guard
            .entries
            .retain(|_, entry| now_ms.saturating_sub(entry.last_seen_ms) <= SCANNER_DEDUP_TTL_MS);
        guard.last_cleanup_ms = now_ms;
    }

    match guard.entries.get_mut(&event_key) {
        Some(existing) => {
            existing.last_seen_ms = now_ms;
            existing.sources.insert(feed_source.to_string());
            false
        }
        None => {
            let mut sources = HashSet::new();
            sources.insert(feed_source.to_string());
            guard.entries.insert(
                event_key,
                SeenSignal {
                    event_type,
                    mint: mint.to_string(),
                    signature: signature.to_string(),
                    slot,
                    first_feed_source: feed_source.to_string(),
                    first_seen_ms: now_ms,
                    last_seen_ms: now_ms,
                    sources,
                },
            );
            true
        }
    }
}

async fn dispatch_live_event(
    cfg: &AppConfig,
    event: ScannerEvent,
    new_token_tx: &mpsc::Sender<NewToken>,
    buy_tx: &mpsc::Sender<PumpBuyEvent>,
    metrics: &LiveDispatchMetrics,
) -> Result<bool> {
    match event {
        ScannerEvent::NewToken(token) => {
            persist_scanner_live_token(cfg, &token).await;
            metrics.new_token_count.fetch_add(1, Ordering::Relaxed);
            Ok(new_token_tx.send(token).await.is_ok())
        }
        ScannerEvent::Buy(buy) => {
            metrics.buy_count.fetch_add(1, Ordering::Relaxed);
            match buy_tx.try_send(buy) {
                Ok(()) => Ok(true),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    let dropped = metrics.dropped_buy_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if dropped == 1 || dropped % 100 == 0 {
                        warn!(
                            "scanner: buy channel full; dropping live buy events | dropped_total={}",
                            dropped
                        );
                    }
                    Ok(true)
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Ok(false),
            }
        }
    }
}

async fn persist_scanner_live_token(cfg: &AppConfig, token: &NewToken) {
    let record = scanner_live_token_record(token);
    if let Err(err) = append_jsonl(&cfg.scanner_live_tokens_file, &record).await {
        warn!("scanner: scanner_live_tokens append failed | {}", err);
    }
    if cfg.scanned_tokens_file != cfg.scanner_live_tokens_file {
        if let Err(err) = append_jsonl(&cfg.scanned_tokens_file, &record).await {
            warn!("scanner: scanned_tokens append failed | {}", err);
        }
    }
}

fn scanner_live_token_record(token: &NewToken) -> Value {
    json!({
        "detected_at_ms": token.detected_at_ms,
        "mint": &token.mint,
        "symbol": &token.symbol,
        "name": &token.name,
        "creator": &token.creator,
        "bonding_curve": &token.bonding_curve,
        "signature": &token.signature,
        "slot": token.slot,
        "uri": &token.uri,
        "is_v2": token.is_v2,
        "feed_source": &token.feed_source,
    })
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
        .with_context(|| format!("open scanner output file failed: {}", path_ref.display()))?;

    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    file.write_all(&line).await?;
    Ok(())
}

async fn live_dispatch_heartbeat_loop(
    cfg: Arc<AppConfig>,
    metrics: Arc<LiveDispatchMetrics>,
    new_token_tx: mpsc::Sender<NewToken>,
    buy_tx: mpsc::Sender<PumpBuyEvent>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(SCANNER_LIVE_HEARTBEAT_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;

    let mut prev_new_tokens = 0u64;
    let mut prev_buys = 0u64;
    let mut prev_dropped_buys = 0u64;
    loop {
        interval.tick().await;
        let new_tokens = metrics.new_token_count.load(Ordering::Relaxed);
        let buys = metrics.buy_count.load(Ordering::Relaxed);
        let dropped_buys = metrics.dropped_buy_count.load(Ordering::Relaxed);
        info!(
            "scanner: live heartbeat | mode={} | new_token_live_count={} (+{}) | buy_live_count={} (+{}) | buy_dropped_total={} (+{}) | new_token_queue_depth={} | buy_queue_depth={} | live_file={} | scanned_file={}",
            cfg.scanner_mode,
            new_tokens,
            new_tokens.saturating_sub(prev_new_tokens),
            buys,
            buys.saturating_sub(prev_buys),
            dropped_buys,
            dropped_buys.saturating_sub(prev_dropped_buys),
            SCANNER_NEW_TOKEN_CHANNEL_CAPACITY.saturating_sub(new_token_tx.capacity()),
            SCANNER_BUY_CHANNEL_CAPACITY.saturating_sub(buy_tx.capacity()),
            cfg.scanner_live_tokens_file,
            cfg.scanned_tokens_file,
        );
        prev_new_tokens = new_tokens;
        prev_buys = buys;
        prev_dropped_buys = dropped_buys;
    }
}

fn build_processed_endpoints(cfg: &AppConfig) -> Vec<FeedEndpoint> {
    if !cfg.scanner_mode.allows_processed() {
        return Vec::new();
    }

    let mut endpoints = Vec::new();
    endpoints.push(FeedEndpoint::processed(
        cfg.scanner_primary_feed_label.clone(),
        cfg.scanner_grpc_url.clone(),
        cfg.scanner_grpc_token.clone(),
    ));

    if let Some(url) = cfg.scanner_secondary_grpc_url.clone() {
        if same_stream_endpoint(&cfg.scanner_grpc_url, &url) {
            warn!(
                "scanner: skipping duplicate processed fallback | primary={} | secondary={}",
                cfg.scanner_grpc_url, url
            );
        } else {
            endpoints.push(FeedEndpoint::processed(
                cfg.scanner_secondary_feed_label.clone(),
                url,
                cfg.scanner_secondary_grpc_token
                    .clone()
                    .or_else(|| cfg.scanner_grpc_token.clone()),
            ));
        }
    }

    endpoints
}

fn format_endpoint_descriptor(endpoint: &FeedEndpoint) -> String {
    let region = infer_stream_region(&endpoint.url).unwrap_or("-");
    format!(
        "{}:{}:{}@{}",
        endpoint.label,
        stream_provider(&endpoint.url),
        classify_stream_endpoint(&endpoint.url),
        region
    )
}

fn build_deshred_endpoint(cfg: &AppConfig) -> Option<FeedEndpoint> {
    if !cfg.scanner_mode.allows_deshred() {
        return None;
    }

    cfg.scanner_deshred_grpc_url.clone().map(|url| {
        FeedEndpoint::deshred(
            cfg.scanner_deshred_feed_label.clone(),
            url,
            cfg.scanner_deshred_grpc_token
                .clone()
                .or_else(|| cfg.scanner_grpc_token.clone()),
        )
    })
}

#[derive(Debug, Clone, Copy)]
enum ProcessedRetryClass {
    FastReconnect,
    Backoff,
}

fn classify_processed_retry(err: &anyhow::Error) -> ProcessedRetryClass {
    let message = err.to_string().to_ascii_lowercase();
    if message.contains("stream ended unexpectedly")
        || message.contains("idle timeout")
        || message.contains("transport error")
        || message.contains("connection reset")
    {
        ProcessedRetryClass::FastReconnect
    } else {
        ProcessedRetryClass::Backoff
    }
}

fn next_processed_retry_delay(current: Duration, class: ProcessedRetryClass) -> Duration {
    match class {
        ProcessedRetryClass::FastReconnect => {
            if current.is_zero() {
                Duration::from_secs(1)
            } else {
                (current * 2).min(SCANNER_FAST_RETRY_MAX_DELAY)
            }
        }
        ProcessedRetryClass::Backoff => {
            if current.is_zero() {
                Duration::from_secs(1)
            } else {
                (current * 2).min(Duration::from_secs(30))
            }
        }
    }
}

fn retry_delay_for_processed_error(
    retry_delay: Duration,
    err: &anyhow::Error,
) -> (Duration, ProcessedRetryClass) {
    let class = classify_processed_retry(err);
    let sleep_for = match class {
        ProcessedRetryClass::FastReconnect => retry_delay.min(SCANNER_FAST_RETRY_MAX_DELAY),
        ProcessedRetryClass::Backoff => retry_delay.min(Duration::from_secs(30)),
    };
    (sleep_for, class)
}

async fn should_forward_event(
    seen: &mut HashMap<String, SeenSignal>,
    event: &ScannerEvent,
    now_ms: u64,
    last_cleanup_ms: &mut u64,
    db: Option<&FilterDb>,
    cfg: &AppConfig,
    failover: &Arc<Mutex<FailoverController>>,
    replay_tx: &mpsc::Sender<ScannerEvent>,
) -> Result<bool> {
    if now_ms.saturating_sub(*last_cleanup_ms) >= 30_000 || seen.len() > SCANNER_DEDUP_MAX_KEYS {
        seen.retain(|_, entry| now_ms.saturating_sub(entry.last_seen_ms) <= SCANNER_DEDUP_TTL_MS);
        *last_cleanup_ms = now_ms;
    }

    let (event_key, event_type, mint, signature, slot, feed_source) = event_identity(event);
    match seen.get_mut(&event_key) {
        Some(existing) => {
            existing.last_seen_ms = now_ms;
            if existing.sources.insert(feed_source.to_string()) {
                if let Some(db) = db {
                    warn_if_feed_first_hit_write_failed(
                        db,
                        FeedFirstHitRecord {
                            event_key,
                            event_type: existing.event_type.to_string(),
                            mint: existing.mint.clone(),
                            signature: existing.signature.clone(),
                            slot: existing.slot,
                            first_feed_source: existing.first_feed_source.clone(),
                            first_seen_ms: existing.first_seen_ms,
                            last_feed_source: feed_source.to_string(),
                            last_seen_ms: now_ms,
                            distinct_source_count: existing.sources.len(),
                            lag_to_latest_ms: now_ms.saturating_sub(existing.first_seen_ms),
                        },
                    )
                    .await;
                }
            }
            Ok(false)
        }
        None => {
            let mut sources = HashSet::new();
            sources.insert(feed_source.to_string());
            let first_seen = SeenSignal {
                event_type,
                mint: mint.to_string(),
                signature: signature.to_string(),
                slot,
                first_feed_source: feed_source.to_string(),
                first_seen_ms: now_ms,
                last_seen_ms: now_ms,
                sources,
            };
            if let Some(db) = db {
                warn_if_feed_first_hit_write_failed(
                    db,
                    FeedFirstHitRecord {
                        event_key: event_key.clone(),
                        event_type: event_type.to_string(),
                        mint: mint.to_string(),
                        signature: signature.to_string(),
                        slot,
                        first_feed_source: feed_source.to_string(),
                        first_seen_ms: now_ms,
                        last_feed_source: feed_source.to_string(),
                        last_seen_ms: now_ms,
                        distinct_source_count: 1,
                        lag_to_latest_ms: 0,
                    },
                )
                .await;
            }
            observe_first_hit(
                failover,
                FeedFirstHitEvent::new(
                    event_key.clone(),
                    event_type,
                    mint.to_string(),
                    signature.to_string(),
                    slot,
                    feed_source.to_string(),
                    now_ms,
                ),
                db,
                cfg,
                replay_tx,
            )
            .await;
            seen.insert(event_key, first_seen);
            Ok(true)
        }
    }
}

async fn warn_if_feed_first_hit_write_failed(db: &FilterDb, record: FeedFirstHitRecord) {
    if let Err(err) = db.upsert_feed_first_hit(&record).await {
        warn!(
            "scanner: feed_first_hit write failed | event_key={} | {}",
            record.event_key, err
        );
    }
}

fn event_identity(event: &ScannerEvent) -> (String, &'static str, &str, &str, u64, &str) {
    match event {
        ScannerEvent::NewToken(token) => (
            format!("new:{}:{}", token.signature, token.mint),
            "new",
            token.mint.as_str(),
            token.signature.as_str(),
            token.slot,
            token.feed_source.as_str(),
        ),
        ScannerEvent::Buy(buy) => (
            format!("buy:{}:{}", buy.signature, buy.mint),
            "buy",
            buy.mint.as_str(),
            buy.signature.as_str(),
            buy.slot,
            buy.feed_source.as_str(),
        ),
    }
}

async fn start_processed_feed_loop_fast(
    cfg: Arc<AppConfig>,
    endpoint: FeedEndpoint,
    seen: Arc<StdMutex<FastSeenCache>>,
    metrics: Arc<LiveDispatchMetrics>,
    new_token_tx: mpsc::Sender<NewToken>,
    buy_tx: mpsc::Sender<PumpBuyEvent>,
) -> Result<()> {
    debug_assert_eq!(endpoint.kind, FeedKind::Processed);
    let mut retry_delay = Duration::from_secs(1);

    loop {
        info!(
            "scanner: connecting fast processed feed={} url={} profile={}",
            endpoint.label,
            endpoint.url,
            format_endpoint_descriptor(&endpoint),
        );
        match run_processed_stream_fast(
            cfg.as_ref(),
            &endpoint,
            &seen,
            metrics.as_ref(),
            &new_token_tx,
            &buy_tx,
        )
        .await
        {
            Ok(()) => {
                warn!(
                    "scanner: processed output channel closed | feed={}",
                    endpoint.label
                );
                return Ok(());
            }
            Err(err) => {
                let (sleep_for, retry_class) = retry_delay_for_processed_error(retry_delay, &err);
                error!(
                    "scanner: fast processed feed disconnected | feed={} | retry_class={:?} | retry_in_ms={} | {}",
                    endpoint.label,
                    retry_class,
                    sleep_for.as_millis(),
                    err
                );
                sleep(sleep_for).await;
                retry_delay = next_processed_retry_delay(sleep_for, retry_class);
            }
        }
    }
}

async fn run_processed_stream_fast(
    cfg: &AppConfig,
    endpoint: &FeedEndpoint,
    seen: &Arc<StdMutex<FastSeenCache>>,
    metrics: &LiveDispatchMetrics,
    new_token_tx: &mpsc::Sender<NewToken>,
    buy_tx: &mpsc::Sender<PumpBuyEvent>,
) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint.url.clone())?
        .x_token(endpoint.token.clone())?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(64 * 1024 * 1024)
        .connect()
        .await
        .context("scanner gRPC connect failed")?;

    let (subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(build_subscribe_request()))
        .await
        .context("scanner subscribe request failed")?;
    let keepalive_task = spawn_keepalive_task(
        endpoint.label.clone(),
        "processed",
        subscribe_tx,
        build_ping_request,
    );

    info!(
        "scanner: fast processed subscription ready | feed={} | program={} | idle_timeout_secs={}",
        endpoint.label, PUMP_PROGRAM_ID, cfg.scanner_idle_timeout_secs
    );

    let mut last_message_at = Instant::now();
    let idle_timeout = Duration::from_secs(cfg.scanner_idle_timeout_secs);

    let result = 'stream: loop {
        let next = timeout(Duration::from_secs(5), stream.next()).await;
        match next {
            Ok(Some(Ok(update))) => {
                last_message_at = Instant::now();
                match update.update_oneof {
                    Some(UpdateOneof::Transaction(tx_update)) => {
                        if let Some(tx_info) = tx_update.transaction.as_ref() {
                            for event in decoder::decode_transaction(
                                &endpoint.label,
                                tx_update.slot,
                                tx_info,
                            ) {
                                let should_forward =
                                    should_forward_live_event(seen.as_ref(), &event, now_ms());
                                if !should_forward {
                                    continue;
                                }
                                if !dispatch_live_event(cfg, event, new_token_tx, buy_tx, metrics)
                                    .await?
                                {
                                    break 'stream Ok(());
                                }
                            }
                        }
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        debug!("scanner: processed ping | feed={}", endpoint.label);
                    }
                    Some(UpdateOneof::Pong(_)) => {
                        debug!("scanner: processed pong | feed={}", endpoint.label);
                    }
                    Some(other) => {
                        debug!(
                            "scanner: ignored non-transaction processed update | feed={} | kind={:?}",
                            endpoint.label,
                            std::mem::discriminant(&other)
                        );
                    }
                    None => {}
                }
            }
            Ok(Some(Err(err))) => break Err(err).context("scanner stream read failed"),
            Ok(None) => break Err(anyhow::anyhow!("scanner stream ended unexpectedly")),
            Err(_) if last_message_at.elapsed() >= idle_timeout => {
                break Err(anyhow::anyhow!(
                    "scanner processed feed idle timeout | feed={} | idle_secs={}",
                    endpoint.label,
                    cfg.scanner_idle_timeout_secs
                ));
            }
            Err(_) => {}
        }
    };
    keepalive_task.abort();
    result
}

async fn start_processed_feed_loop(
    cfg: Arc<AppConfig>,
    endpoint: FeedEndpoint,
    tx: mpsc::Sender<ScannerEvent>,
    db: Option<FilterDb>,
    failover: Arc<Mutex<FailoverController>>,
) -> Result<()> {
    debug_assert_eq!(endpoint.kind, FeedKind::Processed);
    let mut retry_delay = Duration::from_secs(1);

    loop {
        record_feed_health(
            db.as_ref(),
            cfg.as_ref(),
            &endpoint,
            &failover,
            FeedHealthEvent::new(
                endpoint.label.clone(),
                endpoint.url.clone(),
                "connecting",
                "opening processed stream",
                now_ms(),
            ),
        )
        .await;
        info!(
            "scanner: connecting processed feed={} url={}",
            endpoint.label, endpoint.url
        );
        match run_processed_stream(cfg.as_ref(), &endpoint, &tx, db.as_ref(), &failover).await {
            Ok(()) => {
                record_feed_health(
                    db.as_ref(),
                    cfg.as_ref(),
                    &endpoint,
                    &failover,
                    FeedHealthEvent::new(
                        endpoint.label.clone(),
                        endpoint.url.clone(),
                        "closed",
                        "processed output channel closed",
                        now_ms(),
                    ),
                )
                .await;
                warn!(
                    "scanner: processed output channel closed | feed={}",
                    endpoint.label
                );
                return Ok(());
            }
            Err(err) => {
                let (sleep_for, retry_class) = retry_delay_for_processed_error(retry_delay, &err);
                record_feed_health(
                    db.as_ref(),
                    cfg.as_ref(),
                    &endpoint,
                    &failover,
                    FeedHealthEvent::new(
                        endpoint.label.clone(),
                        endpoint.url.clone(),
                        "disconnected",
                        err.to_string(),
                        now_ms(),
                    ),
                )
                .await;
                error!(
                    "scanner: processed feed disconnected | feed={} | retry_class={:?} | retry_in_ms={} | {}",
                    endpoint.label,
                    retry_class,
                    sleep_for.as_millis(),
                    err
                );
                sleep(sleep_for).await;
                retry_delay = next_processed_retry_delay(sleep_for, retry_class);
            }
        }
    }
}

async fn run_processed_stream(
    cfg: &AppConfig,
    endpoint: &FeedEndpoint,
    tx: &mpsc::Sender<ScannerEvent>,
    db: Option<&FilterDb>,
    failover: &Arc<Mutex<FailoverController>>,
) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint.url.clone())?
        .x_token(endpoint.token.clone())?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(64 * 1024 * 1024)
        .connect()
        .await
        .context("scanner gRPC connect failed")?;

    let (subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(build_subscribe_request()))
        .await
        .context("scanner subscribe request failed")?;
    let keepalive_task = spawn_keepalive_task(
        endpoint.label.clone(),
        "processed",
        subscribe_tx,
        build_ping_request,
    );

    record_feed_health(
        db,
        cfg,
        endpoint,
        failover,
        FeedHealthEvent::new(
            endpoint.label.clone(),
            endpoint.url.clone(),
            "ready",
            "processed subscription ready",
            now_ms(),
        ),
    )
    .await;
    info!(
        "scanner: processed subscription ready | feed={} | program={}",
        endpoint.label, PUMP_PROGRAM_ID
    );
    if let Some(db) = db {
        spawn_catchup_replay(cfg, db, tx, format!("processed_ready:{}", endpoint.label));
    }

    let mut last_message_at = Instant::now();
    let idle_timeout = Duration::from_secs(cfg.scanner_idle_timeout_secs);

    let result = 'stream: loop {
        let next = timeout(Duration::from_secs(5), stream.next()).await;
        match next {
            Ok(Some(Ok(update))) => {
                last_message_at = Instant::now();
                match update.update_oneof {
                    Some(UpdateOneof::Transaction(tx_update)) => {
                        if let Some(tx_info) = tx_update.transaction.as_ref() {
                            for event in decoder::decode_transaction(
                                &endpoint.label,
                                tx_update.slot,
                                tx_info,
                            ) {
                                if tx.send(event).await.is_err() {
                                    break 'stream Ok(());
                                }
                            }
                        }
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        debug!("scanner: processed ping | feed={}", endpoint.label);
                    }
                    Some(UpdateOneof::Pong(_)) => {
                        debug!("scanner: processed pong | feed={}", endpoint.label);
                    }
                    Some(other) => {
                        debug!(
                            "scanner: ignored non-transaction processed update | feed={} | kind={:?}",
                            endpoint.label,
                            std::mem::discriminant(&other)
                        );
                    }
                    None => {}
                }
            }
            Ok(Some(Err(err))) => break Err(err).context("scanner stream read failed"),
            Ok(None) => break Err(anyhow::anyhow!("scanner stream ended unexpectedly")),
            Err(_) if last_message_at.elapsed() >= idle_timeout => {
                break Err(anyhow::anyhow!(
                    "scanner processed feed idle timeout | feed={} | idle_secs={}",
                    endpoint.label,
                    cfg.scanner_idle_timeout_secs
                ));
            }
            Err(_) => {}
        }
    };
    keepalive_task.abort();
    result
}

async fn record_feed_health(
    db: Option<&FilterDb>,
    cfg: &AppConfig,
    endpoint: &FeedEndpoint,
    failover: &Arc<Mutex<FailoverController>>,
    event: FeedHealthEvent,
) {
    if !cfg.persist_feed_health {
        observe_health_change(failover, endpoint, &event, None).await;
        return;
    }

    let Some(db) = db else {
        observe_health_change(failover, endpoint, &event, None).await;
        return;
    };

    if let Err(err) = db
        .insert_feed_health(&FeedHealthRecord {
            feed_label: event.feed_label.clone(),
            feed_url: event.feed_url.clone(),
            status: event.status.clone(),
            detail: event.detail.clone(),
            ts_ms: event.ts_ms,
        })
        .await
    {
        warn!("scanner: insert feed health failed | {}", err);
    }
    observe_health_change(failover, endpoint, &event, Some(db)).await;
}

fn build_ping_request(id: i32) -> SubscribeRequest {
    SubscribeRequest {
        ping: Some(SubscribeRequestPing { id }),
        ..Default::default()
    }
}

fn spawn_keepalive_task<S, Request, F>(
    feed_label: String,
    feed_kind: &'static str,
    mut sink: S,
    mut build_request: F,
) -> tokio::task::JoinHandle<()>
where
    S: Sink<Request> + Unpin + Send + 'static,
    S::Error: std::fmt::Display + Send + Sync + 'static,
    Request: Send + 'static,
    F: FnMut(i32) -> Request + Send + 'static,
{
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_secs(SCANNER_KEEPALIVE_SECS.max(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;

        let mut ping_id = 1i32;
        loop {
            interval.tick().await;
            if let Err(err) = sink.send(build_request(ping_id)).await {
                warn!(
                    "scanner: {} keepalive send failed | feed={} | {}",
                    feed_kind, feed_label, err
                );
                break;
            }
            ping_id = ping_id.checked_add(1).unwrap_or(1);
        }
    })
}

fn build_subscribe_request() -> SubscribeRequest {
    let mut transactions = HashMap::new();
    transactions.insert(
        "pump_scanner".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            account_include: vec![PUMP_PROGRAM_ID.to_string()],
            account_exclude: vec![],
            account_required: vec![],
            signature: None,
        },
    );

    SubscribeRequest {
        transactions,
        commitment: Some(CommitmentLevel::Processed as i32),
        ping: None,
        ..Default::default()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn observe_health_change(
    failover: &Arc<Mutex<FailoverController>>,
    endpoint: &FeedEndpoint,
    event: &FeedHealthEvent,
    db: Option<&FilterDb>,
) {
    let change = {
        let mut guard = failover.lock().await;
        guard.observe_health(endpoint.kind, event)
    };
    if let Some(change) = change {
        log_selection_change(db, &change, &endpoint.url).await;
    }
}

async fn observe_first_hit(
    failover: &Arc<Mutex<FailoverController>>,
    event: FeedFirstHitEvent,
    db: Option<&FilterDb>,
    cfg: &AppConfig,
    replay_tx: &mpsc::Sender<ScannerEvent>,
) {
    let change = {
        let mut guard = failover.lock().await;
        guard.observe_first_hit(&event.feed_source, event.detected_at_ms)
    };
    if let Some(change) = change {
        if cfg.persist_feed_health {
            log_selection_change(db, &change, "").await;
        } else {
            info!(
                "scanner: preferred {:?} feed switched | prev={} | next={} | reason={}",
                change.kind,
                change.previous_label.as_deref().unwrap_or("-"),
                change.preferred_label.as_deref().unwrap_or("-"),
                change.reason,
            );
        }
        if let Some(db) = db {
            spawn_catchup_replay(
                cfg,
                db,
                replay_tx,
                format!(
                    "preferred_switch:{:?}:{}->{}",
                    change.kind,
                    change.previous_label.as_deref().unwrap_or("-"),
                    change.preferred_label.as_deref().unwrap_or("-")
                ),
            );
        }
    }
}

async fn log_selection_change(
    db: Option<&FilterDb>,
    change: &crate::scanner::failover::FeedSelectionChange,
    feed_url: &str,
) {
    info!(
        "scanner: preferred {:?} feed switched | prev={} | next={} | reason={}",
        change.kind,
        change.previous_label.as_deref().unwrap_or("-"),
        change.preferred_label.as_deref().unwrap_or("-"),
        change.reason,
    );
    let Some(db) = db else {
        return;
    };
    let detail = format!(
        "prev={} next={} reason={}",
        change.previous_label.as_deref().unwrap_or("-"),
        change.preferred_label.as_deref().unwrap_or("-"),
        change.reason
    );
    if let Err(err) = db
        .insert_feed_health(&FeedHealthRecord {
            feed_label: format!("preferred_{:?}", change.kind).to_ascii_lowercase(),
            feed_url: feed_url.to_string(),
            status: "preferred_switch".to_string(),
            detail,
            ts_ms: change.ts_ms,
        })
        .await
    {
        warn!("scanner: insert preferred feed health failed | {}", err);
    }
}

pub(crate) async fn replay_pending_raw_events(
    cfg: &AppConfig,
    db: &FilterDb,
    tx: &mpsc::Sender<ScannerEvent>,
    reason: &str,
) -> Result<usize> {
    if cfg.scanner_catchup_window_ms == 0 || cfg.scanner_catchup_max_events == 0 {
        return Ok(0);
    }

    let to_ms = now_ms();
    let from_ms = to_ms.saturating_sub(cfg.scanner_catchup_window_ms);
    let raw_events = db.list_raw_events_window(from_ms, to_ms).await?;
    if raw_events.is_empty() {
        return Ok(0);
    }
    let decided_mints: HashSet<String> = db
        .list_filter_results_window(from_ms, to_ms)
        .await?
        .into_iter()
        .map(|record| record.mint)
        .collect();
    let mut unresolved = raw_events
        .into_iter()
        .filter(|record| !decided_mints.contains(&record.mint))
        .collect::<Vec<_>>();
    unresolved.sort_by_key(|record| record.recorded_at_ms);
    if unresolved.len() > cfg.scanner_catchup_max_events {
        let skip = unresolved
            .len()
            .saturating_sub(cfg.scanner_catchup_max_events);
        unresolved.drain(0..skip);
    }

    let mut replayed = 0usize;
    for record in unresolved {
        if let Some(event) = raw_event_to_scanner_event(&record)? {
            if tx.send(event).await.is_err() {
                break;
            }
            replayed = replayed.saturating_add(1);
        }
    }

    if replayed > 0 {
        info!(
            "scanner: replayed {} unresolved raw events from last {}ms | reason={}",
            replayed, cfg.scanner_catchup_window_ms, reason
        );
        if cfg.persist_feed_health {
            let _ = db
                .insert_feed_health(&FeedHealthRecord {
                    feed_label: "scanner_catchup".to_string(),
                    feed_url: String::new(),
                    status: "catchup_replay".to_string(),
                    detail: format!(
                        "reason={} replayed={} from_ms={} to_ms={}",
                        reason, replayed, from_ms, to_ms
                    ),
                    ts_ms: now_ms(),
                })
                .await;
        }
    }

    Ok(replayed)
}

fn spawn_catchup_replay(
    cfg: &AppConfig,
    db: &FilterDb,
    tx: &mpsc::Sender<ScannerEvent>,
    reason: String,
) {
    if cfg.scanner_catchup_window_ms == 0 || cfg.scanner_catchup_max_events == 0 {
        return;
    }
    let cfg = cfg.clone();
    let db = db.clone();
    let tx = tx.clone();
    tokio::spawn(async move {
        if let Err(err) = replay_pending_raw_events(&cfg, &db, &tx, &reason).await {
            warn!(
                "scanner: catchup replay failed | reason={} | {}",
                reason, err
            );
        }
    });
}

async fn feed_runtime_snapshot_loop(
    cfg: Arc<AppConfig>,
    db: Option<FilterDb>,
    failover: Arc<Mutex<FailoverController>>,
) {
    if cfg.scanner_health_snapshot_secs == 0 {
        return;
    }

    let mut tick =
        tokio::time::interval(Duration::from_secs(cfg.scanner_health_snapshot_secs.max(5)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await;

    loop {
        tick.tick().await;
        let snapshots = {
            let guard = failover.lock().await;
            guard.runtime_snapshots(now_ms())
        };
        if snapshots.is_empty() {
            continue;
        }
        persist_feed_runtime_snapshots(db.as_ref(), cfg.as_ref(), &snapshots).await;
    }
}

async fn persist_feed_runtime_snapshots(
    db: Option<&FilterDb>,
    cfg: &AppConfig,
    snapshots: &[FeedRuntimeSnapshot],
) {
    for snapshot in snapshots {
        let detail = format!(
            "kind={:?} preferred={} score={} first_hits={} stale_ms={} last_status_ms={} last_first_hit_ms={} detail={}",
            snapshot.kind,
            snapshot.is_preferred,
            snapshot.score,
            snapshot.first_hit_count,
            snapshot.stale_ms,
            snapshot.last_status_ms,
            snapshot.last_first_hit_ms,
            snapshot.detail,
        );
        info!(
            "scanner: feed runtime snapshot | label={} | status={:?} | preferred={} | score={} | first_hits={} | stale_ms={}",
            snapshot.feed_label,
            snapshot.status,
            snapshot.is_preferred,
            snapshot.score,
            snapshot.first_hit_count,
            snapshot.stale_ms,
        );
        if !cfg.persist_feed_health {
            continue;
        }
        let Some(db) = db else {
            continue;
        };
        if let Err(err) = db
            .insert_feed_health(&FeedHealthRecord {
                feed_label: snapshot.feed_label.clone(),
                feed_url: snapshot.feed_url.clone(),
                status: "runtime_snapshot".to_string(),
                detail,
                ts_ms: now_ms(),
            })
            .await
        {
            warn!("scanner: insert runtime feed snapshot failed | {}", err);
        }
    }
}
