use crate::config::AppConfig;
use crate::filter::{FeedFirstHitRecord, FeedHealthRecord, FilterDb};
use crate::scanner::failover::{FailoverController, FeedFirstHitEvent, FeedHealthEvent};
use crate::scanner::feed::{FeedEndpoint, FeedKind};
use crate::scanner::raw_event::raw_event_to_scanner_event;
use crate::scanner::{decoder, deshred, ScannerEvent, PUMP_PROGRAM_ID};
use anyhow::{Context, Result};
use futures::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, timeout, Instant};
use tonic::transport::ClientTlsConfig;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};

const SCANNER_DEDUP_TTL_MS: u64 = 120_000;
const SCANNER_DEDUP_MAX_KEYS: usize = 16_384;

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

pub async fn start(cfg: Arc<AppConfig>, tx: mpsc::Sender<ScannerEvent>) -> Result<()> {
    let processed_endpoints = build_processed_endpoints(cfg.as_ref());
    let deshred_endpoint = build_deshred_endpoint(cfg.as_ref());

    match cfg.scanner_mode {
        crate::scanner::feed::ScannerMode::ProcessedOnly if processed_endpoints.is_empty() => {
            anyhow::bail!("SCANNER_MODE=processed-only but no processed feed is configured");
        }
        crate::scanner::feed::ScannerMode::DeshredOnly if deshred_endpoint.is_none() => {
            anyhow::bail!("SCANNER_MODE=deshred-only but no deshred feed is configured");
        }
        crate::scanner::feed::ScannerMode::Hybrid
            if processed_endpoints.is_empty() || deshred_endpoint.is_none() =>
        {
            anyhow::bail!(
                "SCANNER_MODE=hybrid requires both processed and deshred feeds to be configured"
            );
        }
        _ => {}
    }

    if processed_endpoints.is_empty() && deshred_endpoint.is_none() {
        anyhow::bail!("scanner feed list is empty");
    }

    let db = match FilterDb::new(&cfg.filter_db_path).await {
        Ok(db) => Some(db),
        Err(err) => {
            warn!(
                "scanner: failed to open filter db for feed metrics | {}",
                err
            );
            None
        }
    };
    let failover = Arc::new(Mutex::new(FailoverController::new(
        cfg.scanner_failover_stale_ms,
    )));

    let (raw_tx, mut raw_rx) = mpsc::channel::<ScannerEvent>(4096);

    if let Some(db) = db.as_ref() {
        replay_pending_raw_events(cfg.as_ref(), db, &raw_tx).await?;
    }

    for endpoint in processed_endpoints {
        let cfg_clone = cfg.clone();
        let tx_clone = raw_tx.clone();
        let db_clone = db.clone();
        let failover_clone = failover.clone();
        tokio::spawn(async move {
            if let Err(err) =
                start_processed_feed_loop(cfg_clone, endpoint, tx_clone, db_clone, failover_clone)
                    .await
            {
                error!("scanner: processed feed task exited | {}", err);
            }
        });
    }

    if let Some(endpoint) = deshred_endpoint {
        let cfg_clone = cfg.clone();
        let tx_clone = raw_tx.clone();
        let db_clone = db.clone();
        let failover_clone = failover.clone();
        tokio::spawn(async move {
            if let Err(err) =
                deshred::start_feed_loop(cfg_clone, endpoint, tx_clone, db_clone, failover_clone)
                    .await
            {
                error!("scanner: deshred feed task exited | {}", err);
            }
        });
    }

    drop(raw_tx);

    let mut seen = HashMap::<String, SeenSignal>::new();
    let mut last_cleanup_ms = now_ms();
    while let Some(event) = raw_rx.recv().await {
        let now = now_ms();
        if should_forward_event(
            &mut seen,
            &event,
            now,
            &mut last_cleanup_ms,
            db.as_ref(),
            cfg.as_ref(),
            failover.as_ref(),
        )
        .await?
        {
            if tx.send(event).await.is_err() {
                return Ok(());
            }
        }
    }

    Ok(())
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
        endpoints.push(FeedEndpoint::processed(
            cfg.scanner_secondary_feed_label.clone(),
            url,
            cfg.scanner_secondary_grpc_token
                .clone()
                .or_else(|| cfg.scanner_grpc_token.clone()),
        ));
    }

    endpoints
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

async fn should_forward_event(
    seen: &mut HashMap<String, SeenSignal>,
    event: &ScannerEvent,
    now_ms: u64,
    last_cleanup_ms: &mut u64,
    db: Option<&FilterDb>,
    cfg: &AppConfig,
    failover: &Arc<Mutex<FailoverController>>,
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
                    db.upsert_feed_first_hit(&FeedFirstHitRecord {
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
                    })
                    .await?;
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
                db.upsert_feed_first_hit(&FeedFirstHitRecord {
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
                })
                .await?;
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
            )
            .await;
            seen.insert(event_key, first_seen);
            Ok(true)
        }
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

async fn start_processed_feed_loop(
    cfg: Arc<AppConfig>,
    endpoint: FeedEndpoint,
    tx: mpsc::Sender<ScannerEvent>,
    db: Option<FilterDb>,
    failover: Arc<Mutex<FailoverController>>,
) -> Result<()> {
    debug_assert_eq!(endpoint.kind, FeedKind::Processed);
    let mut retry_delay = Duration::from_secs(1);
    const MAX_DELAY: Duration = Duration::from_secs(30);

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
        match run_processed_stream(cfg.as_ref(), &endpoint, &tx, db.as_ref()).await {
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
                    "scanner: processed feed disconnected | feed={} | retry_in={}s | {}",
                    endpoint.label,
                    retry_delay.as_secs(),
                    err
                );
                sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(MAX_DELAY);
            }
        }
    }
}

async fn run_processed_stream(
    cfg: &AppConfig,
    endpoint: &FeedEndpoint,
    tx: &mpsc::Sender<ScannerEvent>,
    db: Option<&FilterDb>,
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

    let (_, mut stream) = client
        .subscribe_with_request(Some(build_subscribe_request()))
        .await
        .context("scanner subscribe request failed")?;

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

    let mut last_message_at = Instant::now();
    let idle_timeout = Duration::from_secs(cfg.scanner_idle_timeout_secs);

    loop {
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
                                    return Ok(());
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
            Ok(Some(Err(err))) => return Err(err).context("scanner stream read failed"),
            Ok(None) => anyhow::bail!("scanner stream ended unexpectedly"),
            Err(_) if last_message_at.elapsed() >= idle_timeout => {
                anyhow::bail!(
                    "scanner processed feed idle timeout | feed={} | idle_secs={}",
                    endpoint.label,
                    cfg.scanner_idle_timeout_secs
                );
            }
            Err(_) => {}
        }
    }
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
            feed_label: event.feed_label,
            feed_url: event.feed_url,
            status: event.status,
            detail: event.detail,
            ts_ms: event.ts_ms,
        })
        .await
    {
        warn!("scanner: insert feed health failed | {}", err);
    }
    observe_health_change(failover, endpoint, &event, Some(db)).await;
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

async fn replay_pending_raw_events(
    cfg: &AppConfig,
    db: &FilterDb,
    tx: &mpsc::Sender<ScannerEvent>,
) -> Result<()> {
    if cfg.scanner_catchup_window_ms == 0 || cfg.scanner_catchup_max_events == 0 {
        return Ok(());
    }

    let to_ms = now_ms();
    let from_ms = to_ms.saturating_sub(cfg.scanner_catchup_window_ms);
    let raw_events = db.list_raw_events_window(from_ms, to_ms).await?;
    if raw_events.is_empty() {
        return Ok(());
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
            "scanner: replayed {} unresolved raw events from last {}ms before live feed start",
            replayed, cfg.scanner_catchup_window_ms
        );
    }

    Ok(())
}
