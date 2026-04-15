use crate::config::AppConfig;
use crate::filter::{FeedHealthRecord, FilterDb};
use crate::scanner::failover::FeedHealthEvent;
use crate::scanner::feed::FeedEndpoint;
use crate::scanner::{decoder, ScannerEvent, PUMP_PROGRAM_ID};
use anyhow::{Context, Result};
use futures::StreamExt;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant};
use tonic::transport::ClientTlsConfig;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update_deshred::UpdateOneof, SubscribeDeshredRequest,
    SubscribeRequestFilterDeshredTransactions,
};

pub async fn start_feed_loop(
    cfg: std::sync::Arc<AppConfig>,
    endpoint: FeedEndpoint,
    tx: mpsc::Sender<ScannerEvent>,
    db: Option<FilterDb>,
) -> Result<()> {
    let mut retry_delay = Duration::from_secs(1);
    const MAX_DELAY: Duration = Duration::from_secs(30);

    loop {
        record_feed_health(
            db.as_ref(),
            cfg.as_ref(),
            FeedHealthEvent::new(
                endpoint.label.clone(),
                endpoint.url.clone(),
                "connecting",
                "opening deshred stream",
                now_ms(),
            ),
        )
        .await;
        info!(
            "scanner: connecting deshred feed={} url={}",
            endpoint.label, endpoint.url
        );
        match run_stream(cfg.as_ref(), &endpoint, &tx, db.as_ref()).await {
            Ok(()) => {
                record_feed_health(
                    db.as_ref(),
                    cfg.as_ref(),
                    FeedHealthEvent::new(
                        endpoint.label.clone(),
                        endpoint.url.clone(),
                        "closed",
                        "deshred output channel closed",
                        now_ms(),
                    ),
                )
                .await;
                warn!(
                    "scanner: deshred output channel closed | feed={}",
                    endpoint.label
                );
                return Ok(());
            }
            Err(err) => {
                record_feed_health(
                    db.as_ref(),
                    cfg.as_ref(),
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
                    "scanner: deshred feed disconnected | feed={} | retry_in={}s | {}",
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

async fn run_stream(
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
        .context("scanner deshred gRPC connect failed")?;

    let (_, mut stream) = client
        .subscribe_deshred_with_request(Some(build_subscribe_request()))
        .await
        .context("scanner deshred subscribe request failed")?;

    record_feed_health(
        db,
        cfg,
        FeedHealthEvent::new(
            endpoint.label.clone(),
            endpoint.url.clone(),
            "ready",
            "deshred subscription ready",
            now_ms(),
        ),
    )
    .await;
    info!(
        "scanner: deshred subscription ready | feed={} | program={}",
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
                    Some(UpdateOneof::DeshredTransaction(tx_update)) => {
                        if let Some(tx_info) = tx_update.transaction.as_ref() {
                            for event in decoder::decode_deshred_transaction(
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
                        debug!("scanner: deshred ping | feed={}", endpoint.label);
                    }
                    Some(UpdateOneof::Pong(_)) => {
                        debug!("scanner: deshred pong | feed={}", endpoint.label);
                    }
                    None => {}
                }
            }
            Ok(Some(Err(err))) => return Err(err).context("scanner deshred stream read failed"),
            Ok(None) => anyhow::bail!("scanner deshred stream ended unexpectedly"),
            Err(_) if last_message_at.elapsed() >= idle_timeout => {
                anyhow::bail!(
                    "scanner deshred idle timeout | feed={} | idle_secs={}",
                    endpoint.label,
                    cfg.scanner_idle_timeout_secs
                );
            }
            Err(_) => {}
        }
    }
}

async fn record_feed_health(db: Option<&FilterDb>, cfg: &AppConfig, event: FeedHealthEvent) {
    if !cfg.persist_feed_health {
        return;
    }

    let Some(db) = db else {
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
        warn!("scanner: insert deshred feed health failed | {}", err);
    }
}

fn build_subscribe_request() -> SubscribeDeshredRequest {
    let mut deshred_transactions = HashMap::new();
    deshred_transactions.insert(
        "pump_scanner_deshred".to_string(),
        SubscribeRequestFilterDeshredTransactions {
            vote: Some(false),
            account_include: vec![PUMP_PROGRAM_ID.to_string()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeDeshredRequest {
        deshred_transactions,
        slots: HashMap::new(),
        ping: None,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
