use crate::config::AppConfig;
use crate::scanner::feed::FeedEndpoint;
use crate::scanner::{decoder, ScannerEvent, PUMP_PROGRAM_ID};
use anyhow::{Context, Result};
use futures::StreamExt;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant};
use tonic::transport::ClientTlsConfig;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};

pub async fn start(cfg: Arc<AppConfig>, tx: mpsc::Sender<ScannerEvent>) -> Result<()> {
    let endpoints = build_endpoints(cfg.as_ref());
    let Some(primary) = endpoints.first().cloned() else {
        anyhow::bail!("scanner feed list is empty");
    };

    for endpoint in endpoints.into_iter().skip(1) {
        let cfg_clone = cfg.clone();
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            if let Err(err) = start_feed_loop(cfg_clone, endpoint, tx_clone).await {
                error!("scanner: secondary feed task exited | {}", err);
            }
        });
    }

    start_feed_loop(cfg, primary, tx).await
}

fn build_endpoints(cfg: &AppConfig) -> Vec<FeedEndpoint> {
    let mut endpoints = vec![FeedEndpoint::new(
        cfg.scanner_primary_feed_label.clone(),
        cfg.scanner_grpc_url.clone(),
        cfg.scanner_grpc_token.clone(),
    )];

    if let Some(url) = cfg.scanner_secondary_grpc_url.clone() {
        endpoints.push(FeedEndpoint::new(
            cfg.scanner_secondary_feed_label.clone(),
            url,
            cfg.scanner_secondary_grpc_token.clone().or_else(|| cfg.scanner_grpc_token.clone()),
        ));
    }

    endpoints
}

async fn start_feed_loop(
    cfg: Arc<AppConfig>,
    endpoint: FeedEndpoint,
    tx: mpsc::Sender<ScannerEvent>,
) -> Result<()> {
    let mut retry_delay = Duration::from_secs(1);
    const MAX_DELAY: Duration = Duration::from_secs(30);

    loop {
        info!(
            "scanner: connecting feed={} url={}",
            endpoint.label, endpoint.url
        );
        match run_stream(cfg.as_ref(), &endpoint, &tx).await {
            Ok(()) => {
                warn!("scanner: output channel closed | feed={}", endpoint.label);
                return Ok(());
            }
            Err(err) => {
                error!(
                    "scanner: feed disconnected | feed={} | retry_in={}s | {}",
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

    info!(
        "scanner: subscription ready | feed={} | program={}",
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
                            for event in
                                decoder::decode_transaction(&endpoint.label, tx_update.slot, tx_info)
                            {
                                if tx.send(event).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        debug!("scanner: ping | feed={}", endpoint.label);
                    }
                    Some(UpdateOneof::Pong(_)) => {
                        debug!("scanner: pong | feed={}", endpoint.label);
                    }
                    Some(other) => {
                        debug!(
                            "scanner: ignored non-transaction update | feed={} | kind={:?}",
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
                    "scanner feed idle timeout | feed={} | idle_secs={}",
                    endpoint.label,
                    cfg.scanner_idle_timeout_secs
                );
            }
            Err(_) => {}
        }
    }
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
