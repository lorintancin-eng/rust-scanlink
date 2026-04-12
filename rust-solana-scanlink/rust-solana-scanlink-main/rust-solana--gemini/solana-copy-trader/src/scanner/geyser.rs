use crate::config::AppConfig;
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
    let mut retry_delay = Duration::from_secs(1);
    const MAX_DELAY: Duration = Duration::from_secs(30);

    loop {
        info!("扫链：正在连接 gRPC 节点 {}", cfg.scanner_grpc_url);
        match run_stream(&cfg, &tx).await {
            Ok(()) => {
                warn!("扫链：输出通道已关闭，停止扫链任务");
                return Ok(());
            }
            Err(err) => {
                error!(
                    "扫链：连接断开，{} 秒后重连 | {}",
                    retry_delay.as_secs(),
                    err
                );
                sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(MAX_DELAY);
            }
        }
    }
}

async fn run_stream(cfg: &AppConfig, tx: &mpsc::Sender<ScannerEvent>) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(cfg.scanner_grpc_url.clone())?
        .x_token(cfg.scanner_grpc_token.clone())?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(64 * 1024 * 1024)
        .connect()
        .await
        .context("扫链：gRPC 连接失败")?;

    let (_, mut stream) = client
        .subscribe_with_request(Some(build_subscribe_request()))
        .await
        .context("扫链：发送订阅请求失败")?;

    info!("扫链：订阅成功，开始监听 Pump.fun 新币与早期买入");

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
                            for event in decoder::decode_transaction(tx_update.slot, tx_info) {
                                if tx.send(event).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        debug!("扫链：收到 gRPC Ping");
                    }
                    Some(UpdateOneof::Pong(_)) => {
                        debug!("扫链：收到 gRPC Pong");
                    }
                    Some(other) => {
                        debug!(
                            "扫链：忽略非交易更新 {:?}",
                            std::mem::discriminant(&other)
                        );
                    }
                    None => {}
                }
            }
            Ok(Some(Err(err))) => return Err(err).context("扫链：消息流读取失败"),
            Ok(None) => anyhow::bail!("扫链：gRPC 流意外结束"),
            Err(_) if last_message_at.elapsed() >= idle_timeout => {
                anyhow::bail!(
                    "扫链：超过 {} 秒没有收到任何消息，主动重连",
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
