use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConsensusKey {
    group_id: String,
    token_mint: Pubkey,
}

#[derive(Debug, Clone)]
pub struct BuySignal {
    pub group_id: String,
    pub group_name: String,
    pub token_mint: Pubkey,
    pub wallet: Pubkey,
    pub detected_at: Instant,
    pub signature: String,
    pub consensus_min_wallets: usize,
    pub consensus_timeout_secs: u64,
}

#[derive(Debug, Clone)]
pub struct ConsensusTrigger {
    pub group_id: String,
    pub group_name: String,
    pub token_mint: Pubkey,
    pub wallets: Vec<Pubkey>,
    pub first_signature: String,
    pub triggered_at: Instant,
}

#[derive(Debug, Clone)]
struct TokenSignals {
    signals: Vec<BuySignal>,
    triggered: bool,
    min_wallets: usize,
    timeout: Duration,
}

pub struct ConsensusEngine {
    signals: Arc<DashMap<ConsensusKey, TokenSignals>>,
}

impl ConsensusEngine {
    pub fn new() -> Self {
        info!("Consensus engine initialized in group mode");
        Self {
            signals: Arc::new(DashMap::new()),
        }
    }

    pub fn submit_signal(
        &self,
        signal: BuySignal,
        trigger_tx: &mpsc::UnboundedSender<ConsensusTrigger>,
    ) {
        let key = ConsensusKey {
            group_id: signal.group_id.clone(),
            token_mint: signal.token_mint,
        };
        let now = Instant::now();

        let mut entry = self.signals.entry(key.clone()).or_insert_with(|| TokenSignals {
            signals: Vec::new(),
            triggered: false,
            min_wallets: signal.consensus_min_wallets,
            timeout: Duration::from_secs(signal.consensus_timeout_secs.max(1)),
        });

        if entry.triggered {
            debug!(
                "Skip consensus signal: [{}] {} already triggered",
                signal.group_name,
                &signal.token_mint.to_string()[..12],
            );
            return;
        }

        let timeout = entry.timeout;
        entry
            .signals
            .retain(|candidate| now.duration_since(candidate.detected_at) < timeout);

        if entry.signals.iter().any(|candidate| candidate.wallet == signal.wallet) {
            debug!(
                "Skip duplicate consensus wallet: [{}] {}.. -> {}",
                signal.group_name,
                &signal.wallet.to_string()[..8],
                &signal.token_mint.to_string()[..12],
            );
            return;
        }

        entry.signals.push(signal.clone());
        let unique_wallets: Vec<Pubkey> = entry.signals.iter().map(|candidate| candidate.wallet).collect();

        info!(
            "Consensus signal: [{}] {} | {}/{} wallets",
            signal.group_name,
            &signal.token_mint.to_string()[..12],
            unique_wallets.len(),
            entry.min_wallets,
        );

        if unique_wallets.len() >= entry.min_wallets {
            entry.triggered = true;
            let trigger = ConsensusTrigger {
                group_id: signal.group_id,
                group_name: signal.group_name,
                token_mint: signal.token_mint,
                wallets: unique_wallets,
                first_signature: entry
                    .signals
                    .first()
                    .map(|candidate| candidate.signature.clone())
                    .unwrap_or_default(),
                triggered_at: Instant::now(),
            };

            info!(
                "Consensus reached: [{}] {}",
                trigger.group_name,
                &trigger.token_mint.to_string()[..12],
            );

            if trigger_tx.send(trigger).is_err() {
                warn!("Consensus trigger channel closed");
            }
        }
    }

    pub fn revoke_signal(&self, group_id: &str, mint: &Pubkey, wallet: &Pubkey) -> bool {
        let key = ConsensusKey {
            group_id: group_id.to_string(),
            token_mint: *mint,
        };

        if let Some(mut entry) = self.signals.get_mut(&key) {
            if entry.triggered {
                return false;
            }

            let before = entry.signals.len();
            entry.signals.retain(|candidate| candidate.wallet != *wallet);
            return before != entry.signals.len();
        }

        false
    }

    pub fn start_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let signals = self.signals.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let now = Instant::now();
                signals.retain(|_, entry| {
                    entry
                        .signals
                        .retain(|signal| now.duration_since(signal.detected_at) < entry.timeout);
                    if entry.signals.is_empty() {
                        return false;
                    }
                    if entry.triggered {
                        let expired = entry
                            .signals
                            .iter()
                            .all(|signal| now.duration_since(signal.detected_at) > entry.timeout * 2);
                        return !expired;
                    }
                    true
                });
            }
        })
    }

    pub fn pending_count(&self) -> usize {
        self.signals.iter().filter(|entry| !entry.triggered).count()
    }
}
