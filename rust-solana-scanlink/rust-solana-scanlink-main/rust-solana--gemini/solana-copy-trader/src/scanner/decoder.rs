use crate::scanner::{DISC_BUY, DISC_CREATE, DISC_CREATE_V2, PUMP_PROGRAM_ID};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, info, warn};
use yellowstone_grpc_proto::prelude::{
    Message, SubscribeUpdateDeshredTransactionInfo, SubscribeUpdateTransactionInfo,
    TransactionStatusMeta,
};

const MAX_FALLBACK_BUY_LAMPORTS: u64 = 50_000_000_000;
const MAX_NEW_TOKEN_NAME_LEN: usize = 96;
const MAX_NEW_TOKEN_SYMBOL_LEN: usize = 32;
const MAX_NEW_TOKEN_URI_LEN: usize = 512;
const MIN_NEW_TOKEN_ACCOUNT_COUNT: usize = 3;

static UNKNOWN_PUMP_DISC_COUNTS: OnceLock<Mutex<HashMap<[u8; 8], usize>>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScannerEventRawMeta {
    pub source_kind: String,
    pub instruction_data_b58: Option<String>,
    pub instruction_accounts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerEventMeta {
    pub event_type: String,
    pub mint: String,
    pub signature: String,
    pub slot: u64,
    pub feed_source: String,
    pub detected_at_ms: u64,
    pub raw_meta: ScannerEventRawMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewToken {
    pub mint: String,
    pub bonding_curve: String,
    pub creator: String,
    pub feed_source: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub is_v2: bool,
    pub detected_at_ms: u64,
    pub signature: String,
    pub slot: u64,
    pub instruction_data: Vec<u8>,
    pub instruction_accounts: Vec<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct PumpBuyEvent {
    pub mint: String,
    pub buyer: Pubkey,
    pub feed_source: String,
    pub token_program: Pubkey,
    pub sol_amount_lamports: u64,
    pub instruction_data: Vec<u8>,
    pub instruction_accounts: Vec<Pubkey>,
    pub signature: String,
    pub slot: u64,
    pub detected_at_ms: u64,
    pub detected_at: Instant,
}

#[derive(Debug, Clone)]
pub enum ScannerEvent {
    NewToken(NewToken),
    Buy(PumpBuyEvent),
}

impl NewToken {
    pub fn meta(&self) -> ScannerEventMeta {
        ScannerEventMeta {
            event_type: "new_token".to_string(),
            mint: self.mint.clone(),
            signature: self.signature.clone(),
            slot: self.slot,
            feed_source: self.feed_source.clone(),
            detected_at_ms: self.detected_at_ms,
            raw_meta: ScannerEventRawMeta {
                source_kind: feed_source_kind(&self.feed_source).to_string(),
                instruction_data_b58: (!self.instruction_data.is_empty())
                    .then(|| bs58::encode(&self.instruction_data).into_string()),
                instruction_accounts: self
                    .instruction_accounts
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            },
        }
    }
}

impl PumpBuyEvent {
    pub fn meta(&self) -> ScannerEventMeta {
        ScannerEventMeta {
            event_type: "buy".to_string(),
            mint: self.mint.clone(),
            signature: self.signature.clone(),
            slot: self.slot,
            feed_source: self.feed_source.clone(),
            detected_at_ms: self.detected_at_ms,
            raw_meta: ScannerEventRawMeta {
                source_kind: feed_source_kind(&self.feed_source).to_string(),
                instruction_data_b58: (!self.instruction_data.is_empty())
                    .then(|| bs58::encode(&self.instruction_data).into_string()),
                instruction_accounts: self
                    .instruction_accounts
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            },
        }
    }
}

impl ScannerEvent {
    pub fn meta(&self) -> ScannerEventMeta {
        match self {
            ScannerEvent::NewToken(token) => token.meta(),
            ScannerEvent::Buy(buy) => buy.meta(),
        }
    }
}

pub fn decode_transaction(
    feed_source: &str,
    slot: u64,
    tx_info: &SubscribeUpdateTransactionInfo,
) -> Vec<ScannerEvent> {
    let Some(tx) = tx_info.transaction.as_ref() else {
        return Vec::new();
    };
    let Some(message) = tx.message.as_ref() else {
        return Vec::new();
    };

    decode_message(
        feed_source,
        slot,
        &transaction_signature(&tx_info.signature),
        message,
        tx_info.meta.as_ref(),
        tx_info
            .meta
            .as_ref()
            .map(|meta| meta.loaded_writable_addresses.as_slice())
            .unwrap_or(&[]),
        tx_info
            .meta
            .as_ref()
            .map(|meta| meta.loaded_readonly_addresses.as_slice())
            .unwrap_or(&[]),
    )
}

pub fn decode_deshred_transaction(
    feed_source: &str,
    slot: u64,
    tx_info: &SubscribeUpdateDeshredTransactionInfo,
) -> Vec<ScannerEvent> {
    let Some(tx) = tx_info.transaction.as_ref() else {
        return Vec::new();
    };
    let Some(message) = tx.message.as_ref() else {
        return Vec::new();
    };

    decode_message(
        feed_source,
        slot,
        &transaction_signature(&tx_info.signature),
        message,
        None,
        tx_info.loaded_writable_addresses.as_slice(),
        tx_info.loaded_readonly_addresses.as_slice(),
    )
}

fn decode_message(
    feed_source: &str,
    slot: u64,
    signature: &str,
    message: &Message,
    meta: Option<&TransactionStatusMeta>,
    loaded_writable_addresses: &[Vec<u8>],
    loaded_readonly_addresses: &[Vec<u8>],
) -> Vec<ScannerEvent> {
    let detected_at_ms = now_ms();
    let detected_at = Instant::now();

    let account_keys = build_account_keys(
        message.account_keys.as_slice(),
        loaded_writable_addresses,
        loaded_readonly_addresses,
    );
    if account_keys.is_empty() {
        return Vec::new();
    }

    let mut events = Vec::new();

    for ix in &message.instructions {
        decode_instruction(
            slot,
            signature,
            feed_source,
            detected_at_ms,
            detected_at,
            meta,
            &ix.data,
            &ix.accounts,
            ix.program_id_index as usize,
            &account_keys,
            &mut events,
        );
    }

    if let Some(meta) = meta {
        for inner in &meta.inner_instructions {
            for ix in &inner.instructions {
                decode_instruction(
                    slot,
                    signature,
                    feed_source,
                    detected_at_ms,
                    detected_at,
                    Some(meta),
                    &ix.data,
                    &ix.accounts,
                    ix.program_id_index as usize,
                    &account_keys,
                    &mut events,
                );
            }
        }
    }

    events
}

fn transaction_signature(signature: &[u8]) -> String {
    if signature.is_empty() {
        "unknown".to_string()
    } else {
        bs58::encode(signature).into_string()
    }
}

fn decode_instruction(
    slot: u64,
    signature: &str,
    feed_source: &str,
    detected_at_ms: u64,
    detected_at: Instant,
    meta: Option<&TransactionStatusMeta>,
    data: &[u8],
    account_indices: &[u8],
    program_idx: usize,
    account_keys: &[Pubkey],
    events: &mut Vec<ScannerEvent>,
) {
    let Some(program_id) = account_keys.get(program_idx) else {
        return;
    };
    if program_id.to_string() != PUMP_PROGRAM_ID || data.len() < 8 {
        return;
    }

    let Ok(disc) = <[u8; 8]>::try_from(&data[..8]) else {
        return;
    };

    if disc == DISC_CREATE || disc == DISC_CREATE_V2 {
        if let Some(token) = decode_new_token(
            slot,
            signature,
            feed_source,
            detected_at_ms,
            disc == DISC_CREATE_V2,
            data,
            account_indices,
            account_keys,
        ) {
            info!(
                "扫链：发现新币 {} ({}) | mint={} | creator={} | v2={} | feed={}",
                token.name, token.symbol, token.mint, token.creator, token.is_v2, token.feed_source
            );
            events.push(ScannerEvent::NewToken(token));
        }
        return;
    }

    if disc == DISC_BUY {
        if let Some(buy) = decode_buy(
            slot,
            signature,
            feed_source,
            detected_at_ms,
            detected_at,
            meta,
            data,
            account_indices,
            account_keys,
        ) {
            debug!(
                "扫链：捕获买入 | mint={} | buyer={} | sol={:.4} | sig={} | feed={}",
                buy.mint,
                buy.buyer,
                buy.sol_amount_lamports as f64 / 1e9,
                buy.signature,
                buy.feed_source
            );
            events.push(ScannerEvent::Buy(buy));
        }
        return;
    }

    if let Some(token) = decode_create_like_unknown_instruction(
        slot,
        signature,
        feed_source,
        detected_at_ms,
        data,
        account_indices,
        account_keys,
    ) {
        info!(
            "scanner: recovered create-like Pump instruction | sig={} | disc={:?} | mint={} | creator={} | feed={}",
            signature, disc, token.mint, token.creator, token.feed_source
        );
        events.push(ScannerEvent::NewToken(token));
        return;
    }

    log_unknown_pump_instruction(
        signature,
        feed_source,
        disc,
        data.len(),
        account_indices.len(),
    );

    debug!(
        "扫链：发现未识别 Pump 指令 | sig={} | disc={:?} | feed={}",
        signature, disc, feed_source
    );
}

fn decode_create_like_unknown_instruction(
    slot: u64,
    signature: &str,
    feed_source: &str,
    detected_at_ms: u64,
    data: &[u8],
    account_indices: &[u8],
    account_keys: &[Pubkey],
) -> Option<NewToken> {
    if account_indices.len() < MIN_NEW_TOKEN_ACCOUNT_COUNT || data.len() <= 8 {
        return None;
    }
    let payload = &data[8..];
    let mut offset = 0usize;
    let name = read_borsh_string(payload, &mut offset)?;
    let symbol = read_borsh_string(payload, &mut offset)?;
    let uri = read_borsh_string(payload, &mut offset)?;
    if !looks_like_new_token_payload(&name, &symbol, &uri) {
        return None;
    }

    let mint = indexed_account(account_indices, account_keys, 0)?;
    let bonding_curve = indexed_account(account_indices, account_keys, 2)?;
    let creator = read_pubkey_string(payload, &mut offset).or_else(|| {
        instruction_accounts(account_indices, account_keys)
            .last()
            .map(ToString::to_string)
    })?;

    Some(NewToken {
        mint: mint.to_string(),
        bonding_curve: bonding_curve.to_string(),
        creator,
        feed_source: feed_source.to_string(),
        name,
        symbol,
        uri,
        is_v2: true,
        detected_at_ms,
        signature: signature.to_string(),
        slot,
        instruction_data: data.to_vec(),
        instruction_accounts: instruction_accounts(account_indices, account_keys),
    })
}

fn decode_new_token(
    slot: u64,
    signature: &str,
    feed_source: &str,
    detected_at_ms: u64,
    is_v2: bool,
    data: &[u8],
    account_indices: &[u8],
    account_keys: &[Pubkey],
) -> Option<NewToken> {
    let payload = &data[8..];
    let mut offset = 0usize;

    let name = read_borsh_string(payload, &mut offset)?;
    let symbol = read_borsh_string(payload, &mut offset)?;
    let uri = read_borsh_string(payload, &mut offset)?;

    let mint = indexed_account(account_indices, account_keys, 0)?;
    let bonding_curve = indexed_account(account_indices, account_keys, 2)?;
    let creator = read_pubkey_string(payload, &mut offset)?;

    Some(NewToken {
        mint: mint.to_string(),
        bonding_curve: bonding_curve.to_string(),
        creator: creator.to_string(),
        feed_source: feed_source.to_string(),
        name,
        symbol,
        uri,
        is_v2,
        detected_at_ms,
        signature: signature.to_string(),
        slot,
        instruction_data: data.to_vec(),
        instruction_accounts: account_indices
            .iter()
            .filter_map(|idx| account_keys.get(*idx as usize).copied())
            .collect(),
    })
}

fn decode_buy(
    slot: u64,
    signature: &str,
    feed_source: &str,
    detected_at_ms: u64,
    detected_at: Instant,
    meta: Option<&TransactionStatusMeta>,
    data: &[u8],
    account_indices: &[u8],
    account_keys: &[Pubkey],
) -> Option<PumpBuyEvent> {
    if data.len() < 24 {
        return None;
    }

    let mint = indexed_account(account_indices, account_keys, 2)?;
    let buyer_account_idx = *account_indices.get(6)? as usize;
    let buyer = *account_keys.get(buyer_account_idx)?;
    let token_program = indexed_account(account_indices, account_keys, 8)?;
    let instruction_accounts: Vec<Pubkey> = account_indices
        .iter()
        .filter_map(|idx| account_keys.get(*idx as usize).copied())
        .collect();
    if instruction_accounts.len() < 9 {
        warn!("扫链：买入指令账户长度异常，跳过 sig={}", signature);
        return None;
    }

    let fallback_max_sol_cost_lamports = u64::from_le_bytes(data[16..24].try_into().ok()?);
    let sol_amount_lamports =
        estimate_buy_sol_amount_lamports(meta, buyer_account_idx, fallback_max_sol_cost_lamports);

    Some(PumpBuyEvent {
        mint: mint.to_string(),
        buyer,
        feed_source: feed_source.to_string(),
        token_program,
        sol_amount_lamports,
        instruction_data: data.to_vec(),
        instruction_accounts,
        signature: signature.to_string(),
        slot,
        detected_at_ms,
        detected_at,
    })
}

fn feed_source_kind(feed_source: &str) -> &'static str {
    if feed_source.to_ascii_lowercase().contains("deshred") {
        "deshred"
    } else {
        "processed"
    }
}

fn indexed_account(
    account_indices: &[u8],
    account_keys: &[Pubkey],
    index: usize,
) -> Option<Pubkey> {
    let account_idx = *account_indices.get(index)? as usize;
    account_keys.get(account_idx).copied()
}

fn instruction_accounts(account_indices: &[u8], account_keys: &[Pubkey]) -> Vec<Pubkey> {
    account_indices
        .iter()
        .filter_map(|idx| account_keys.get(*idx as usize).copied())
        .collect()
}

fn build_account_keys(
    static_keys: &[Vec<u8>],
    loaded_writable_addresses: &[Vec<u8>],
    loaded_readonly_addresses: &[Vec<u8>],
) -> Vec<Pubkey> {
    let mut account_keys: Vec<Pubkey> = static_keys
        .iter()
        .filter_map(|key| {
            if key.len() == 32 {
                <[u8; 32]>::try_from(key.as_slice())
                    .ok()
                    .map(Pubkey::new_from_array)
            } else {
                None
            }
        })
        .collect();

    for address in loaded_writable_addresses
        .iter()
        .chain(loaded_readonly_addresses.iter())
    {
        if address.len() == 32 {
            if let Ok(bytes) = <[u8; 32]>::try_from(address.as_slice()) {
                account_keys.push(Pubkey::new_from_array(bytes));
            }
        }
    }

    account_keys
}

fn read_borsh_string(data: &[u8], offset: &mut usize) -> Option<String> {
    if *offset + 4 > data.len() {
        return None;
    }
    let len = u32::from_le_bytes(data[*offset..*offset + 4].try_into().ok()?) as usize;
    *offset += 4;
    if *offset + len > data.len() {
        return None;
    }
    let bytes = data[*offset..*offset + len].to_vec();
    *offset += len;
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

fn read_pubkey_string(data: &[u8], offset: &mut usize) -> Option<String> {
    if *offset + 32 > data.len() {
        return None;
    }
    let bytes: [u8; 32] = data[*offset..*offset + 32].try_into().ok()?;
    *offset += 32;
    Some(Pubkey::new_from_array(bytes).to_string())
}

fn looks_like_new_token_payload(name: &str, symbol: &str, uri: &str) -> bool {
    let name = name.trim();
    let symbol = symbol.trim();
    let uri = uri.trim();
    if name.is_empty()
        || symbol.is_empty()
        || uri.is_empty()
        || name.len() > MAX_NEW_TOKEN_NAME_LEN
        || symbol.len() > MAX_NEW_TOKEN_SYMBOL_LEN
        || uri.len() > MAX_NEW_TOKEN_URI_LEN
    {
        return false;
    }

    uri.starts_with("http://")
        || uri.starts_with("https://")
        || uri.starts_with("ipfs://")
        || uri.starts_with("ar://")
}

fn log_unknown_pump_instruction(
    signature: &str,
    feed_source: &str,
    disc: [u8; 8],
    data_len: usize,
    account_count: usize,
) {
    let counts = UNKNOWN_PUMP_DISC_COUNTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = match counts.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };
    let counter = guard.entry(disc).or_insert(0);
    *counter += 1;
    if *counter <= 3 || *counter % 100 == 0 {
        warn!(
            "scanner: unknown Pump instruction | sig={} | feed={} | disc={:?} | data_len={} | accounts={} | seen={}",
            signature, feed_source, disc, data_len, account_count, *counter
        );
    }
}

fn estimate_buy_sol_amount_lamports(
    meta: Option<&TransactionStatusMeta>,
    buyer_account_idx: usize,
    fallback_max_sol_cost_lamports: u64,
) -> u64 {
    if let Some(meta) = meta {
        if let (Some(pre_balance), Some(post_balance)) = (
            meta.pre_balances.get(buyer_account_idx),
            meta.post_balances.get(buyer_account_idx),
        ) {
            if pre_balance > post_balance {
                let total_spent = pre_balance.saturating_sub(*post_balance);
                let fee_adjusted = if buyer_account_idx == 0 {
                    total_spent.saturating_sub(meta.fee)
                } else {
                    total_spent
                };
                if fee_adjusted > 0 {
                    return fee_adjusted;
                }
                return 0;
            }
        }
    }

    sanitize_fallback_buy_lamports(fallback_max_sol_cost_lamports)
}

fn sanitize_fallback_buy_lamports(lamports: u64) -> u64 {
    if lamports > MAX_FALLBACK_BUY_LAMPORTS {
        0
    } else {
        lamports
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

    #[test]
    fn test_read_borsh_string_normal() {
        let data = [0x04, 0x00, 0x00, 0x00, 0x50, 0x45, 0x50, 0x45];
        let mut off = 0;
        assert_eq!(read_borsh_string(&data, &mut off), Some("PEPE".to_string()));
        assert_eq!(off, 8);
    }

    #[test]
    fn test_read_borsh_string_truncated() {
        let data = [0x10, 0x00, 0x00, 0x00, 0x41];
        let mut off = 0;
        assert_eq!(read_borsh_string(&data, &mut off), None);
    }

    #[test]
    fn test_discriminator_const() {
        assert_eq!(DISC_CREATE[0], 0x18);
        assert_eq!(DISC_CREATE_V2[0], 214);
        assert_ne!(DISC_CREATE, DISC_CREATE_V2);
    }

    #[test]
    fn test_read_pubkey_string() {
        let expected = Pubkey::new_unique();
        let mut off = 0;
        assert_eq!(
            read_pubkey_string(expected.as_ref(), &mut off),
            Some(expected.to_string())
        );
        assert_eq!(off, 32);
    }

    #[test]
    fn create_like_unknown_instruction_detects_metadata_payload() {
        let mint = Pubkey::new_unique();
        let filler = Pubkey::new_unique();
        let bonding_curve = Pubkey::new_unique();
        let creator = Pubkey::new_unique();
        let account_keys = vec![mint, filler, bonding_curve];
        let account_indices = vec![0u8, 1u8, 2u8];

        let mut data = vec![9, 8, 7, 6, 5, 4, 3, 2];
        data.extend_from_slice(&(4u32.to_le_bytes()));
        data.extend_from_slice(b"PEPE");
        data.extend_from_slice(&(4u32.to_le_bytes()));
        data.extend_from_slice(b"PEPE");
        let uri = b"https://example.com/meta.json";
        data.extend_from_slice(&((uri.len() as u32).to_le_bytes()));
        data.extend_from_slice(uri);
        data.extend_from_slice(creator.as_ref());

        let token = decode_create_like_unknown_instruction(
            1,
            "sig",
            "secondary_processed",
            123,
            &data,
            &account_indices,
            &account_keys,
        )
        .expect("fallback should decode");

        assert_eq!(token.mint, mint.to_string());
        assert_eq!(token.bonding_curve, bonding_curve.to_string());
        assert_eq!(token.creator, creator.to_string());
        assert_eq!(token.name, "PEPE");
    }

    #[test]
    fn create_like_unknown_instruction_rejects_non_metadata_payload() {
        let account_keys = vec![
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        let account_indices = vec![0u8, 1u8, 2u8];
        let data = vec![9u8; 32];
        assert!(decode_create_like_unknown_instruction(
            1,
            "sig",
            "secondary_processed",
            123,
            &data,
            &account_indices,
            &account_keys,
        )
        .is_none());
    }

    #[test]
    fn build_account_keys_appends_loaded_alt_addresses() {
        let static_key = Pubkey::new_unique();
        let writable = Pubkey::new_unique();
        let readonly = Pubkey::new_unique();
        let keys = build_account_keys(
            &[static_key.to_bytes().to_vec()],
            &[writable.to_bytes().to_vec()],
            &[readonly.to_bytes().to_vec()],
        );
        assert_eq!(keys, vec![static_key, writable, readonly]);
    }

    #[test]
    fn buy_sol_amount_prefers_balance_delta_over_max_cost() {
        let meta = TransactionStatusMeta {
            pre_balances: vec![2_000_000_000],
            post_balances: vec![1_500_000_000],
            fee: 5_000,
            ..Default::default()
        };
        assert_eq!(
            estimate_buy_sol_amount_lamports(Some(&meta), 0, u64::MAX),
            499_995_000
        );
    }

    #[test]
    fn buy_sol_amount_zeroes_absurd_fallback_when_meta_missing() {
        assert_eq!(estimate_buy_sol_amount_lamports(None, 0, u64::MAX), 0);
        assert_eq!(
            estimate_buy_sol_amount_lamports(None, 0, 900_000_000),
            900_000_000
        );
    }
}
