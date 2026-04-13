use crate::scanner::{DISC_BUY, DISC_CREATE, DISC_CREATE_V2, PUMP_PROGRAM_ID};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
use yellowstone_grpc_proto::prelude::{
    SubscribeUpdateTransactionInfo, TransactionStatusMeta,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewToken {
    pub mint: String,
    pub bonding_curve: String,
    pub creator: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub is_v2: bool,
    pub discovered_at_ms: u64,
    pub signature: String,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct PumpBuyEvent {
    pub mint: String,
    pub buyer: Pubkey,
    pub token_program: Pubkey,
    pub sol_amount_lamports: u64,
    pub instruction_data: Vec<u8>,
    pub instruction_accounts: Vec<Pubkey>,
    pub signature: String,
    pub slot: u64,
    pub detected_at: Instant,
}

#[derive(Debug, Clone)]
pub enum ScannerEvent {
    NewToken(NewToken),
    Buy(PumpBuyEvent),
}

pub fn decode_transaction(slot: u64, tx_info: &SubscribeUpdateTransactionInfo) -> Vec<ScannerEvent> {
    let Some(tx) = tx_info.transaction.as_ref() else {
        return Vec::new();
    };
    let Some(message) = tx.message.as_ref() else {
        return Vec::new();
    };

    let signature = if tx_info.signature.is_empty() {
        "unknown".to_string()
    } else {
        bs58::encode(&tx_info.signature).into_string()
    };
    let discovered_at_ms = now_ms();
    let detected_at = Instant::now();

    let account_keys = build_account_keys(message.account_keys.as_slice(), tx_info.meta.as_ref());
    if account_keys.is_empty() {
        return Vec::new();
    }

    let mut events = Vec::new();

    for ix in &message.instructions {
        decode_instruction(
            slot,
            &signature,
            discovered_at_ms,
            detected_at,
            &ix.data,
            &ix.accounts,
            ix.program_id_index as usize,
            &account_keys,
            &mut events,
        );
    }

    if let Some(meta) = tx_info.meta.as_ref() {
        for inner in &meta.inner_instructions {
            for ix in &inner.instructions {
                decode_instruction(
                    slot,
                    &signature,
                    discovered_at_ms,
                    detected_at,
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

fn decode_instruction(
    slot: u64,
    signature: &str,
    discovered_at_ms: u64,
    detected_at: Instant,
    data: &[u8],
    account_indices: &[u8],
    program_idx: usize,
    account_keys: &[Pubkey],
    events: &mut Vec<ScannerEvent>,
) {
    let Some(program_id) = account_keys.get(program_idx) else {
        return;
    };
    if program_id.to_string() != PUMP_PROGRAM_ID {
        return;
    }
    if data.len() < 8 {
        return;
    }

    let Ok(disc) = <[u8; 8]>::try_from(&data[..8]) else {
        return;
    };

    if disc == DISC_CREATE || disc == DISC_CREATE_V2 {
        if let Some(token) = decode_new_token(
            slot,
            signature,
            discovered_at_ms,
            disc == DISC_CREATE_V2,
            data,
            account_indices,
            account_keys,
        ) {
            info!(
                "扫链：发现新币 {} ({}) | mint={} | creator={} | v2={}",
                token.name,
                token.symbol,
                token.mint,
                token.creator,
                token.is_v2
            );
            events.push(ScannerEvent::NewToken(token));
        }
        return;
    }

    if disc == DISC_BUY {
        if let Some(buy) = decode_buy(slot, signature, detected_at, data, account_indices, account_keys)
        {
            debug!(
                "扫链：捕获买入 | mint={} | buyer={} | sol={:.4} | sig={}",
                buy.mint,
                buy.buyer,
                buy.sol_amount_lamports as f64 / 1e9,
                buy.signature
            );
            events.push(ScannerEvent::Buy(buy));
        }
        return;
    }

    debug!(
        "扫链：发现未识别 Pump 指令 | sig={} | disc={:?}",
        signature, disc
    );
}

fn decode_new_token(
    slot: u64,
    signature: &str,
    discovered_at_ms: u64,
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
        name,
        symbol,
        uri,
        is_v2,
        discovered_at_ms,
        signature: signature.to_string(),
        slot,
    })
}

fn decode_buy(
    slot: u64,
    signature: &str,
    detected_at: Instant,
    data: &[u8],
    account_indices: &[u8],
    account_keys: &[Pubkey],
) -> Option<PumpBuyEvent> {
    if data.len() < 24 {
        return None;
    }

    let mint = indexed_account(account_indices, account_keys, 2)?;
    let buyer = indexed_account(account_indices, account_keys, 6)?;
    let token_program = indexed_account(account_indices, account_keys, 8)?;
    let instruction_accounts: Vec<Pubkey> = account_indices
        .iter()
        .filter_map(|idx| account_keys.get(*idx as usize).copied())
        .collect();
    if instruction_accounts.len() < 9 {
        warn!("扫链：买入指令账户长度异常，跳过 sig={}", signature);
        return None;
    }

    let sol_amount_lamports = u64::from_le_bytes(data[16..24].try_into().ok()?);

    Some(PumpBuyEvent {
        mint: mint.to_string(),
        buyer,
        token_program,
        sol_amount_lamports,
        instruction_data: data.to_vec(),
        instruction_accounts,
        signature: signature.to_string(),
        slot,
        detected_at,
    })
}

fn indexed_account(account_indices: &[u8], account_keys: &[Pubkey], index: usize) -> Option<Pubkey> {
    let account_idx = *account_indices.get(index)? as usize;
    account_keys.get(account_idx).copied()
}

fn build_account_keys(
    static_keys: &[Vec<u8>],
    meta: Option<&TransactionStatusMeta>,
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

    if let Some(meta) = meta {
        for address in meta
            .loaded_writable_addresses
            .iter()
            .chain(meta.loaded_readonly_addresses.iter())
        {
            if address.len() == 32 {
                if let Ok(bytes) = <[u8; 32]>::try_from(address.as_slice()) {
                    account_keys.push(Pubkey::new_from_array(bytes));
                }
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
}

