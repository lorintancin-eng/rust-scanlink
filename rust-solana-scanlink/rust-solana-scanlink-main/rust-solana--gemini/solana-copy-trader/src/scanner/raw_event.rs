use crate::filter::RawEventRecord;
use crate::scanner::{NewToken, PumpBuyEvent, ScannerEvent};
use anyhow::{Context, Result};
use serde_json::Value;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::time::Instant;

pub fn raw_event_to_scanner_event(record: &RawEventRecord) -> Result<Option<ScannerEvent>> {
    let payload: Value = serde_json::from_str(&record.payload_json)
        .with_context(|| format!("parse raw scanner payload failed: {}", record.event_type))?;
    match record.event_type.as_str() {
        "new_token" => Ok(Some(ScannerEvent::NewToken(NewToken {
            mint: string_field(&payload, "mint").unwrap_or_else(|| record.mint.clone()),
            bonding_curve: string_field(&payload, "bonding_curve").unwrap_or_default(),
            creator: string_field(&payload, "creator")
                .or_else(|| record.actor.clone())
                .unwrap_or_default(),
            feed_source: string_field(&payload, "feed_source")
                .unwrap_or_else(|| record.feed_source.clone()),
            name: string_field(&payload, "name").unwrap_or_default(),
            symbol: string_field(&payload, "symbol").unwrap_or_default(),
            uri: string_field(&payload, "uri").unwrap_or_default(),
            is_v2: payload
                .get("is_v2")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            detected_at_ms: payload
                .get("detected_at_ms")
                .and_then(Value::as_u64)
                .unwrap_or(record.recorded_at_ms),
            signature: string_field(&payload, "signature")
                .unwrap_or_else(|| record.signature.clone()),
            slot: payload
                .get("slot")
                .and_then(Value::as_u64)
                .unwrap_or(record.slot),
            instruction_data: decode_instruction_data(&payload)?,
            instruction_accounts: decode_instruction_accounts(&payload)?,
        }))),
        "buy" => {
            let buyer = string_field(&payload, "buyer")
                .or_else(|| record.actor.clone())
                .context("raw buy payload missing buyer")?;
            let buyer = Pubkey::from_str(&buyer).context("invalid replay buy buyer pubkey")?;
            let token_program = string_field(&payload, "token_program")
                .and_then(|value| Pubkey::from_str(&value).ok())
                .unwrap_or_default();
            Ok(Some(ScannerEvent::Buy(PumpBuyEvent {
                mint: string_field(&payload, "mint").unwrap_or_else(|| record.mint.clone()),
                buyer,
                feed_source: string_field(&payload, "feed_source")
                    .unwrap_or_else(|| record.feed_source.clone()),
                token_program,
                sol_amount_lamports: payload
                    .get("sol_amount_lamports")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                instruction_data: decode_instruction_data(&payload)?,
                instruction_accounts: decode_instruction_accounts(&payload)?,
                signature: string_field(&payload, "signature")
                    .unwrap_or_else(|| record.signature.clone()),
                slot: payload
                    .get("slot")
                    .and_then(Value::as_u64)
                    .unwrap_or(record.slot),
                detected_at_ms: payload
                    .get("detected_at_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(record.recorded_at_ms),
                detected_at: Instant::now(),
            })))
        }
        _ => Ok(None),
    }
}

fn string_field(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.trim().is_empty())
}

fn decode_instruction_data(payload: &Value) -> Result<Vec<u8>> {
    let Some(value) = string_field(payload, "instruction_data_b58")
        .or_else(|| payload.get("raw_meta").and_then(|raw| string_field(raw, "instruction_data_b58")))
    else {
        return Ok(Vec::new());
    };
    bs58::decode(value)
        .into_vec()
        .context("invalid raw instruction_data_b58")
}

fn decode_instruction_accounts(payload: &Value) -> Result<Vec<Pubkey>> {
    let values = payload
        .get("instruction_accounts")
        .or_else(|| payload.get("raw_meta").and_then(|raw| raw.get("instruction_accounts")))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    values
        .iter()
        .filter_map(Value::as_str)
        .map(|value| Pubkey::from_str(value).context("invalid raw instruction account"))
        .collect()
}
