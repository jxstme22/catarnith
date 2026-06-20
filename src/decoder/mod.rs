use crate::types::{DecodedTx, TradeSide};
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::Value;
use solana_pubkey::Pubkey;
use std::collections::{BTreeSet, HashMap};

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const CREATE_EVENT_DISCRIMINATOR: [u8; 8] = [27, 114, 169, 77, 222, 235, 99, 118];
const TRADE_EVENT_DISCRIMINATOR: [u8; 8] = [189, 219, 127, 211, 78, 230, 97, 238];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PumpTradeObservation {
    pub mint: String,
    pub sol_lamports: u64,
    pub token_amount_raw: u64,
    pub is_buy: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SummaryTxRow {
    pub signature: String,
    #[serde(default)]
    pub ok: bool,
    #[serde(default)]
    pub err: Option<Value>,
    #[serde(default)]
    pub slot: Option<u64>,
    #[serde(default)]
    pub block_time: Option<i64>,
    #[serde(default)]
    pub fee_lamports: Option<u64>,
    #[serde(default)]
    pub wallet_lamports_delta: Option<i64>,
    #[serde(default)]
    pub program_ids: Vec<String>,
    #[serde(default)]
    pub token_mints: Vec<String>,
    #[serde(default)]
    pub mentions_mayhem_program: bool,
    #[serde(default)]
    pub mentions_mayhem_agent_wallet: bool,
    #[serde(default)]
    pub mentions_common_pump_program: bool,
    #[serde(default)]
    pub log_messages: Vec<String>,
}

pub fn decode_summary_line(line: &str) -> Result<DecodedTx> {
    let row: SummaryTxRow =
        serde_json::from_str(line).context("failed to parse summary JSONL row")?;
    Ok(decode_summary_row(row))
}

pub fn decode_summary_row(row: SummaryTxRow) -> DecodedTx {
    let instruction_names = extract_instruction_names(&row.log_messages);
    let side = classify_side(row.ok, &instruction_names);
    let timestamp_ms = row.block_time.map(|seconds| seconds * 1_000);

    DecodedTx {
        signature: row.signature,
        slot: row.slot.unwrap_or_default(),
        timestamp_ms,
        ok: row.ok,
        side,
        instruction_names,
        program_ids: row.program_ids,
        account_keys: Vec::new(),
        mint: row.token_mints.into_iter().next(),
        signer: None,
        sol_delta_lamports: row.wallet_lamports_delta,
        token_delta_raw: None,
        fee_lamports: row.fee_lamports,
        logs: row.log_messages,
        err: row.err.map(|err| err.to_string()),
    }
}

pub fn decode_live_transaction(
    signature: String,
    slot: u64,
    logs: Vec<String>,
    tx: Option<&Value>,
    wallet: Option<&str>,
) -> DecodedTx {
    let instruction_names = extract_instruction_names(&logs);
    let pump_trade = extract_pump_trade_observation(&logs);
    let ok = tx
        .and_then(|value| value.pointer("/meta/err"))
        .map(|err| err.is_null())
        .unwrap_or(true);
    let side = classify_side(ok, &instruction_names);
    let program_ids = tx
        .map(extract_program_ids)
        .unwrap_or_else(|| extract_program_ids_from_logs(&logs));
    let account_keys = tx.map(extract_account_keys).unwrap_or_default();
    let market_owner = tx.and_then(|tx| preferred_market_owner(tx, wallet));
    let token_delta = market_owner
        .as_deref()
        .and_then(|owner| tx.and_then(|tx| wallet_token_delta(tx, owner)));
    let mint = token_delta
        .as_ref()
        .map(|delta| delta.mint.clone())
        .or_else(|| {
            tx.map(extract_token_mints)
                .and_then(|mints| mints.into_iter().find(|mint| mint != WSOL_MINT))
        })
        .or_else(|| extract_pump_create_event_mint(&logs))
        .or_else(|| pump_trade.as_ref().map(|trade| trade.mint.clone()));
    let sol_delta_lamports = market_owner
        .as_deref()
        .and_then(|owner| tx.and_then(|tx| wallet_lamport_delta(tx, owner)))
        .or_else(|| {
            pump_trade.as_ref().and_then(|trade| {
                i64::try_from(trade.sol_lamports).ok().map(|amount| {
                    if trade.is_buy {
                        -amount
                    } else {
                        amount
                    }
                })
            })
        });
    let fee_lamports = tx
        .and_then(|value| value.pointer("/meta/fee"))
        .and_then(|value| value.as_u64());
    let timestamp_ms = tx
        .and_then(|value| value.get("blockTime"))
        .and_then(|value| value.as_i64())
        .map(|seconds| seconds * 1_000);

    DecodedTx {
        signature,
        slot,
        timestamp_ms,
        ok,
        side,
        instruction_names,
        program_ids,
        account_keys,
        mint,
        signer: market_owner,
        sol_delta_lamports,
        token_delta_raw: token_delta.map(|delta| delta.amount_raw).or_else(|| {
            pump_trade.as_ref().map(|trade| {
                let amount = i128::from(trade.token_amount_raw);
                if trade.is_buy {
                    amount
                } else {
                    -amount
                }
            })
        }),
        fee_lamports,
        logs,
        err: tx
            .and_then(|value| value.pointer("/meta/err"))
            .filter(|err| !err.is_null())
            .map(|err| err.to_string()),
    }
}

#[derive(Debug, Clone)]
struct TokenDelta {
    mint: String,
    amount_raw: i128,
}

fn preferred_market_owner(tx: &Value, preferred_wallet: Option<&str>) -> Option<String> {
    if let Some(wallet) = preferred_wallet {
        if extract_account_keys(tx).iter().any(|key| key == wallet) {
            return Some(wallet.to_string());
        }
    }
    extract_signer(tx)
}

fn extract_signer(tx: &Value) -> Option<String> {
    let keys = tx.pointer("/transaction/message/accountKeys")?.as_array()?;
    keys.iter()
        .find(|key| key.get("signer").and_then(Value::as_bool) == Some(true))
        .and_then(|key| key.get("pubkey").and_then(Value::as_str))
        .map(str::to_string)
        .or_else(|| {
            keys.first().and_then(|key| {
                key.as_str().map(str::to_string).or_else(|| {
                    key.get("pubkey")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
            })
        })
}

fn wallet_token_delta(tx: &Value, wallet: &str) -> Option<TokenDelta> {
    let mut pre = HashMap::<String, i128>::new();
    let mut post = HashMap::<String, i128>::new();
    collect_token_balances(tx, "/meta/preTokenBalances", wallet, &mut pre);
    collect_token_balances(tx, "/meta/postTokenBalances", wallet, &mut post);

    let mints = pre
        .keys()
        .chain(post.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut best = None::<TokenDelta>;
    for mint in mints {
        if mint == WSOL_MINT {
            continue;
        }
        let amount_raw = post.get(&mint).copied().unwrap_or_default()
            - pre.get(&mint).copied().unwrap_or_default();
        if amount_raw == 0 {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|current| amount_raw.abs() > current.amount_raw.abs())
        {
            best = Some(TokenDelta { mint, amount_raw });
        }
    }
    best
}

fn collect_token_balances(
    tx: &Value,
    pointer: &str,
    wallet: &str,
    out: &mut HashMap<String, i128>,
) {
    let Some(items) = tx.pointer(pointer).and_then(Value::as_array) else {
        return;
    };
    for item in items {
        if item.get("owner").and_then(Value::as_str) != Some(wallet) {
            continue;
        }
        let Some(mint) = item.get("mint").and_then(Value::as_str) else {
            continue;
        };
        let amount = item
            .pointer("/uiTokenAmount/amount")
            .and_then(Value::as_str)
            .and_then(|amount| amount.parse::<i128>().ok())
            .unwrap_or_default();
        *out.entry(mint.to_string()).or_default() += amount;
    }
}

pub fn extract_instruction_names(logs: &[String]) -> Vec<String> {
    logs.iter()
        .filter_map(|log| {
            log.split("Program log: Instruction: ")
                .nth(1)
                .map(|name| name.trim().to_string())
        })
        .collect()
}

pub fn extract_program_ids_from_logs(logs: &[String]) -> Vec<String> {
    logs.iter()
        .filter_map(|log| {
            let rest = log.strip_prefix("Program ")?;
            let (program, suffix) = rest.split_once(' ')?;
            suffix.starts_with("invoke").then(|| program.to_string())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub fn extract_pump_create_event_mint(logs: &[String]) -> Option<String> {
    for bytes in pump_program_data(logs) {
        if !bytes.starts_with(&CREATE_EVENT_DISCRIMINATOR) {
            continue;
        }
        let mut offset = CREATE_EVENT_DISCRIMINATOR.len();
        for _ in 0..3 {
            let length_bytes: [u8; 4] = bytes.get(offset..offset + 4)?.try_into().ok()?;
            offset = offset.saturating_add(4);
            let length = u32::from_le_bytes(length_bytes) as usize;
            offset = offset.checked_add(length)?;
            if offset > bytes.len() {
                return None;
            }
        }
        let mint_bytes: [u8; 32] = bytes.get(offset..offset + 32)?.try_into().ok()?;
        return Some(Pubkey::new_from_array(mint_bytes).to_string());
    }
    None
}

pub fn logs_have_pump_create_signal(logs: &[String]) -> bool {
    extract_pump_create_event_mint(logs).is_some()
        || extract_instruction_names(logs)
            .iter()
            .any(|name| name.starts_with("CreateV"))
}

pub fn has_pump_create_signal(decoded: &DecodedTx) -> bool {
    let create_event_matches_mint = extract_pump_create_event_mint(&decoded.logs)
        .zip(decoded.mint.as_deref())
        .is_some_and(|(event_mint, decoded_mint)| event_mint == decoded_mint);
    create_event_matches_mint
        || decoded
            .instruction_names
            .iter()
            .any(|name| name.starts_with("CreateV"))
}

pub fn extract_pump_trade_observation(logs: &[String]) -> Option<PumpTradeObservation> {
    for bytes in pump_program_data(logs) {
        if !bytes.starts_with(&TRADE_EVENT_DISCRIMINATOR) {
            continue;
        }
        let mint_bytes: [u8; 32] = bytes.get(8..40)?.try_into().ok()?;
        let sol_bytes: [u8; 8] = bytes.get(40..48)?.try_into().ok()?;
        let token_bytes: [u8; 8] = bytes.get(48..56)?.try_into().ok()?;
        let is_buy = *bytes.get(56)? != 0;
        return Some(PumpTradeObservation {
            mint: Pubkey::new_from_array(mint_bytes).to_string(),
            sol_lamports: u64::from_le_bytes(sol_bytes),
            token_amount_raw: u64::from_le_bytes(token_bytes),
            is_buy,
        });
    }
    None
}

fn pump_program_data(logs: &[String]) -> impl Iterator<Item = Vec<u8>> + '_ {
    logs.iter().filter_map(|log| {
        let encoded = log.strip_prefix("Program data: ")?;
        STANDARD.decode(encoded).ok()
    })
}

pub fn classify_side(ok: bool, instruction_names: &[String]) -> TradeSide {
    if instruction_names.iter().any(|name| name.starts_with("Buy")) {
        return TradeSide::Buy;
    }
    if instruction_names
        .iter()
        .any(|name| name.starts_with("Sell"))
    {
        return TradeSide::Sell;
    }
    if instruction_names.iter().any(|name| name == "Swap") {
        return TradeSide::Swap;
    }
    if instruction_names
        .iter()
        .any(|name| name == "Create" || name.starts_with("CreateV"))
    {
        return TradeSide::Create;
    }
    if !ok {
        return TradeSide::Failed;
    }
    TradeSide::Unknown
}

pub fn instruction_label(decoded: &DecodedTx) -> String {
    if decoded.instruction_names.is_empty() {
        "Unknown".to_string()
    } else {
        decoded.instruction_names.join(";")
    }
}

pub fn extract_account_keys(tx: &Value) -> Vec<String> {
    tx.pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array)
        .map(|keys| {
            keys.iter()
                .filter_map(|key| {
                    key.as_str().map(str::to_string).or_else(|| {
                        key.get("pubkey")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn extract_token_mints(tx: &Value) -> Vec<String> {
    let mut out = std::collections::BTreeSet::new();
    for field in ["preTokenBalances", "postTokenBalances"] {
        if let Some(items) = tx
            .pointer(&format!("/meta/{field}"))
            .and_then(Value::as_array)
        {
            for item in items {
                if let Some(mint) = item.get("mint").and_then(Value::as_str) {
                    out.insert(mint.to_string());
                }
            }
        }
    }
    out.into_iter().collect()
}

pub fn wallet_lamport_delta(tx: &Value, wallet: &str) -> Option<i64> {
    let keys = extract_account_keys(tx);
    let pre = tx.pointer("/meta/preBalances")?.as_array()?;
    let post = tx.pointer("/meta/postBalances")?.as_array()?;
    let mut delta = 0i64;
    let mut found = false;
    for (idx, key) in keys.iter().enumerate() {
        if key == wallet {
            let before = pre.get(idx)?.as_i64()?;
            let after = post.get(idx)?.as_i64()?;
            delta += after - before;
            found = true;
        }
    }
    found.then_some(delta)
}

pub fn extract_program_ids(tx: &Value) -> Vec<String> {
    let mut out = std::collections::BTreeSet::new();
    for instruction in top_level_instructions(tx) {
        if let Some(program_id) = instruction.get("programId").and_then(Value::as_str) {
            out.insert(program_id.to_string());
        }
    }
    if let Some(inner_groups) = tx
        .pointer("/meta/innerInstructions")
        .and_then(Value::as_array)
    {
        for group in inner_groups {
            if let Some(instructions) = group.get("instructions").and_then(Value::as_array) {
                for instruction in instructions {
                    if let Some(program_id) = instruction.get("programId").and_then(Value::as_str) {
                        out.insert(program_id.to_string());
                    }
                }
            }
        }
    }
    out.into_iter().collect()
}

fn top_level_instructions(tx: &Value) -> Vec<&Value> {
    tx.pointer("/transaction/message/instructions")
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_string(bytes: &mut Vec<u8>, value: &str) {
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }

    #[test]
    fn decodes_create_and_trade_events_directly_from_processed_logs() {
        let create_mint = Pubkey::new_from_array([7u8; 32]);
        let mut create = CREATE_EVENT_DISCRIMINATOR.to_vec();
        push_string(&mut create, "Fresh");
        push_string(&mut create, "NEW");
        push_string(&mut create, "https://example.test");
        create.extend_from_slice(create_mint.as_ref());

        let mut trade = TRADE_EVENT_DISCRIMINATOR.to_vec();
        trade.extend_from_slice(create_mint.as_ref());
        trade.extend_from_slice(&42_000_000u64.to_le_bytes());
        trade.extend_from_slice(&900_000_000u64.to_le_bytes());
        trade.push(1);

        let logs = vec![
            "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]".to_string(),
            "Program log: Instruction: CreateV2".to_string(),
            format!("Program data: {}", STANDARD.encode(create)),
            "Program log: Instruction: BuyV2".to_string(),
            format!("Program data: {}", STANDARD.encode(trade)),
        ];

        let decoded = decode_live_transaction("sig".to_string(), 123, logs, None, None);
        assert_eq!(
            decoded.mint.as_deref(),
            Some(create_mint.to_string().as_str())
        );
        assert_eq!(decoded.side, TradeSide::Buy);
        assert_eq!(decoded.sol_delta_lamports, Some(-42_000_000));
        assert_eq!(decoded.token_delta_raw, Some(900_000_000));
        assert!(decoded
            .program_ids
            .iter()
            .any(|program| program == "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"));
    }
}
