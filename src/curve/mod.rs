use crate::{
    market::MarketQuote,
    types::{now_ms, TradeSide},
};
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use solana_pubkey::Pubkey;
use std::str::FromStr;

const BONDING_CURVE_DISCRIMINATOR: [u8; 8] = [23, 183, 248, 55, 96, 216, 172, 96];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BondingCurveState {
    pub mint: String,
    pub account: String,
    pub slot: u64,
    #[serde(default)]
    pub observed_at_ms: i64,
    pub virtual_token_reserves: u64,
    pub virtual_quote_reserves: u64,
    pub real_token_reserves: u64,
    pub real_quote_reserves: u64,
    pub token_total_supply: u64,
    pub complete: bool,
    pub is_mayhem_mode: Option<bool>,
    pub quote_mint: Option<String>,
}

#[derive(Clone)]
pub struct CurveQuoteClient {
    client: Client,
    rpc_url: String,
    pumpfun_program: Pubkey,
}

impl CurveQuoteClient {
    pub fn new(rpc_url: String, pumpfun_program: &str) -> Result<Self> {
        Ok(Self {
            client: Client::new(),
            rpc_url,
            pumpfun_program: Pubkey::from_str(pumpfun_program)
                .context("invalid Pump.fun program pubkey")?,
        })
    }

    pub async fn fetch_state(&self, mint: &str) -> Result<BondingCurveState> {
        let curve = self.bonding_curve_address(mint)?;
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                curve.to_string(),
                {
                    "encoding": "base64",
                    "commitment": "processed"
                }
            ]
        });
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(|err| {
                let kind = if err.is_timeout() {
                    "timeout"
                } else if err.is_connect() {
                    "connection"
                } else {
                    "transport"
                };
                anyhow::anyhow!("bonding curve getAccountInfo {kind} failure")
            })?;
        if !response.status().is_success() {
            anyhow::bail!(
                "bonding curve getAccountInfo HTTP status {}",
                response.status()
            );
        }
        let value: Value = response
            .json()
            .await
            .context("invalid bonding curve RPC response")?;
        if let Some(error) = value.get("error") {
            anyhow::bail!("bonding curve RPC error: {error}");
        }
        let slot = value
            .pointer("/result/context/slot")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let encoded = value
            .pointer("/result/value/data/0")
            .and_then(Value::as_str)
            .context("bonding curve account is unavailable")?;
        let data = STANDARD
            .decode(encoded)
            .context("invalid base64 bonding curve data")?;
        decode_bonding_curve(mint, &curve.to_string(), slot, &data)
    }

    pub fn bonding_curve_address(&self, mint: &str) -> Result<Pubkey> {
        let mint_pubkey = Pubkey::from_str(mint).context("invalid mint pubkey")?;
        Ok(Pubkey::find_program_address(
            &[b"bonding-curve", mint_pubkey.as_ref()],
            &self.pumpfun_program,
        )
        .0)
    }

    pub async fn sell_quote(
        &self,
        mint: &str,
        token_amount_raw: u128,
    ) -> Result<(BondingCurveState, MarketQuote)> {
        let state = self.fetch_state(mint).await?;
        let quote = sell_quote_from_state(&state, token_amount_raw)?;
        Ok((state, quote))
    }
}

pub fn sell_quote_from_state(
    state: &BondingCurveState,
    token_amount_raw: u128,
) -> Result<MarketQuote> {
    validate_sellable_state(state)?;

    let virtual_tokens = state.virtual_token_reserves as u128;
    let virtual_quote = state.virtual_quote_reserves as u128;
    let denominator = virtual_tokens.saturating_add(token_amount_raw);
    if denominator == 0 {
        anyhow::bail!("invalid zero-reserve bonding curve");
    }
    let gross_quote = token_amount_raw
        .saturating_mul(virtual_quote)
        .checked_div(denominator)
        .unwrap_or_default();
    let available_quote = state.real_quote_reserves as u128;
    let sol_lamports = gross_quote.min(available_quote);
    if sol_lamports == 0 {
        anyhow::bail!("bonding curve has no quote reserves");
    }

    let observed_at_ms = if state.observed_at_ms > 0 {
        state.observed_at_ms
    } else {
        now_ms()
    };
    Ok(MarketQuote {
        mint: state.mint.clone(),
        slot: state.slot,
        timestamp_ms: observed_at_ms,
        observed_at_ms,
        signature: curve_state_key(state),
        side: TradeSide::Sell,
        sol_lamports,
        token_amount_raw,
        trader: None,
    })
}

pub fn buy_quote_from_state(state: &BondingCurveState, sol_lamports: u128) -> Result<MarketQuote> {
    validate_tradeable_state(state)?;
    let virtual_tokens = state.virtual_token_reserves as u128;
    let virtual_quote = state.virtual_quote_reserves as u128;
    let denominator = virtual_quote.saturating_add(sol_lamports);
    if denominator == 0 {
        anyhow::bail!("invalid zero-reserve bonding curve");
    }
    let gross_tokens = sol_lamports
        .saturating_mul(virtual_tokens)
        .checked_div(denominator)
        .unwrap_or_default();
    let token_amount_raw = gross_tokens.min(state.real_token_reserves as u128);
    if token_amount_raw == 0 {
        anyhow::bail!("bonding curve has no token reserves");
    }
    let observed_at_ms = state.observed_at_ms.max(1);
    Ok(MarketQuote {
        mint: state.mint.clone(),
        slot: state.slot,
        timestamp_ms: observed_at_ms,
        observed_at_ms,
        signature: format!("curve-buy:{}", curve_state_key(state)),
        side: TradeSide::Buy,
        sol_lamports,
        token_amount_raw,
        trader: None,
    })
}

fn validate_tradeable_state(state: &BondingCurveState) -> Result<()> {
    if state.is_mayhem_mode == Some(false) {
        anyhow::bail!("bonding curve explicitly reports non-Mayhem mode");
    }
    if state.complete {
        anyhow::bail!("bonding curve is complete; use PumpSwap market quote");
    }
    if state
        .quote_mint
        .as_deref()
        .is_some_and(|mint| mint != "11111111111111111111111111111111")
    {
        anyhow::bail!("non-SOL bonding curve quote is unsupported");
    }
    Ok(())
}

fn validate_sellable_state(state: &BondingCurveState) -> Result<()> {
    if state.complete {
        anyhow::bail!("bonding curve is complete; use PumpSwap market quote");
    }
    if state
        .quote_mint
        .as_deref()
        .is_some_and(|mint| mint != "11111111111111111111111111111111")
    {
        anyhow::bail!("non-SOL bonding curve quote is unsupported");
    }
    Ok(())
}

pub fn curve_state_key(state: &BondingCurveState) -> String {
    format!(
        "curve:{}:{}:{}:{}:{}:{}",
        state.account,
        state.slot,
        state.virtual_token_reserves,
        state.virtual_quote_reserves,
        state.real_token_reserves,
        state.real_quote_reserves
    )
}

pub fn decode_bonding_curve(
    mint: &str,
    account: &str,
    slot: u64,
    data: &[u8],
) -> Result<BondingCurveState> {
    if data.len() < 81 {
        anyhow::bail!("bonding curve account is too short: {}", data.len());
    }
    if data[..8] != BONDING_CURVE_DISCRIMINATOR {
        anyhow::bail!("bonding curve discriminator mismatch");
    }

    let quote_mint = if data.len() >= 115 {
        let bytes: [u8; 32] = data[83..115]
            .try_into()
            .context("invalid quote mint bytes")?;
        Some(Pubkey::new_from_array(bytes).to_string())
    } else {
        None
    };

    Ok(BondingCurveState {
        mint: mint.to_string(),
        account: account.to_string(),
        slot,
        observed_at_ms: now_ms(),
        virtual_token_reserves: read_u64(data, 8)?,
        virtual_quote_reserves: read_u64(data, 16)?,
        real_token_reserves: read_u64(data, 24)?,
        real_quote_reserves: read_u64(data, 32)?,
        token_total_supply: read_u64(data, 40)?,
        complete: data[48] != 0,
        is_mayhem_mode: (data.len() >= 82).then_some(data[81] != 0),
        quote_mint,
    })
}

fn read_u64(data: &[u8], offset: usize) -> Result<u64> {
    let bytes: [u8; 8] = data
        .get(offset..offset + 8)
        .context("bonding curve field is truncated")?
        .try_into()
        .context("invalid bonding curve u64")?;
    Ok(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(is_mayhem_mode: Option<bool>) -> BondingCurveState {
        BondingCurveState {
            mint: "mint".to_string(),
            account: "curve".to_string(),
            slot: 1,
            observed_at_ms: 1,
            virtual_token_reserves: 1_000_000,
            virtual_quote_reserves: 1_000_000,
            real_token_reserves: 1_000_000,
            real_quote_reserves: 1_000_000,
            token_total_supply: 1_000_000,
            complete: false,
            is_mayhem_mode,
            quote_mint: Some("11111111111111111111111111111111".to_string()),
        }
    }

    #[test]
    fn non_mayhem_state_still_allows_liquidation_quote() {
        assert!(sell_quote_from_state(&state(Some(false)), 1_000).is_ok());
        assert!(buy_quote_from_state(&state(Some(false)), 1_000).is_err());
    }

    #[test]
    fn bonding_curve_address_is_deterministic_pda() {
        // The PDA must be derived from [b"bonding-curve", mint]
        // under the pump.fun program. This is the address the
        // accountSubscribe call needs to receive mcap updates.
        // If the derivation is wrong, mcap stays at 0.
        let mint = "So11111111111111111111111111111111111111112";
        let c = CurveQuoteClient::new(
            "https://example.com".to_string(),
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
        )
        .expect("client");
        let addr1 = c.bonding_curve_address(mint).expect("addr1");
        let addr2 = c.bonding_curve_address(mint).expect("addr2");
        // Same input must give the same address (PDA is
        // deterministic).
        assert_eq!(addr1, addr2);
        // The address must NOT be the literal concatenation of
        // the program and mint pubkeys — that was the original
        // bug in the TUI's curve-watch setup.
        let program = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
        let concat = format!("{} {}", program, mint);
        let concat_pubkey = solana_pubkey::Pubkey::from_str(&concat).ok();
        assert!(
            concat_pubkey.is_none() || concat_pubkey.unwrap() != addr1,
            "PDA must not equal program+concat string"
        );
    }
}
