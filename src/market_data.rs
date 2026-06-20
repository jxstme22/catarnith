//! Shared on-chain market data service.
//!
//! `MarketData` fetches and caches the two numbers catarnith needs:
//!   - SOL/USD from Pyth Hermes
//!   - bonding-curve state from Helius RPC
//!
//! All mcap and position-USD math is derived from the curve state,
//! so paper and live modes see the exact same numbers.

use crate::{
    curve::{buy_quote_from_state, sell_quote_from_state, BondingCurveState, CurveQuoteClient},
    pyth::fetch_sol_price_usd,
};
use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

const SOL_PRICE_TTL: Duration = Duration::from_secs(5);
const CURVE_TTL: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct MarketData {
    rpc_url: String,
    pumpfun_program: String,
    sol_price: std::sync::Arc<Mutex<Option<(f64, Instant)>>>,
    curve_cache: std::sync::Arc<Mutex<HashMap<String, (BondingCurveState, Instant)>>>,
}

impl MarketData {
    pub fn new(rpc_url: String, pumpfun_program: String) -> Self {
        Self {
            rpc_url,
            pumpfun_program,
            sol_price: std::sync::Arc::new(Mutex::new(None)),
            curve_cache: std::sync::Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Replace the RPC endpoint and program ID after the config for
    /// the active mode is known. Used when catarnith starts with a
    /// fallback/default config and only loads the real config later.
    pub fn reconfigure(&mut self, rpc_url: String, pumpfun_program: String) {
        self.rpc_url = rpc_url;
        self.pumpfun_program = pumpfun_program;
    }

    /// SOL/USD from Pyth, cached for SOL_PRICE_TTL.
    pub async fn sol_price_usd(&self) -> Result<f64> {
        {
            let guard = self.sol_price.lock().unwrap();
            if let Some((price, fetched_at)) = guard.as_ref() {
                if fetched_at.elapsed() < SOL_PRICE_TTL {
                    return Ok(*price);
                }
            }
        }
        let price = fetch_sol_price_usd()
            .await
            .context("fetch SOL/USD from Pyth")?;
        *self.sol_price.lock().unwrap() = Some((price, Instant::now()));
        Ok(price)
    }

    /// Bonding-curve state for `mint`, cached for CURVE_TTL.
    pub async fn curve_state(&self, mint: &str) -> Result<BondingCurveState> {
        {
            let guard = self.curve_cache.lock().unwrap();
            if let Some((state, fetched_at)) = guard.get(mint) {
                if fetched_at.elapsed() < CURVE_TTL {
                    return Ok(state.clone());
                }
            }
        }
        let client = CurveQuoteClient::new(self.rpc_url.clone(), &self.pumpfun_program)
            .context("construct curve client")?;
        let state = client
            .fetch_state(mint)
            .await
            .context("fetch curve state")?;
        self.curve_cache
            .lock()
            .unwrap()
            .insert(mint.to_string(), (state.clone(), Instant::now()));
        Ok(state)
    }

    /// Full market-cap in USD from curve reserves.
    pub fn mcap_usd(state: &BondingCurveState, sol_price_usd: f64) -> f64 {
        if state.virtual_token_reserves == 0 {
            return 0.0;
        }
        let mcap_sol = (state.token_total_supply as f64) * (state.virtual_quote_reserves as f64)
            / (state.virtual_token_reserves as f64)
            / 1_000_000_000.0;
        mcap_sol * sol_price_usd
    }

    /// Mark-to-market USD value of `token_amount_raw` tokens.
    /// `virtual_quote_reserves` are lamports, so the intermediate
    /// product is lamports and must be divided by 1e9 to get SOL.
    pub fn position_usd(
        state: &BondingCurveState,
        token_amount_raw: u128,
        sol_price_usd: f64,
    ) -> f64 {
        if state.virtual_token_reserves == 0 {
            return 0.0;
        }
        let price_lamports_per_token =
            state.virtual_quote_reserves as f64 / state.virtual_token_reserves as f64;
        let position_lamports = token_amount_raw as f64 * price_lamports_per_token;
        (position_lamports / 1_000_000_000.0) * sol_price_usd
    }

    /// Paper buy: how many tokens does `lamports` buy on this curve
    /// after adverse slippage.
    pub async fn paper_buy_tokens(
        &self,
        mint: &str,
        lamports: u64,
        slippage_bps: u32,
    ) -> Result<u128> {
        let state = self.curve_state(mint).await?;
        let quote =
            buy_quote_from_state(&state, lamports as u128).context("buy quote from state")?;
        let slippage = (10_000 - slippage_bps) as f64 / 10_000.0;
        Ok((quote.token_amount_raw as f64 * slippage) as u128)
    }

    /// Paper sell: how many lamports does `token_amount_raw` return
    /// after adverse slippage.
    pub async fn paper_sell_lamports(
        &self,
        mint: &str,
        token_amount_raw: u128,
        slippage_bps: u32,
    ) -> Result<u64> {
        let state = self.curve_state(mint).await?;
        let quote =
            sell_quote_from_state(&state, token_amount_raw).context("sell quote from state")?;
        let slippage = (10_000 - slippage_bps) as f64 / 10_000.0;
        Ok((quote.sol_lamports as f64 * slippage) as u64)
    }
}
