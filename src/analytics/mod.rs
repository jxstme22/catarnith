use crate::{curve::BondingCurveState, market::MarketStats};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryFeatures {
    pub mint: String,
    pub order_id: String,
    pub source_signature: Option<String>,
    pub discovery_source: String,
    pub discovery_seen_ts_ms: i64,
    #[serde(default)]
    pub source_signal_slot: u64,
    pub entry_ts_ms: i64,
    pub entry_latency_ms: i64,
    pub filled_lamports: u64,
    pub filled_token_amount_raw: u128,
    pub fee_lamports: u64,
    pub curve_account: Option<String>,
    pub curve_slot: Option<u64>,
    pub virtual_token_reserves: Option<u64>,
    pub virtual_quote_reserves: Option<u64>,
    pub real_token_reserves: Option<u64>,
    pub real_quote_reserves: Option<u64>,
    pub estimated_price_impact_bps: Option<u64>,
    pub observed_buys: u64,
    pub observed_sells: u64,
    pub observed_unique_buyers: usize,
}

impl EntryFeatures {
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        mint: String,
        order_id: String,
        source_signature: Option<String>,
        discovery_source: String,
        discovery_seen_ts_ms: i64,
        source_signal_slot: u64,
        entry_ts_ms: i64,
        filled_lamports: u64,
        filled_token_amount_raw: u128,
        fee_lamports: u64,
        curve: Option<&BondingCurveState>,
        stats: MarketStats,
    ) -> Self {
        let estimated_price_impact_bps = curve.map(|state| {
            let input = filled_lamports as u128;
            let denominator = (state.virtual_quote_reserves as u128).saturating_add(input);
            if denominator == 0 {
                0
            } else {
                input
                    .saturating_mul(10_000)
                    .checked_div(denominator)
                    .unwrap_or_default()
                    .min(u64::MAX as u128) as u64
            }
        });
        Self {
            mint,
            order_id,
            source_signature,
            discovery_source,
            discovery_seen_ts_ms,
            source_signal_slot,
            entry_ts_ms,
            entry_latency_ms: entry_ts_ms.saturating_sub(discovery_seen_ts_ms),
            filled_lamports,
            filled_token_amount_raw,
            fee_lamports,
            curve_account: curve.map(|state| state.account.clone()),
            curve_slot: curve.map(|state| state.slot),
            virtual_token_reserves: curve.map(|state| state.virtual_token_reserves),
            virtual_quote_reserves: curve.map(|state| state.virtual_quote_reserves),
            real_token_reserves: curve.map(|state| state.real_token_reserves),
            real_quote_reserves: curve.map(|state| state.real_quote_reserves),
            estimated_price_impact_bps,
            observed_buys: stats.observed_buys,
            observed_sells: stats.observed_sells,
            observed_unique_buyers: stats.observed_unique_buyers,
        }
    }
}
