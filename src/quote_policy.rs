use crate::{
    curve::{sell_quote_from_state, BondingCurveState},
    market::MarketQuote,
    types::TradeSide,
};
use anyhow::{bail, Result};

pub fn validate_entry_curve_slot(
    signal_slot: u64,
    curve_slot: u64,
    max_ahead_slots: u64,
) -> Result<()> {
    if signal_slot == 0 {
        return Ok(());
    }
    if curve_slot < signal_slot {
        bail!("entry_curve_slot_behind signal_slot={signal_slot} curve_slot={curve_slot}");
    }
    let ahead_slots = curve_slot.saturating_sub(signal_slot);
    if ahead_slots > max_ahead_slots {
        bail!(
            "entry_curve_slot_ahead signal_slot={signal_slot} curve_slot={curve_slot} ahead_slots={ahead_slots} max_ahead_slots={max_ahead_slots}"
        );
    }
    Ok(())
}

pub fn causal_curve_exit_quote(
    state: &BondingCurveState,
    entry_slot: Option<u64>,
    held_token_amount_raw: u128,
    current_ms: i64,
    max_age_ms: i64,
) -> Option<MarketQuote> {
    let entry_slot = entry_slot?;
    if state.slot < entry_slot || quote_age_ms(state.observed_at_ms, current_ms) > max_age_ms {
        return None;
    }
    sell_quote_from_state(state, held_token_amount_raw).ok()
}

pub fn causal_trade_exit_quote(
    quote: &MarketQuote,
    entry_slot: Option<u64>,
    held_token_amount_raw: u128,
    current_ms: i64,
    max_age_ms: i64,
) -> Option<MarketQuote> {
    let entry_slot = entry_slot?;
    if quote.side != TradeSide::Sell
        || quote.trader.is_none()
        || quote.slot < entry_slot
        || quote.age_ms(current_ms) > max_age_ms
        || quote.token_amount_raw < held_token_amount_raw
    {
        return None;
    }
    Some(quote.clone())
}

fn quote_age_ms(observed_at_ms: i64, current_ms: i64) -> i64 {
    current_ms.saturating_sub(observed_at_ms).max(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(slot: u64, observed_at_ms: i64) -> BondingCurveState {
        BondingCurveState {
            mint: "mint".to_string(),
            account: "curve".to_string(),
            slot,
            observed_at_ms,
            virtual_token_reserves: 1_000_000_000,
            virtual_quote_reserves: 1_000_000_000,
            real_token_reserves: 1_000_000_000,
            real_quote_reserves: 1_000_000_000,
            token_total_supply: 1_000_000_000,
            complete: false,
            is_mayhem_mode: Some(true),
            quote_mint: Some("11111111111111111111111111111111".to_string()),
        }
    }

    fn trade(slot: u64, side: TradeSide, token_amount_raw: u128) -> MarketQuote {
        MarketQuote {
            mint: "mint".to_string(),
            slot,
            timestamp_ms: 2_000,
            observed_at_ms: 2_000,
            signature: "trade".to_string(),
            side,
            sol_lamports: 10_000_000,
            token_amount_raw,
            trader: Some("trader".to_string()),
        }
    }

    #[test]
    fn rejects_far_future_entry_curve_state() {
        let error = validate_entry_curve_slot(100, 440, 8).unwrap_err();
        assert!(error.to_string().contains("entry_curve_slot_ahead"));
    }

    #[test]
    fn rejects_pre_entry_curve_and_trade_quotes() {
        assert!(
            causal_curve_exit_quote(&state(99, 2_000), Some(100), 10_000, 2_000, 2_000).is_none()
        );
        assert!(causal_trade_exit_quote(
            &trade(99, TradeSide::Sell, 10_000),
            Some(100),
            10_000,
            2_000,
            2_000,
        )
        .is_none());
    }

    #[test]
    fn trade_fallback_requires_sell_side_and_full_observed_size() {
        assert!(causal_trade_exit_quote(
            &trade(101, TradeSide::Buy, 20_000),
            Some(100),
            10_000,
            2_000,
            2_000,
        )
        .is_none());
        assert!(causal_trade_exit_quote(
            &trade(101, TradeSide::Sell, 9_999),
            Some(100),
            10_000,
            2_000,
            2_000,
        )
        .is_none());
        assert!(causal_trade_exit_quote(
            &trade(101, TradeSide::Sell, 10_000),
            Some(100),
            10_000,
            2_000,
            2_000,
        )
        .is_some());
    }
}
