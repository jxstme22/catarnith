use crate::{
    market::MarketQuote,
    types::{
        now_ms, Action, BuyOrder, Decision, DecodedTx, ExecutionReport, ExecutionStatus, SellOrder,
        TradeSide,
    },
};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "side", rename_all = "snake_case")]
pub enum Order {
    Buy(BuyOrder),
    Sell(SellOrder),
}

impl Order {
    pub fn id(&self) -> &str {
        match self {
            Order::Buy(order) => &order.id,
            Order::Sell(order) => &order.id,
        }
    }

    pub fn mint(&self) -> &str {
        match self {
            Order::Buy(order) => &order.mint,
            Order::Sell(order) => &order.mint,
        }
    }
}

pub trait Executor {
    fn execute(&self, order: &Order) -> Result<ExecutionReport>;
}

#[derive(Debug, Default, Clone)]
pub struct PaperExecutor;

#[derive(Debug, Clone, Copy)]
pub struct PaperExecutionSettings {
    pub slippage_bps: u32,
    pub fee_lamports_floor: u64,
}

impl PaperExecutor {
    pub fn reject(&self, order: &Order, reason: &str) -> ExecutionReport {
        paper_rejected(order, reason)
    }

    pub fn execute_quote(
        &self,
        order: &Order,
        quote: &MarketQuote,
        held_token_amount_raw: u128,
        settings: PaperExecutionSettings,
    ) -> Result<ExecutionReport> {
        if settings.slippage_bps >= 10_000 {
            return Ok(paper_rejected(order, "paper_slippage_bps_invalid"));
        }
        if order.mint() != quote.mint {
            return Ok(paper_rejected(order, "market_quote_mint_mismatch"));
        }

        match order {
            Order::Buy(order) => {
                if quote.side != TradeSide::Buy {
                    return Ok(paper_rejected(
                        &Order::Buy(order.clone()),
                        "market_quote_side_not_buy",
                    ));
                }
                let quoted_tokens = scale_ratio(
                    order.lamports as u128,
                    quote.token_amount_raw,
                    quote.sol_lamports,
                );
                let filled_tokens = apply_adverse_slippage(quoted_tokens, settings.slippage_bps);
                if filled_tokens == 0 {
                    return Ok(paper_rejected(
                        &Order::Buy(order.clone()),
                        "market_buy_fill_rounded_to_zero",
                    ));
                }
                Ok(ExecutionReport {
                    order_id: order.id.clone(),
                    signature: Some(quote.signature.clone()),
                    quote_slot: Some(quote.slot),
                    status: ExecutionStatus::PaperFilled,
                    requested_lamports: order.lamports,
                    filled_lamports: Some(order.lamports),
                    filled_token_amount_raw: Some(filled_tokens),
                    fee_lamports: Some(settings.fee_lamports_floor),
                    error: None,
                    latency_ms: Some(now_ms().saturating_sub(quote.observed_at_ms).max(0) as u64),
                })
            }
            Order::Sell(order) => {
                if held_token_amount_raw == 0 {
                    return Ok(paper_rejected(
                        &Order::Sell(order.clone()),
                        "no_paper_token_inventory",
                    ));
                }
                if quote.side != TradeSide::Sell {
                    return Ok(paper_rejected(
                        &Order::Sell(order.clone()),
                        "market_quote_side_not_sell",
                    ));
                }
                if quote.token_amount_raw < held_token_amount_raw {
                    return Ok(paper_rejected(
                        &Order::Sell(order.clone()),
                        "market_sell_observation_too_small",
                    ));
                }
                let quoted_lamports = scale_ratio(
                    held_token_amount_raw,
                    quote.sol_lamports,
                    quote.token_amount_raw,
                );
                let filled_lamports = apply_adverse_slippage(quoted_lamports, settings.slippage_bps)
                    .min(u64::MAX as u128) as u64;
                Ok(ExecutionReport {
                    order_id: order.id.clone(),
                    signature: Some(quote.signature.clone()),
                    quote_slot: Some(quote.slot),
                    status: ExecutionStatus::PaperFilled,
                    requested_lamports: 0,
                    filled_lamports: Some(filled_lamports),
                    filled_token_amount_raw: Some(held_token_amount_raw),
                    fee_lamports: Some(settings.fee_lamports_floor),
                    error: None,
                    latency_ms: Some(now_ms().saturating_sub(quote.observed_at_ms).max(0) as u64),
                })
            }
        }
    }

    pub fn force_close_unpriced(
        &self,
        order: &Order,
        held_token_amount_raw: u128,
        settings: PaperExecutionSettings,
    ) -> ExecutionReport {
        let Order::Sell(order) = order else {
            return paper_rejected(order, "unpriced_force_close_requires_sell");
        };
        if held_token_amount_raw == 0 {
            return paper_rejected(&Order::Sell(order.clone()), "no_paper_token_inventory");
        }

        ExecutionReport {
            order_id: order.id.clone(),
            signature: None,
            quote_slot: None,
            status: ExecutionStatus::PaperFilled,
            requested_lamports: 0,
            filled_lamports: Some(0),
            filled_token_amount_raw: Some(held_token_amount_raw),
            fee_lamports: Some(settings.fee_lamports_floor),
            error: None,
            latency_ms: Some(0),
        }
    }

    pub fn execute_observed(
        &self,
        order: &Order,
        observation: &DecodedTx,
        held_token_amount_raw: u128,
        settings: PaperExecutionSettings,
    ) -> Result<ExecutionReport> {
        if settings.slippage_bps >= 10_000 {
            return Ok(paper_rejected(order, "paper_slippage_bps_invalid"));
        }

        match order {
            Order::Buy(order) => {
                if observation.side != TradeSide::Buy {
                    return Ok(paper_rejected(
                        &Order::Buy(order.clone()),
                        "observed_market_side_not_buy",
                    ));
                }
                let Some(observed_spend) = observation
                    .sol_delta_lamports
                    .filter(|delta| *delta < 0)
                    .and_then(|delta| delta.checked_abs())
                    .map(|delta| delta as u128)
                else {
                    return Ok(paper_rejected(
                        &Order::Buy(order.clone()),
                        "missing_observed_buy_sol_delta",
                    ));
                };
                let Some(observed_tokens) = observation
                    .token_delta_raw
                    .filter(|delta| *delta > 0)
                    .map(|delta| delta as u128)
                else {
                    return Ok(paper_rejected(
                        &Order::Buy(order.clone()),
                        "missing_observed_buy_token_delta",
                    ));
                };
                let quoted_tokens =
                    scale_ratio(order.lamports as u128, observed_tokens, observed_spend);
                let filled_tokens = apply_adverse_slippage(quoted_tokens, settings.slippage_bps);
                if filled_tokens == 0 {
                    return Ok(paper_rejected(
                        &Order::Buy(order.clone()),
                        "observed_buy_fill_rounded_to_zero",
                    ));
                }
                Ok(ExecutionReport {
                    order_id: order.id.clone(),
                    signature: None,
                    quote_slot: Some(observation.slot),
                    status: ExecutionStatus::PaperFilled,
                    requested_lamports: order.lamports,
                    filled_lamports: Some(order.lamports),
                    filled_token_amount_raw: Some(filled_tokens),
                    fee_lamports: Some(observed_fee(observation, settings)),
                    error: None,
                    latency_ms: Some(0),
                })
            }
            Order::Sell(order) => {
                if held_token_amount_raw == 0 {
                    return Ok(paper_rejected(
                        &Order::Sell(order.clone()),
                        "no_paper_token_inventory",
                    ));
                }
                if observation.side != TradeSide::Sell {
                    return Ok(paper_rejected(
                        &Order::Sell(order.clone()),
                        "observed_market_side_not_sell",
                    ));
                }
                let Some(observed_receive) = observation
                    .sol_delta_lamports
                    .filter(|delta| *delta > 0)
                    .map(|delta| delta as u128)
                else {
                    return Ok(paper_rejected(
                        &Order::Sell(order.clone()),
                        "missing_observed_sell_sol_delta",
                    ));
                };
                let Some(observed_tokens_sold) = observation
                    .token_delta_raw
                    .filter(|delta| *delta < 0)
                    .and_then(|delta| delta.checked_abs())
                    .map(|delta| delta as u128)
                else {
                    return Ok(paper_rejected(
                        &Order::Sell(order.clone()),
                        "missing_observed_sell_token_delta",
                    ));
                };
                if observed_tokens_sold < held_token_amount_raw {
                    return Ok(paper_rejected(
                        &Order::Sell(order.clone()),
                        "observed_sell_size_too_small",
                    ));
                }
                let quoted_lamports = scale_ratio(
                    held_token_amount_raw,
                    observed_receive,
                    observed_tokens_sold,
                );
                let filled_lamports = apply_adverse_slippage(quoted_lamports, settings.slippage_bps)
                    .min(u64::MAX as u128) as u64;
                Ok(ExecutionReport {
                    order_id: order.id.clone(),
                    signature: None,
                    quote_slot: Some(observation.slot),
                    status: ExecutionStatus::PaperFilled,
                    requested_lamports: 0,
                    filled_lamports: Some(filled_lamports),
                    filled_token_amount_raw: Some(held_token_amount_raw),
                    fee_lamports: Some(observed_fee(observation, settings)),
                    error: None,
                    latency_ms: Some(0),
                })
            }
        }
    }
}

impl Executor for PaperExecutor {
    fn execute(&self, order: &Order) -> Result<ExecutionReport> {
        Ok(paper_rejected(order, "observed_market_context_required"))
    }
}

fn observed_fee(observation: &DecodedTx, settings: PaperExecutionSettings) -> u64 {
    observation
        .fee_lamports
        .unwrap_or_default()
        .max(settings.fee_lamports_floor)
}

fn scale_ratio(quantity: u128, numerator: u128, denominator: u128) -> u128 {
    if denominator == 0 {
        return 0;
    }
    quantity.saturating_mul(numerator) / denominator
}

fn apply_adverse_slippage(value: u128, slippage_bps: u32) -> u128 {
    scale_ratio(value, (10_000 - slippage_bps) as u128, 10_000)
}

fn paper_rejected(order: &Order, reason: &str) -> ExecutionReport {
    ExecutionReport {
        order_id: order.id().to_string(),
        signature: None,
        quote_slot: None,
        status: ExecutionStatus::PaperRejected,
        requested_lamports: match order {
            Order::Buy(order) => order.lamports,
            Order::Sell(_) => 0,
        },
        filled_lamports: None,
        filled_token_amount_raw: None,
        fee_lamports: Some(0),
        error: Some(reason.to_string()),
        latency_ms: Some(0),
    }
}

pub fn order_from_decision(decision: &Decision) -> Option<Order> {
    if !decision.risk_approved {
        return None;
    }

    let mint = decision.mint.clone()?;
    let timestamp_ms = decision.timestamp_ms.max(now_ms());
    match decision.action {
        Action::Buy => Some(Order::Buy(BuyOrder {
            id: format!("order-buy-{}-{}", timestamp_ms, mint_prefix(&mint)),
            timestamp_ms,
            mint,
            lamports: decision.requested_lamports?,
            source_decision_id: decision.id.clone(),
            source_signature: decision.source_signature.clone(),
        })),
        Action::Sell => Some(Order::Sell(SellOrder {
            id: format!("order-sell-{}-{}", timestamp_ms, mint_prefix(&mint)),
            timestamp_ms,
            mint,
            source_decision_id: decision.id.clone(),
            source_signature: decision.source_signature.clone(),
        })),
        _ => None,
    }
}

fn mint_prefix(mint: &str) -> String {
    mint.chars().take(8).collect()
}

/// Build a `SellOrder` from a mint and an origin label, without going
/// through the `Decision` pipeline. Used by the TUI panic-sell path so
/// an emergency exit still produces a real `Order` and a journal row.
pub fn order_from_decision_sell(mint: &str, origin: &str) -> SellOrder {
    let timestamp_ms = crate::types::now_ms();
    SellOrder {
        id: format!("order-sell-panic-{}-{}", timestamp_ms, mint_prefix(mint)),
        timestamp_ms,
        mint: mint.to_string(),
        source_decision_id: format!("panic-sell:{origin}"),
        source_signature: None,
    }
}

#[cfg(test)]
mod panic_sell_tests {
    use super::*;

    #[test]
    fn order_from_decision_sell_has_panic_origin_and_mint() {
        let order = order_from_decision_sell(
            "So11111111111111111111111111111111111111112",
            "tui-emergency",
        );
        assert_eq!(order.mint, "So11111111111111111111111111111111111111112");
        assert!(order.source_decision_id.contains("tui-emergency"));
        assert!(order.id.starts_with("order-sell-panic-"));
        assert!(order.source_signature.is_none());
    }
}
