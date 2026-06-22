//! Execution wrapper for catarnith.
//!
//! Dispatches to `LivePumpExecutor` in live mode and to a curve-backed
//! paper simulator in paper mode. Paper mode never signs or sends a
//! transaction; fills are computed from the same on-chain curve data
//! that live mode would hit.

use anyhow::{Context, Result};
use catarnith::{
    config::Config,
    curve::{buy_quote_from_state, sell_quote_from_state},
    executor::{order_from_decision_sell, Order, PaperExecutionSettings, PaperExecutor},
    live::LivePumpExecutor,
    market_data::MarketData,
    types::{ExecutionReport, Mode},
};
use std::sync::Arc;

#[allow(clippy::large_enum_variant)]
pub enum ScanExecutor {
    Live(LivePumpExecutor),
    Paper {
        paper: PaperExecutor,
        market: Arc<MarketData>,
        cfg: Config,
    },
}

impl ScanExecutor {
    pub async fn new(cfg: &Config, market: Arc<MarketData>) -> Result<Self> {
        if cfg.mode == Mode::Paper {
            Ok(Self::Paper {
                paper: PaperExecutor,
                market,
                cfg: cfg.clone(),
            })
        } else {
            let live = LivePumpExecutor::new(cfg)
                .await
                .context("construct LivePumpExecutor for scan")?;
            Ok(Self::Live(live))
        }
    }

    pub async fn execute(
        &self,
        order: &Order,
        sell_token_amount_raw: Option<u128>,
        buy_slippage_bps: Option<u32>,
    ) -> Result<ExecutionReport> {
        match self {
            Self::Live(e) => {
                e.execute(order, sell_token_amount_raw, buy_slippage_bps)
                    .await
            }
            Self::Paper { paper, market, cfg } => {
                let settings = PaperExecutionSettings {
                    slippage_bps: cfg.paper_slippage_bps,
                    fee_lamports_floor: cfg.paper_fee_lamports_floor,
                };
                match order {
                    Order::Buy(buy) => {
                        let state = market.curve_state(&buy.mint).await?;
                        let quote = buy_quote_from_state(&state, buy.lamports as u128)
                            .context("paper buy quote")?;
                        paper.execute_quote(order, &quote, 0, settings)
                    }
                    Order::Sell(_) => {
                        let amount = sell_token_amount_raw.unwrap_or(0);
                        let state = market.curve_state(order.mint()).await?;
                        let quote =
                            sell_quote_from_state(&state, amount).context("paper sell quote")?;
                        paper.execute_quote(order, &quote, amount, settings)
                    }
                }
            }
        }
    }

    pub async fn panic_sell_with_slippage(
        &self,
        mint: &str,
        amount: u128,
        slippage_bps: Option<u32>,
    ) -> Result<ExecutionReport> {
        match self {
            Self::Live(e) => {
                if let Some(bps) = slippage_bps {
                    e.panic_sell_with_slippage(mint, amount, bps).await
                } else {
                    e.panic_sell(mint, amount).await
                }
            }
            Self::Paper { paper, market, cfg } => {
                let state = market.curve_state(mint).await?;
                let quote = sell_quote_from_state(&state, amount).context("paper sell quote")?;
                let settings = PaperExecutionSettings {
                    slippage_bps: slippage_bps.unwrap_or(cfg.paper_slippage_bps),
                    fee_lamports_floor: cfg.paper_fee_lamports_floor,
                };
                let order = Order::Sell(order_from_decision_sell(mint, "paper-panic-sell"));
                paper.execute_quote(&order, &quote, amount, settings)
            }
        }
    }

    pub async fn fetch_token_balance(&self, mint: &str) -> Result<u128> {
        match self {
            Self::Live(e) => e.fetch_token_balance(mint).await,
            Self::Paper { .. } => Ok(0),
        }
    }

    /// Last-resort sell through Jupiter. Live-only; the local sell path
    /// is always tried first. Bails on paper mode and when no
    /// `JUP_API_KEY` is configured.
    pub async fn jupiter_sell_fallback(&self, mint: &str, amount: u128) -> Result<ExecutionReport> {
        match self {
            Self::Live(e) => e.jupiter_sell(mint, amount).await,
            Self::Paper { .. } => anyhow::bail!("jupiter fallback is live-only"),
        }
    }

    pub fn is_paper(&self) -> bool {
        matches!(self, Self::Paper { .. })
    }

    pub fn wallet_label(&self) -> String {
        match self {
            Self::Paper { .. } => "PAPER".to_string(),
            Self::Live(e) => e
                .wallet()
                .map(|p| p.to_string())
                .unwrap_or_else(|| "LIVE".to_string()),
        }
    }
}
