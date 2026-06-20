use crate::types::{now_ms, DecodedTx, TradeSide};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketQuote {
    pub mint: String,
    pub slot: u64,
    pub timestamp_ms: i64,
    pub observed_at_ms: i64,
    pub signature: String,
    pub side: TradeSide,
    pub sol_lamports: u128,
    pub token_amount_raw: u128,
    #[serde(default)]
    pub trader: Option<String>,
}

impl MarketQuote {
    pub fn from_decoded(decoded: &DecodedTx) -> Option<Self> {
        if !decoded.ok || !matches!(decoded.side, TradeSide::Buy | TradeSide::Sell) {
            return None;
        }
        let mint = decoded.mint.clone()?;
        let sol_delta = decoded.sol_delta_lamports?;
        let token_delta = decoded.token_delta_raw?;
        let direction_matches = match decoded.side {
            TradeSide::Buy => sol_delta < 0 && token_delta > 0,
            TradeSide::Sell => sol_delta > 0 && token_delta < 0,
            _ => false,
        };
        if !direction_matches {
            return None;
        }
        let sol_lamports = sol_delta.unsigned_abs() as u128;
        let token_amount_raw = token_delta.unsigned_abs();
        if sol_lamports == 0 || token_amount_raw == 0 {
            return None;
        }

        Some(Self {
            mint,
            slot: decoded.slot,
            timestamp_ms: decoded.timestamp_ms.unwrap_or_else(now_ms),
            observed_at_ms: now_ms(),
            signature: decoded.signature.clone(),
            side: decoded.side,
            sol_lamports,
            token_amount_raw,
            trader: decoded.signer.clone(),
        })
    }

    pub fn age_ms(&self, current_ms: i64) -> i64 {
        current_ms.saturating_sub(self.observed_at_ms)
    }
}

#[derive(Debug, Default)]
pub struct MarketTracker {
    latest_by_mint: HashMap<String, MarketQuote>,
    latest_sell_by_mint: HashMap<String, MarketQuote>,
    stats_by_mint: HashMap<String, MarketStats>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketStats {
    pub observed_buys: u64,
    pub observed_sells: u64,
    pub observed_unique_buyers: usize,
    #[serde(skip)]
    buyers: HashSet<String>,
}

impl MarketTracker {
    pub fn update(&mut self, quote: MarketQuote) {
        if quote.trader.is_some() {
            let stats = self.stats_by_mint.entry(quote.mint.clone()).or_default();
            if quote.side == TradeSide::Buy {
                stats.observed_buys = stats.observed_buys.saturating_add(1);
                if let Some(trader) = quote.trader.as_ref() {
                    stats.buyers.insert(trader.clone());
                    stats.observed_unique_buyers = stats.buyers.len();
                }
            } else if quote.side == TradeSide::Sell {
                stats.observed_sells = stats.observed_sells.saturating_add(1);
                let replace = self
                    .latest_sell_by_mint
                    .get(&quote.mint)
                    .is_none_or(|current| {
                        (quote.slot, quote.timestamp_ms, &quote.signature)
                            > (current.slot, current.timestamp_ms, &current.signature)
                    });
                if replace {
                    self.latest_sell_by_mint
                        .insert(quote.mint.clone(), quote.clone());
                }
            }
        }
        let replace = self.latest_by_mint.get(&quote.mint).is_none_or(|current| {
            (quote.slot, quote.timestamp_ms, &quote.signature)
                > (current.slot, current.timestamp_ms, &current.signature)
        });
        if replace {
            self.latest_by_mint.insert(quote.mint.clone(), quote);
        }
    }

    pub fn latest(&self, mint: &str) -> Option<&MarketQuote> {
        self.latest_by_mint.get(mint)
    }

    pub fn latest_sell(&self, mint: &str) -> Option<&MarketQuote> {
        self.latest_sell_by_mint.get(mint)
    }

    pub fn stats(&self, mint: &str) -> MarketStats {
        self.stats_by_mint.get(mint).cloned().unwrap_or_default()
    }
}
