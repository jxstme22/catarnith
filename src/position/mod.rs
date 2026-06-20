use crate::{
    executor::Order,
    risk::RiskSnapshot,
    types::{now_ms, BuyOrder, ExecutionReport, ExecutionStatus, SellOrder},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionState {
    Candidate,
    Open,
    PartiallyExited,
    Closed,
    Stopped,
    Errored,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub mint: String,
    pub state: PositionState,
    pub entry_order_ids: Vec<String>,
    pub exit_order_ids: Vec<String>,
    pub entry_lamports: u64,
    pub token_amount_raw: u128,
    pub realized_lamports: i64,
    pub fee_lamports: u64,
    pub failed_fee_lamports: u64,
    pub buy_count: u32,
    #[serde(default)]
    pub has_taken_profit: bool,
    #[serde(default)]
    pub entry_quote_slot: Option<u64>,
    #[serde(default)]
    pub entry_confirmation_pending: bool,
    #[serde(default)]
    pub entry_signature: Option<String>,
    pub first_entry_ts_ms: Option<i64>,
    pub last_update_ts_ms: Option<i64>,
}

impl Position {
    fn new(mint: String) -> Self {
        Self {
            mint,
            state: PositionState::Candidate,
            entry_order_ids: Vec::new(),
            exit_order_ids: Vec::new(),
            entry_lamports: 0,
            token_amount_raw: 0,
            realized_lamports: 0,
            fee_lamports: 0,
            failed_fee_lamports: 0,
            buy_count: 0,
            has_taken_profit: false,
            entry_quote_slot: None,
            entry_confirmation_pending: false,
            entry_signature: None,
            first_entry_ts_ms: None,
            last_update_ts_ms: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct PositionManager {
    positions: HashMap<String, Position>,
}

impl PositionManager {
    pub fn restore(positions: impl IntoIterator<Item = Position>) -> Self {
        Self {
            positions: positions
                .into_iter()
                .map(|position| (position.mint.clone(), position))
                .collect(),
        }
    }

    pub fn has_open_position(&self, mint: &str) -> bool {
        self.positions.get(mint).is_some_and(|position| {
            matches!(
                position.state,
                PositionState::Open | PositionState::PartiallyExited
            )
        })
    }

    pub fn open_positions(&self) -> usize {
        self.positions
            .values()
            .filter(|position| {
                matches!(
                    position.state,
                    PositionState::Open | PositionState::PartiallyExited
                )
            })
            .count()
    }

    pub fn snapshot_for_mint(&self, mint: Option<&str>) -> RiskSnapshot {
        let daily_window_start_ms = now_ms().saturating_sub(86_400_000);
        let mut snapshot = RiskSnapshot {
            open_positions: self.open_positions(),
            total_open_lamports: self
                .positions
                .values()
                .filter(|position| {
                    matches!(
                        position.state,
                        PositionState::Open | PositionState::PartiallyExited
                    )
                })
                .map(|position| position.entry_lamports)
                .sum(),
            daily_realized_loss_lamports: self
                .positions
                .values()
                .filter(|position| {
                    position
                        .last_update_ts_ms
                        .is_none_or(|updated| updated >= daily_window_start_ms)
                })
                .filter(|position| position.realized_lamports < 0)
                .map(|position| position.realized_lamports.saturating_abs())
                .sum(),
            ..RiskSnapshot::default()
        };

        if let Some(mint) = mint {
            if let Some(position) = self.positions.get(mint) {
                snapshot.exposure_for_mint = position.entry_lamports;
                snapshot.buys_for_mint = position.buy_count;
            }
        }

        snapshot
    }

    pub fn positions(&self) -> impl Iterator<Item = &Position> {
        self.positions.values()
    }

    pub fn position_for_mint(&self, mint: &str) -> Option<&Position> {
        self.positions.get(mint)
    }

    pub fn token_amount_for_mint(&self, mint: &str) -> u128 {
        self.positions
            .get(mint)
            .map(|position| position.token_amount_raw)
            .unwrap_or_default()
    }

    pub fn record_order_report(&mut self, order: &Order, report: &ExecutionReport) {
        match order {
            Order::Buy(order) => self.record_buy(order, report),
            Order::Sell(order) => self.record_sell(order, report),
        }
    }

    pub fn record_buy(&mut self, order: &BuyOrder, report: &ExecutionReport) {
        let position = self
            .positions
            .entry(order.mint.clone())
            .or_insert_with(|| Position::new(order.mint.clone()));
        position.last_update_ts_ms = Some(order.timestamp_ms);

        if matches!(
            report.status,
            ExecutionStatus::PaperFilled
                | ExecutionStatus::LiveConfirmed
                | ExecutionStatus::LiveSubmitted
        ) {
            position.state = PositionState::Open;
            position.entry_order_ids.push(order.id.clone());
            let fee_lamports = report.fee_lamports.unwrap_or_default();
            position.entry_lamports = position
                .entry_lamports
                .saturating_add(report.filled_lamports.unwrap_or(order.lamports))
                .saturating_add(fee_lamports);
            position.token_amount_raw = position
                .token_amount_raw
                .saturating_add(report.filled_token_amount_raw.unwrap_or_default());
            position.fee_lamports = position.fee_lamports.saturating_add(fee_lamports);
            position.buy_count = position.buy_count.saturating_add(1);
            if position.first_entry_ts_ms.is_none() {
                position.first_entry_ts_ms = Some(order.timestamp_ms);
                position.has_taken_profit = false;
            }
            position.entry_quote_slot = match (position.entry_quote_slot, report.quote_slot) {
                (Some(current), Some(next)) => Some(current.max(next)),
                (current, next) => current.or(next),
            };
            position.entry_confirmation_pending = report.status == ExecutionStatus::LiveSubmitted;
            position.entry_signature.clone_from(&report.signature);
        } else if !matches!(
            report.status,
            ExecutionStatus::PaperRejected | ExecutionStatus::LiveSubmitted
        ) {
            position.state = PositionState::Errored;
            position.failed_fee_lamports = position
                .failed_fee_lamports
                .saturating_add(report.fee_lamports.unwrap_or_default());
        }
    }

    pub fn record_sell(&mut self, order: &SellOrder, report: &ExecutionReport) {
        let position = self
            .positions
            .entry(order.mint.clone())
            .or_insert_with(|| Position::new(order.mint.clone()));
        position.last_update_ts_ms = Some(order.timestamp_ms);
        position.exit_order_ids.push(order.id.clone());

        if matches!(
            report.status,
            ExecutionStatus::PaperFilled
                | ExecutionStatus::LiveConfirmed
                | ExecutionStatus::LiveReconciled
        ) {
            let fee_lamports = report.fee_lamports.unwrap_or_default();
            let proceeds_lamports = report.filled_lamports.unwrap_or_default();
            let held_before = position.token_amount_raw;
            let reported_sold_tokens = report
                .filled_token_amount_raw
                .unwrap_or(held_before)
                .min(held_before);
            let closes_pending_live_inventory = position.entry_confirmation_pending
                && report.status == ExecutionStatus::LiveConfirmed
                && report.signature.is_some()
                && reported_sold_tokens > 0;
            let sold_tokens = if closes_pending_live_inventory {
                held_before
            } else {
                reported_sold_tokens
            };
            let allocated_cost = if held_before == 0 {
                position.entry_lamports
            } else {
                (position.entry_lamports as u128)
                    .saturating_mul(sold_tokens)
                    .checked_div(held_before)
                    .unwrap_or_default()
                    .min(u64::MAX as u128) as u64
            };
            position.fee_lamports = position.fee_lamports.saturating_add(fee_lamports);
            let realized =
                proceeds_lamports as i128 - allocated_cost as i128 - fee_lamports as i128;
            position.realized_lamports = position
                .realized_lamports
                .saturating_add(realized.clamp(i64::MIN as i128, i64::MAX as i128) as i64);
            position.entry_lamports = position.entry_lamports.saturating_sub(allocated_cost);
            position.token_amount_raw = position.token_amount_raw.saturating_sub(sold_tokens);
            if position.token_amount_raw == 0 || sold_tokens == held_before {
                position.state = PositionState::Closed;
                position.entry_lamports = 0;
                position.token_amount_raw = 0;
                position.buy_count = 0;
                position.has_taken_profit = false;
                position.entry_quote_slot = None;
                position.entry_confirmation_pending = false;
                position.entry_signature = None;
                position.first_entry_ts_ms = None;
            } else {
                position.state = PositionState::PartiallyExited;
                position.has_taken_profit = true;
                if report.status == ExecutionStatus::LiveConfirmed && report.signature.is_some() {
                    position.entry_confirmation_pending = false;
                }
            }
        } else if !matches!(
            report.status,
            ExecutionStatus::PaperRejected | ExecutionStatus::LiveSubmitted
        ) {
            position.state = PositionState::Errored;
            position.failed_fee_lamports = position
                .failed_fee_lamports
                .saturating_add(report.fee_lamports.unwrap_or_default());
        }
    }

    pub fn reconcile_unlanded_provisional_buy(&mut self, order: &SellOrder) -> bool {
        let Some(position) = self.positions.get_mut(&order.mint) else {
            return false;
        };
        if !position.entry_confirmation_pending {
            return false;
        }

        position.exit_order_ids.push(order.id.clone());
        position.state = PositionState::Closed;
        position.entry_lamports = 0;
        position.token_amount_raw = 0;
        position.buy_count = 0;
        position.has_taken_profit = false;
        position.entry_quote_slot = None;
        position.entry_confirmation_pending = false;
        position.entry_signature = None;
        position.first_entry_ts_ms = None;
        position.last_update_ts_ms = Some(order.timestamp_ms);
        true
    }

    pub fn reconcile_external_inventory_depletion(&mut self, order: &SellOrder) -> bool {
        let Some(position) = self.positions.get_mut(&order.mint) else {
            return false;
        };
        if position.entry_confirmation_pending {
            return false;
        }

        position.exit_order_ids.push(order.id.clone());
        position.state = PositionState::Closed;
        position.entry_lamports = 0;
        position.token_amount_raw = 0;
        position.buy_count = 0;
        position.has_taken_profit = false;
        position.entry_quote_slot = None;
        position.entry_signature = None;
        position.first_entry_ts_ms = None;
        position.last_update_ts_ms = Some(order.timestamp_ms);
        true
    }

    pub fn defer_pending_inventory_recheck(&mut self, mint: &str, timestamp_ms: i64) -> bool {
        let Some(position) = self.positions.get_mut(mint) else {
            return false;
        };
        if !position.entry_confirmation_pending {
            return false;
        }
        position.last_update_ts_ms = Some(timestamp_ms);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ExecutionStatus;

    fn report(status: ExecutionStatus, token_amount_raw: u128) -> ExecutionReport {
        ExecutionReport {
            order_id: "order".to_string(),
            signature: Some("signature".to_string()),
            quote_slot: None,
            status,
            requested_lamports: 13_025_001,
            filled_lamports: Some(13_025_001),
            filled_token_amount_raw: Some(token_amount_raw),
            fee_lamports: Some(0),
            error: None,
            latency_ms: Some(1),
        }
    }

    #[test]
    fn provisional_live_buy_stays_open_until_inventory_is_reconciled() {
        let mint = "mint".to_string();
        let buy = BuyOrder {
            id: "buy".to_string(),
            timestamp_ms: 1_000,
            mint: mint.clone(),
            lamports: 13_025_001,
            source_decision_id: "decision-buy".to_string(),
            source_signature: None,
        };
        let sell = SellOrder {
            id: "sell".to_string(),
            timestamp_ms: 2_000,
            mint: mint.clone(),
            source_decision_id: "decision-sell".to_string(),
            source_signature: None,
        };
        let mut positions = PositionManager::default();

        positions.record_buy(&buy, &report(ExecutionStatus::LiveSubmitted, 500_000_000));
        let pending = positions
            .position_for_mint(&mint)
            .expect("provisional position should exist");
        assert_eq!(pending.state, PositionState::Open);
        assert!(pending.entry_confirmation_pending);
        assert_eq!(pending.entry_signature.as_deref(), Some("signature"));

        positions.record_sell(&sell, &report(ExecutionStatus::LiveReconciled, 500_000_000));
        let closed = positions
            .position_for_mint(&mint)
            .expect("reconciled position should exist");
        assert_eq!(closed.state, PositionState::Closed);
        assert!(!closed.entry_confirmation_pending);
        assert!(closed.entry_signature.is_none());
    }

    #[test]
    fn confirmed_sell_closes_optimistic_provisional_buy_inventory() {
        let mint = "mint".to_string();
        let buy = BuyOrder {
            id: "buy".to_string(),
            timestamp_ms: 1_000,
            mint: mint.clone(),
            lamports: 23_025_001,
            source_decision_id: "decision-buy".to_string(),
            source_signature: None,
        };
        let sell = SellOrder {
            id: "sell".to_string(),
            timestamp_ms: 4_000,
            mint: mint.clone(),
            source_decision_id: "decision-sell".to_string(),
            source_signature: None,
        };
        let buy_report = ExecutionReport {
            order_id: "buy".to_string(),
            signature: Some("buy-signature".to_string()),
            quote_slot: None,
            status: ExecutionStatus::LiveSubmitted,
            requested_lamports: 23_025_001,
            filled_lamports: Some(23_025_001),
            filled_token_amount_raw: Some(722_105_128_631),
            fee_lamports: Some(0),
            error: Some("confirmation_pending".to_string()),
            latency_ms: Some(1),
        };
        let sell_report = ExecutionReport {
            order_id: "sell".to_string(),
            signature: Some("sell-signature".to_string()),
            quote_slot: Some(426_447_692),
            status: ExecutionStatus::LiveConfirmed,
            requested_lamports: 0,
            filled_lamports: Some(19_770_087),
            filled_token_amount_raw: Some(699_422_546_473),
            fee_lamports: Some(0),
            error: None,
            latency_ms: Some(1_928),
        };
        let mut positions = PositionManager::default();

        positions.record_buy(&buy, &buy_report);
        positions.record_sell(&sell, &sell_report);

        let closed = positions
            .position_for_mint(&mint)
            .expect("sold provisional position should remain in history");
        assert_eq!(closed.state, PositionState::Closed);
        assert_eq!(closed.entry_lamports, 0);
        assert_eq!(closed.token_amount_raw, 0);
        assert_eq!(closed.realized_lamports, -3_254_914);
        assert!(!closed.entry_confirmation_pending);
        assert!(closed.entry_signature.is_none());
        assert!(closed.first_entry_ts_ms.is_none());
    }

    #[test]
    fn unlanded_provisional_buy_closes_without_realizing_phantom_loss() {
        let mint = "mint".to_string();
        let buy = BuyOrder {
            id: "buy".to_string(),
            timestamp_ms: 1_000,
            mint: mint.clone(),
            lamports: 13_025_001,
            source_decision_id: "decision-buy".to_string(),
            source_signature: None,
        };
        let sell = SellOrder {
            id: "sell".to_string(),
            timestamp_ms: 121_000,
            mint: mint.clone(),
            source_decision_id: "decision-sell".to_string(),
            source_signature: None,
        };
        let mut positions = PositionManager::default();

        positions.record_buy(&buy, &report(ExecutionStatus::LiveSubmitted, 500_000_000));
        assert!(positions.reconcile_unlanded_provisional_buy(&sell));

        let closed = positions
            .position_for_mint(&mint)
            .expect("reconciled position should exist");
        assert_eq!(closed.state, PositionState::Closed);
        assert_eq!(closed.entry_lamports, 0);
        assert_eq!(closed.token_amount_raw, 0);
        assert_eq!(closed.realized_lamports, 0);
        assert_eq!(closed.exit_order_ids, vec!["sell"]);
        assert!(!closed.entry_confirmation_pending);
        assert!(closed.entry_signature.is_none());
        assert!(closed.first_entry_ts_ms.is_none());
    }

    #[test]
    fn externally_depleted_inventory_closes_without_booking_a_fake_total_loss() {
        let mint = "mint".to_string();
        let buy = BuyOrder {
            id: "buy".to_string(),
            timestamp_ms: 1_000,
            mint: mint.clone(),
            lamports: 13_025_001,
            source_decision_id: "decision-buy".to_string(),
            source_signature: None,
        };
        let sell = SellOrder {
            id: "sell".to_string(),
            timestamp_ms: 5_000,
            mint: mint.clone(),
            source_decision_id: "decision-sell".to_string(),
            source_signature: None,
        };
        let mut positions = PositionManager::default();

        positions.record_buy(&buy, &report(ExecutionStatus::LiveConfirmed, 500_000_000));
        assert!(positions.reconcile_external_inventory_depletion(&sell));

        let closed = positions
            .position_for_mint(&mint)
            .expect("reconciled position should exist");
        assert_eq!(closed.state, PositionState::Closed);
        assert_eq!(closed.entry_lamports, 0);
        assert_eq!(closed.token_amount_raw, 0);
        assert_eq!(closed.realized_lamports, 0);
        assert_eq!(closed.exit_order_ids, vec!["sell"]);
    }

    #[test]
    fn pending_inventory_recheck_updates_only_provisional_positions() {
        let mint = "mint".to_string();
        let buy = BuyOrder {
            id: "buy".to_string(),
            timestamp_ms: 1_000,
            mint: mint.clone(),
            lamports: 13_025_001,
            source_decision_id: "decision-buy".to_string(),
            source_signature: None,
        };
        let mut positions = PositionManager::default();

        positions.record_buy(&buy, &report(ExecutionStatus::LiveSubmitted, 500_000_000));
        assert!(positions.defer_pending_inventory_recheck(&mint, 4_000));
        assert_eq!(
            positions
                .position_for_mint(&mint)
                .and_then(|position| position.last_update_ts_ms),
            Some(4_000)
        );

        let confirmed_mint = "confirmed".to_string();
        let mut confirmed_buy = buy;
        confirmed_buy.mint.clone_from(&confirmed_mint);
        positions.record_buy(
            &confirmed_buy,
            &report(ExecutionStatus::LiveConfirmed, 500_000_000),
        );
        assert!(!positions.defer_pending_inventory_recheck(&confirmed_mint, 5_000));
    }
}
