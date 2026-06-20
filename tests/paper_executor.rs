mod common;

use catarnith::{
    executor::{Order, PaperExecutionSettings, PaperExecutor},
    market::MarketQuote,
    position::PositionState,
    types::{BuyOrder, ExecutionStatus, SellOrder, TradeSide},
};
use common::decoded_buy;

#[test]
fn observed_paper_executor_scales_real_market_fill_and_pnl() {
    let executor = PaperExecutor;
    let settings = PaperExecutionSettings {
        slippage_bps: 500,
        fee_lamports_floor: 5_000,
    };
    let mint = "paper-mint".to_string();
    let buy_order = Order::Buy(BuyOrder {
        id: "paper-buy".to_string(),
        timestamp_ms: 1_000,
        mint: mint.clone(),
        lamports: 5_000_000,
        source_decision_id: "decision-buy".to_string(),
        source_signature: Some("market-buy".to_string()),
    });
    let mut buy_observation = decoded_buy(Vec::new());
    buy_observation.mint = Some(mint.clone());
    buy_observation.sol_delta_lamports = Some(-10_000_000);
    buy_observation.token_delta_raw = Some(1_000_000);
    buy_observation.fee_lamports = Some(5_000);

    let buy_report = executor
        .execute_observed(&buy_order, &buy_observation, 0, settings)
        .expect("observed paper buy should execute");
    assert_eq!(buy_report.status, ExecutionStatus::PaperFilled);
    assert_eq!(buy_report.filled_token_amount_raw, Some(475_000));

    let mut positions = catarnith::position::PositionManager::default();
    positions.record_order_report(&buy_order, &buy_report);

    let sell_order = Order::Sell(SellOrder {
        id: "paper-sell".to_string(),
        timestamp_ms: 2_000,
        mint: mint.clone(),
        source_decision_id: "decision-sell".to_string(),
        source_signature: Some("market-sell".to_string()),
    });
    let mut sell_observation = buy_observation.clone();
    sell_observation.side = TradeSide::Sell;
    sell_observation.sol_delta_lamports = Some(20_000_000);
    sell_observation.token_delta_raw = Some(-1_000_000);

    let sell_report = executor
        .execute_observed(
            &sell_order,
            &sell_observation,
            positions.token_amount_for_mint(&mint),
            settings,
        )
        .expect("observed paper sell should execute");
    assert_eq!(sell_report.status, ExecutionStatus::PaperFilled);
    assert_eq!(sell_report.filled_lamports, Some(9_025_000));
    positions.record_order_report(&sell_order, &sell_report);

    let position = positions
        .position_for_mint(&mint)
        .expect("paper position should exist");
    assert_eq!(position.token_amount_raw, 0);
    assert_eq!(position.realized_lamports, 4_015_000);
}

#[test]
fn quote_executor_supports_independent_forced_exit() {
    let executor = PaperExecutor;
    let settings = PaperExecutionSettings {
        slippage_bps: 300,
        fee_lamports_floor: 5_000,
    };
    let order = Order::Sell(SellOrder {
        id: "forced-exit".to_string(),
        timestamp_ms: 6_000,
        mint: "mint".to_string(),
        source_decision_id: "max-hold".to_string(),
        source_signature: None,
    });
    let quote = MarketQuote {
        mint: "mint".to_string(),
        slot: 1,
        timestamp_ms: 6_000,
        observed_at_ms: 6_000,
        signature: "market-tick".to_string(),
        side: TradeSide::Sell,
        sol_lamports: 20_000_000,
        token_amount_raw: 1_000_000,
        trader: None,
    };

    let report = executor
        .execute_quote(&order, &quote, 500_000, settings)
        .expect("quote-backed forced exit should execute");

    assert_eq!(report.status, ExecutionStatus::PaperFilled);
    assert_eq!(report.filled_lamports, Some(9_700_000));
    assert_eq!(report.signature.as_deref(), Some("market-tick"));
}

#[test]
fn quote_executor_closes_dust_position_at_zero_proceeds() {
    let executor = PaperExecutor;
    let settings = PaperExecutionSettings {
        slippage_bps: 300,
        fee_lamports_floor: 5_000,
    };
    let mint = "dust-mint".to_string();
    let buy_order = Order::Buy(BuyOrder {
        id: "dust-buy".to_string(),
        timestamp_ms: 1_000,
        mint: mint.clone(),
        lamports: 13_025_001,
        source_decision_id: "entry".to_string(),
        source_signature: None,
    });
    let buy_report = catarnith::types::ExecutionReport {
        order_id: "dust-buy".to_string(),
        signature: None,
        quote_slot: Some(1),
        status: ExecutionStatus::PaperFilled,
        requested_lamports: 13_025_001,
        filled_lamports: Some(13_025_001),
        filled_token_amount_raw: Some(900_000),
        fee_lamports: Some(5_000),
        error: None,
        latency_ms: Some(0),
    };
    let mut positions = catarnith::position::PositionManager::default();
    positions.record_order_report(&buy_order, &buy_report);

    let sell_order = Order::Sell(SellOrder {
        id: "dust-sell".to_string(),
        timestamp_ms: 2_000,
        mint: mint.clone(),
        source_decision_id: "max-hold".to_string(),
        source_signature: None,
    });
    let quote = MarketQuote {
        mint: mint.clone(),
        slot: 2,
        timestamp_ms: 2_000,
        observed_at_ms: 2_000,
        signature: "dust-curve".to_string(),
        side: TradeSide::Sell,
        sol_lamports: 1,
        token_amount_raw: 900_000,
        trader: None,
    };

    let report = executor
        .execute_quote(&sell_order, &quote, 900_000, settings)
        .expect("dust liquidation should execute");
    assert_eq!(report.status, ExecutionStatus::PaperFilled);
    assert_eq!(report.filled_lamports, Some(0));

    positions.record_order_report(&sell_order, &report);
    let position = positions
        .position_for_mint(&mint)
        .expect("dust position should exist");
    assert_eq!(position.state, PositionState::Closed);
    assert_eq!(position.token_amount_raw, 0);
    assert_eq!(position.realized_lamports, -13_035_001);
}

#[test]
fn unpriced_force_close_books_full_loss_and_releases_position() {
    let executor = PaperExecutor;
    let settings = PaperExecutionSettings {
        slippage_bps: 300,
        fee_lamports_floor: 5_000,
    };
    let mint = "unpriced-mint".to_string();
    let buy_order = Order::Buy(BuyOrder {
        id: "unpriced-buy".to_string(),
        timestamp_ms: 1_000,
        mint: mint.clone(),
        lamports: 13_025_001,
        source_decision_id: "entry".to_string(),
        source_signature: None,
    });
    let buy_report = catarnith::types::ExecutionReport {
        order_id: buy_order.id().to_string(),
        signature: None,
        quote_slot: Some(1),
        status: ExecutionStatus::PaperFilled,
        requested_lamports: 13_025_001,
        filled_lamports: Some(13_025_001),
        filled_token_amount_raw: Some(900_000),
        fee_lamports: Some(17_000),
        error: None,
        latency_ms: Some(0),
    };
    let mut positions = catarnith::position::PositionManager::default();
    positions.record_order_report(&buy_order, &buy_report);

    let sell_order = Order::Sell(SellOrder {
        id: "unpriced-sell".to_string(),
        timestamp_ms: 9_000,
        mint: mint.clone(),
        source_decision_id: "max-hold-unpriced".to_string(),
        source_signature: None,
    });
    let report = executor.force_close_unpriced(&sell_order, 900_000, settings);
    assert_eq!(report.status, ExecutionStatus::PaperFilled);
    assert_eq!(report.filled_lamports, Some(0));
    assert_eq!(report.filled_token_amount_raw, Some(900_000));

    positions.record_order_report(&sell_order, &report);
    let position = positions
        .position_for_mint(&mint)
        .expect("unpriced position should remain journalable");
    assert_eq!(position.state, PositionState::Closed);
    assert_eq!(position.token_amount_raw, 0);
    assert_eq!(position.realized_lamports, -13_047_001);
    assert_eq!(positions.open_positions(), 0);
}

#[test]
fn quote_executor_rejects_trade_smaller_than_paper_inventory() {
    let executor = PaperExecutor;
    let settings = PaperExecutionSettings {
        slippage_bps: 300,
        fee_lamports_floor: 5_000,
    };
    let order = Order::Sell(SellOrder {
        id: "oversized-exit".to_string(),
        timestamp_ms: 6_000,
        mint: "mint".to_string(),
        source_decision_id: "max-hold".to_string(),
        source_signature: None,
    });
    let quote = MarketQuote {
        mint: "mint".to_string(),
        slot: 2,
        timestamp_ms: 6_000,
        observed_at_ms: 6_000,
        signature: "small-sell".to_string(),
        side: TradeSide::Sell,
        sol_lamports: 20_000_000,
        token_amount_raw: 499_999,
        trader: Some("trader".to_string()),
    };

    let report = executor
        .execute_quote(&order, &quote, 500_000, settings)
        .expect("undersized market observation should be rejected cleanly");

    assert_eq!(report.status, ExecutionStatus::PaperRejected);
    assert_eq!(
        report.error.as_deref(),
        Some("market_sell_observation_too_small")
    );
}

#[test]
fn position_manager_accounts_for_partial_take_profit() {
    let mint = "partial-mint".to_string();
    let buy = Order::Buy(BuyOrder {
        id: "partial-buy".to_string(),
        timestamp_ms: 1_000,
        mint: mint.clone(),
        lamports: 10_000_000,
        source_decision_id: "entry".to_string(),
        source_signature: None,
    });
    let buy_report = catarnith::types::ExecutionReport {
        order_id: "partial-buy".to_string(),
        signature: None,
        quote_slot: Some(10),
        status: ExecutionStatus::PaperFilled,
        requested_lamports: 10_000_000,
        filled_lamports: Some(10_000_000),
        filled_token_amount_raw: Some(10_000_000),
        fee_lamports: Some(1_000),
        error: None,
        latency_ms: Some(0),
    };
    let mut positions = catarnith::position::PositionManager::default();
    positions.record_order_report(&buy, &buy_report);

    let sell = Order::Sell(SellOrder {
        id: "partial-sell".to_string(),
        timestamp_ms: 2_000,
        mint: mint.clone(),
        source_decision_id: "take-profit".to_string(),
        source_signature: None,
    });
    let sell_report = catarnith::types::ExecutionReport {
        order_id: "partial-sell".to_string(),
        signature: None,
        quote_slot: Some(11),
        status: ExecutionStatus::PaperFilled,
        requested_lamports: 0,
        filled_lamports: Some(7_500_000),
        filled_token_amount_raw: Some(5_000_000),
        fee_lamports: Some(1_000),
        error: None,
        latency_ms: Some(0),
    };
    positions.record_order_report(&sell, &sell_report);

    let position = positions
        .position_for_mint(&mint)
        .expect("partial position should remain");
    assert_eq!(position.state, PositionState::PartiallyExited);
    assert_eq!(position.token_amount_raw, 5_000_000);
    assert_eq!(position.entry_lamports, 5_000_500);
    assert_eq!(position.realized_lamports, 2_498_500);
    assert!(position.has_taken_profit);
    assert_eq!(position.entry_quote_slot, Some(10));
}
