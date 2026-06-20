mod common;

use catarnith::{
    curve::{buy_quote_from_state, decode_bonding_curve},
    decoder::decode_live_transaction,
    discovery::{DiscoveryRegistry, DiscoverySignal},
    survival::{parse_pulse_mint_line, summarize_survival_events, PulseMint, SurvivalSettings},
    types::TradeSide,
};
use common::lab_event;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

#[test]
fn survival_lab_marks_fast_loss_and_stale_skips() {
    let settings = SurvivalSettings {
        observation_window_ms: 5_000,
        entry_deadline_ms: 3_000,
        max_hold_ms: 5_000,
        min_successful_buys_in_window: 1,
        max_failed_events_in_window: 2,
        min_buy_spend_lamports_in_window: 10_000_000,
    };
    let mut pulse_mints = BTreeMap::new();
    pulse_mints.insert(
        "fast_loss".to_string(),
        PulseMint {
            mint: "fast_loss".to_string(),
            seen_ts_ms: Some(1_000),
            source: "test".to_string(),
        },
    );
    pulse_mints.insert(
        "stale".to_string(),
        PulseMint {
            mint: "stale".to_string(),
            seen_ts_ms: Some(1_000),
            source: "test".to_string(),
        },
    );

    let events = vec![
        lab_event("fast_loss", TradeSide::Buy, 1_500, true, Some(-13_025_001)),
        lab_event("fast_loss", TradeSide::Sell, 2_500, true, Some(1_000_000)),
        lab_event("stale", TradeSide::Buy, 6_000, true, Some(-13_025_001)),
    ];

    let summary = summarize_survival_events("wallet", 3, events, pulse_mints, settings);
    let fast_loss = summary
        .reports
        .iter()
        .find(|report| report.mint == "fast_loss")
        .expect("fast loss mint should be reported");
    let stale = summary
        .reports
        .iter()
        .find(|report| report.mint == "stale")
        .expect("stale mint should be reported");

    assert!(fast_loss.paper_enter);
    assert!(fast_loss.instant_loss);
    assert!(fast_loss.paper_profit_sol < 0.0);
    assert!(fast_loss.total_profit_sol < 0.0);
    assert!(!stale.paper_enter);
    assert!(stale
        .reason_codes
        .iter()
        .any(|reason| reason == "stale_entry"));
}

#[test]
fn live_decoder_extracts_reference_wallet_market_fill() {
    let Some(path) = reference_transactions_path() else {
        eprintln!("skipping transaction fixture test: wallet_dump/transactions.jsonl not found");
        return;
    };
    let line = BufReader::new(File::open(path).expect("transaction dump should open"))
        .lines()
        .next()
        .expect("transaction dump should have a row")
        .expect("first transaction row should be readable");
    let row: serde_json::Value =
        serde_json::from_str(&line).expect("first transaction row should be JSON");
    let tx = row
        .get("transaction")
        .expect("transaction row should contain transaction");
    let logs = tx
        .pointer("/meta/logMessages")
        .and_then(serde_json::Value::as_array)
        .expect("transaction should contain logs")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect();
    let decoded = decode_live_transaction(
        row.get("signature")
            .and_then(serde_json::Value::as_str)
            .expect("transaction should contain signature")
            .to_string(),
        tx.get("slot")
            .and_then(serde_json::Value::as_u64)
            .expect("transaction should contain slot"),
        logs,
        Some(tx),
        Some("8UbptT8bqtXVKvHkMwEkWqrRLQ5mTs2whQbY5Twfwhfi"),
    );

    assert_eq!(decoded.side, TradeSide::Buy);
    assert!(decoded
        .mint
        .as_deref()
        .is_some_and(|mint| mint.ends_with("pump")));
    assert!(decoded.sol_delta_lamports.is_some_and(|delta| delta < 0));
    assert!(decoded.token_delta_raw.is_some_and(|delta| delta > 0));
}

#[test]
fn pulse_discovery_registry_enforces_one_second_entry_window() {
    let pulse = parse_pulse_mint_line(
        r#"{"mint":"So111111111111111111111111111111111111pump","seen_ts_ms":1000,"source":"axiom_pulse_mayhem"}"#,
    )
    .expect("Pulse row should parse")
    .expect("Pulse row should contain a mint");
    let mut registry = DiscoveryRegistry::default();
    assert!(registry.register(DiscoverySignal::from(pulse)));
    assert!(registry.is_entry_fresh("So111111111111111111111111111111111111pump", 1_999, 1_000));
    assert!(!registry.is_entry_fresh("So111111111111111111111111111111111111pump", 2_001, 1_000));
}

#[test]
fn bonding_curve_decoder_reads_official_mayhem_flag() {
    let mut data = vec![0u8; 115];
    data[..8].copy_from_slice(&[23, 183, 248, 55, 96, 216, 172, 96]);
    data[8..16].copy_from_slice(&1_000_000u64.to_le_bytes());
    data[16..24].copy_from_slice(&2_000_000u64.to_le_bytes());
    data[24..32].copy_from_slice(&900_000u64.to_le_bytes());
    data[32..40].copy_from_slice(&1_500_000u64.to_le_bytes());
    data[40..48].copy_from_slice(&5_000_000u64.to_le_bytes());
    data[81] = 1;

    let state = decode_bonding_curve("mint", "curve", 42, &data)
        .expect("official BondingCurve layout should decode");

    assert_eq!(state.virtual_token_reserves, 1_000_000);
    assert_eq!(state.virtual_quote_reserves, 2_000_000);
    assert_eq!(state.is_mayhem_mode, Some(true));
    assert!(!state.complete);

    let quote = buy_quote_from_state(&state, 1_000_000)
        .expect("tradeable curve should produce a buy quote");
    assert_eq!(quote.token_amount_raw, 333_333);
    assert_eq!(quote.side, TradeSide::Buy);
}

fn reference_transactions_path() -> Option<PathBuf> {
    [
        PathBuf::from("../wallet_dump/transactions.jsonl"),
        PathBuf::from("wallet_dump/transactions.jsonl"),
    ]
    .into_iter()
    .find(|path| path.exists())
}
