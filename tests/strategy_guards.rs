mod common;

use catarnith::{
    classifier::{classify_token, ClassifierConfig},
    config::{Config, Market},
    mayhem::{
        apply_mayhem_evidence, parse_mayhem_metadata_json, MayhemEvidenceClient,
        MayhemEvidenceConfig,
    },
    risk::{RiskEngine, RiskLimits, RiskSnapshot},
    strategy::{BurstStrategy, StrategyContext, StrategySettings},
    types::{Action, Decision, Mode},
};
use common::decoded_buy;

#[test]
fn risk_engine_vetoes_buys_over_per_mint_cap() {
    let limits = RiskLimits {
        max_buy_lamports: 13_025_001,
        max_buys_per_mint: 5,
        max_total_lamports_per_mint: 100_000_000,
        max_total_open_lamports: 300_000_000,
        max_open_positions: 3,
        max_daily_loss_lamports: 500_000_000,
        max_failed_txs_per_minute: 30,
        max_failed_fee_burn_lamports_per_hour: 20_000_000,
        max_slippage_bps: 1_500,
    };
    let risk = RiskEngine::new(limits);
    let decision = Decision {
        id: "decision-test".to_string(),
        timestamp_ms: 0,
        source_signature: None,
        mint: Some("mint".to_string()),
        action: Action::Buy,
        mode: Mode::Paper,
        reason_codes: vec!["test".to_string()],
        requested_lamports: Some(13_025_001),
        risk_approved: false,
        risk_veto_reason: None,
    };
    let snapshot = RiskSnapshot {
        exposure_for_mint: 99_000_000,
        ..RiskSnapshot::default()
    };
    let evaluation = risk.evaluate(&decision, &snapshot);
    assert!(!evaluation.approved);
    assert_eq!(
        evaluation.reason.as_deref(),
        Some("max_total_lamports_per_mint")
    );
}

#[test]
fn risk_engine_does_not_count_ignore_as_veto() {
    let risk = RiskEngine::new(RiskLimits::from(&Config::default()));
    let decision = Decision {
        id: "decision-ignore".to_string(),
        timestamp_ms: 0,
        source_signature: None,
        mint: Some("mint".to_string()),
        action: Action::Ignore,
        mode: Mode::Paper,
        reason_codes: vec!["mayhem_evidence_required".to_string()],
        requested_lamports: None,
        risk_approved: false,
        risk_veto_reason: None,
    };

    let decision = risk.apply(decision, &RiskSnapshot::default());

    assert!(!decision.risk_approved);
    assert!(decision.risk_veto_reason.is_none());
}

#[test]
fn strict_strategy_rejects_indirect_mayhem_like_mints() {
    let cfg = Config::default();
    let decoded = decoded_buy(vec![
        cfg.axiom_route_program.clone(),
        cfg.pumpfun_program.clone(),
        cfg.token_2022_program.clone(),
    ]);
    let classification = classify_token(&decoded, &ClassifierConfig::default());

    assert!(classification.is_mayhem_candidate);
    assert!(!classification.has_verified_mayhem_evidence);

    let mut strategy = BurstStrategy::default();
    let decision = strategy.decide(
        &StrategySettings::from(&cfg),
        &decoded,
        &classification,
        StrategyContext {
            open_positions: 0,
            has_position_for_mint: false,
            buys_for_mint: 0,
            has_discovery_signal: false,
            has_fresh_mint_discovery: false,
            discovery_seen_ts_ms: None,
            observed_buy_lamports: Some(13_025_001),
            observed_buys_for_mint: 1,
            observed_sells_for_mint: 0,
        },
    );

    assert_eq!(decision.action, Action::Ignore);
    assert_eq!(decision.reason_codes, vec!["mayhem_evidence_required"]);
}

#[test]
fn strict_strategy_allows_direct_mayhem_mints() {
    let cfg = Config::default();
    let decoded = decoded_buy(vec![
        cfg.axiom_route_program.clone(),
        cfg.pumpfun_program.clone(),
        cfg.mayhem_program.clone(),
    ]);
    let classification = classify_token(&decoded, &ClassifierConfig::default());

    assert!(classification.has_verified_mayhem_evidence);

    let mut strategy = BurstStrategy::default();
    let decision = strategy.decide(
        &StrategySettings::from(&cfg),
        &decoded,
        &classification,
        StrategyContext {
            open_positions: 0,
            has_position_for_mint: false,
            buys_for_mint: 0,
            has_discovery_signal: false,
            has_fresh_mint_discovery: false,
            discovery_seen_ts_ms: None,
            observed_buy_lamports: Some(13_025_001),
            observed_buys_for_mint: 1,
            observed_sells_for_mint: 0,
        },
    );

    assert_eq!(decision.action, Action::Buy);
    assert!(decision
        .reason_codes
        .iter()
        .any(|reason| reason == "verified_mayhem_evidence"));
    assert!(decision
        .reason_codes
        .iter()
        .any(|reason| reason == "confirmed_axiom_pump_route"));
}

#[test]
fn strict_strategy_rejects_direct_mayhem_without_route_confirmation() {
    let cfg = Config::default();
    let decoded = decoded_buy(vec![
        cfg.pumpfun_program.clone(),
        cfg.mayhem_program.clone(),
    ]);
    let classification = classify_token(&decoded, &ClassifierConfig::default());

    assert!(classification.has_verified_mayhem_evidence);
    assert!(!classification.has_confirmed_execution_route);

    let mut strategy = BurstStrategy::default();
    let decision = strategy.decide(
        &StrategySettings::from(&cfg),
        &decoded,
        &classification,
        StrategyContext {
            open_positions: 0,
            has_position_for_mint: false,
            buys_for_mint: 0,
            has_discovery_signal: false,
            has_fresh_mint_discovery: false,
            discovery_seen_ts_ms: None,
            observed_buy_lamports: Some(13_025_001),
            observed_buys_for_mint: 1,
            observed_sells_for_mint: 0,
        },
    );

    assert_eq!(decision.action, Action::Ignore);
    assert_eq!(decision.reason_codes, vec!["route_confirmation_required"]);
}

#[test]
fn live_freshness_strategy_requires_create_backed_discovery() {
    let cfg = Config {
        require_route_confirmation: false,
        require_discovery_signal: true,
        require_fresh_mint_creation: true,
        ..Config::default()
    };
    let decoded = decoded_buy(vec![
        cfg.pumpfun_program.clone(),
        cfg.mayhem_program.clone(),
    ]);
    let classification = classify_token(&decoded, &ClassifierConfig::default());
    let settings = StrategySettings::from(&cfg);

    let mut strategy = BurstStrategy::default();
    let stale_source = strategy.decide(
        &settings,
        &decoded,
        &classification,
        StrategyContext {
            open_positions: 0,
            has_position_for_mint: false,
            buys_for_mint: 0,
            has_discovery_signal: true,
            has_fresh_mint_discovery: false,
            discovery_seen_ts_ms: decoded.timestamp_ms,
            observed_buy_lamports: Some(100_000_000),
            observed_buys_for_mint: 1,
            observed_sells_for_mint: 0,
        },
    );
    assert_eq!(stale_source.action, Action::Ignore);
    assert_eq!(
        stale_source.reason_codes,
        vec!["fresh_mint_creation_required"]
    );

    let mut strategy = BurstStrategy::default();
    let later_same_mint_buy = strategy.decide(
        &settings,
        &decoded,
        &classification,
        StrategyContext {
            open_positions: 0,
            has_position_for_mint: false,
            buys_for_mint: 0,
            has_discovery_signal: true,
            has_fresh_mint_discovery: true,
            discovery_seen_ts_ms: decoded.timestamp_ms,
            observed_buy_lamports: Some(100_000_000),
            observed_buys_for_mint: 1,
            observed_sells_for_mint: 0,
        },
    );
    assert_eq!(later_same_mint_buy.action, Action::Ignore);
    assert_eq!(
        later_same_mint_buy.reason_codes,
        vec!["fresh_entry_tx_creation_required"]
    );

    let mut create_decoded = decoded.clone();
    create_decoded
        .instruction_names
        .insert(0, "CreateV2".to_string());
    let create_classification = classify_token(&create_decoded, &ClassifierConfig::default());

    let mut strategy = BurstStrategy::default();
    let create_source = strategy.decide(
        &settings,
        &create_decoded,
        &create_classification,
        StrategyContext {
            open_positions: 0,
            has_position_for_mint: false,
            buys_for_mint: 0,
            has_discovery_signal: true,
            has_fresh_mint_discovery: true,
            discovery_seen_ts_ms: create_decoded.timestamp_ms,
            observed_buy_lamports: Some(100_000_000),
            observed_buys_for_mint: 1,
            observed_sells_for_mint: 0,
        },
    );
    assert_eq!(create_source.action, Action::Buy);
}

#[test]
fn non_mayhem_market_requires_fresh_create_entry() {
    let cfg = Config {
        market: Market::NonMayhemOnly,
        require_route_confirmation: false,
        ..Config::default()
    };
    let decoded = decoded_buy(vec![cfg.pumpfun_program.clone()]);
    let classification = classify_token(&decoded, &ClassifierConfig::default());
    let settings = StrategySettings::from(&cfg);
    let context = StrategyContext {
        open_positions: 0,
        has_position_for_mint: false,
        buys_for_mint: 0,
        has_discovery_signal: false,
        has_fresh_mint_discovery: false,
        discovery_seen_ts_ms: None,
        observed_buy_lamports: Some(100_000_000),
        observed_buys_for_mint: 1,
        observed_sells_for_mint: 0,
    };

    let mut strategy = BurstStrategy::default();
    let stale_buy = strategy.decide(&settings, &decoded, &classification, context);
    assert_eq!(stale_buy.action, Action::Ignore);
    assert_eq!(
        stale_buy.reason_codes,
        vec!["fresh_entry_tx_creation_required"]
    );

    let mut create_decoded = decoded.clone();
    create_decoded
        .instruction_names
        .insert(0, "CreateV2".to_string());
    let create_classification = classify_token(&create_decoded, &ClassifierConfig::default());
    let mut strategy = BurstStrategy::default();
    let fresh_buy = strategy.decide(&settings, &create_decoded, &create_classification, context);
    assert_eq!(fresh_buy.action, Action::Buy);

    let mut direct_mayhem_create = create_decoded.clone();
    direct_mayhem_create
        .program_ids
        .push(cfg.mayhem_program.clone());
    let direct_mayhem_classification =
        classify_token(&direct_mayhem_create, &ClassifierConfig::default());
    let mut strategy = BurstStrategy::default();
    let direct_mayhem = strategy.decide(
        &settings,
        &direct_mayhem_create,
        &direct_mayhem_classification,
        context,
    );
    assert_eq!(direct_mayhem.action, Action::Ignore);
    assert_eq!(direct_mayhem.reason_codes, vec!["non_mayhem_market_only"]);

    let mut indirect_mayhem_create = create_decoded.clone();
    indirect_mayhem_create
        .program_ids
        .push(cfg.token_2022_program.clone());
    let indirect_mayhem_classification =
        classify_token(&indirect_mayhem_create, &ClassifierConfig::default());
    assert!(indirect_mayhem_classification.is_mayhem_candidate);
    let mut strategy = BurstStrategy::default();
    let indirect_mayhem = strategy.decide(
        &settings,
        &indirect_mayhem_create,
        &indirect_mayhem_classification,
        context,
    );
    assert_eq!(indirect_mayhem.action, Action::Ignore);
    assert_eq!(indirect_mayhem.reason_codes, vec!["non_mayhem_market_only"]);
}

#[test]
fn agent_entry_filters_reject_bad_size_and_late_sequence() {
    let cfg = Config {
        require_route_confirmation: false,
        min_observed_buy_lamports: 30_000_000,
        max_observed_buy_lamports: Some(500_000_000),
        max_observed_buys_before_entry: Some(1),
        max_observed_sells_before_entry: Some(0),
        ..Config::default()
    };
    let decoded = decoded_buy(vec![
        cfg.pumpfun_program.clone(),
        cfg.mayhem_program.clone(),
    ]);
    let classification = classify_token(&decoded, &ClassifierConfig::default());
    let settings = StrategySettings::from(&cfg);

    let mut strategy = BurstStrategy::default();
    let small = strategy.decide(
        &settings,
        &decoded,
        &classification,
        StrategyContext {
            open_positions: 0,
            has_position_for_mint: false,
            buys_for_mint: 0,
            has_discovery_signal: false,
            has_fresh_mint_discovery: false,
            discovery_seen_ts_ms: None,
            observed_buy_lamports: Some(13_025_001),
            observed_buys_for_mint: 1,
            observed_sells_for_mint: 0,
        },
    );
    assert_eq!(small.action, Action::Ignore);
    assert!(small.reason_codes[0].starts_with("observed_buy_below_min"));

    let mut strategy = BurstStrategy::default();
    let late = strategy.decide(
        &settings,
        &decoded,
        &classification,
        StrategyContext {
            open_positions: 0,
            has_position_for_mint: false,
            buys_for_mint: 0,
            has_discovery_signal: false,
            has_fresh_mint_discovery: false,
            discovery_seen_ts_ms: None,
            observed_buy_lamports: Some(100_000_000),
            observed_buys_for_mint: 2,
            observed_sells_for_mint: 0,
        },
    );
    assert_eq!(late.action, Action::Ignore);
    assert!(late.reason_codes[0].starts_with("agent_buy_sequence_late"));

    let mut strategy = BurstStrategy::default();
    let sold = strategy.decide(
        &settings,
        &decoded,
        &classification,
        StrategyContext {
            open_positions: 0,
            has_position_for_mint: false,
            buys_for_mint: 0,
            has_discovery_signal: false,
            has_fresh_mint_discovery: false,
            discovery_seen_ts_ms: None,
            observed_buy_lamports: Some(100_000_000),
            observed_buys_for_mint: 1,
            observed_sells_for_mint: 1,
        },
    );
    assert_eq!(sold.action, Action::Ignore);
    assert!(sold.reason_codes[0].starts_with("agent_sold_before_entry"));
}

#[tokio::test]
async fn allowlist_confirms_mayhem_mint_source() {
    let mut cfg = Config::default();
    let mint = "So111111111111111111111111111111111111pump";
    let path = std::env::temp_dir().join(format!(
        "mayhem-mints-{}-{}.txt",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    std::fs::write(&path, format!("{mint}\n")).expect("allowlist should be writable");
    cfg.mayhem_mint_allowlist_path = path.to_string_lossy().to_string();

    let decoded = decoded_buy(vec![
        cfg.axiom_route_program.clone(),
        cfg.pumpfun_program.clone(),
        cfg.token_2022_program.clone(),
    ]);
    let classification = classify_token(&decoded, &ClassifierConfig::default());
    let client = MayhemEvidenceClient::new(MayhemEvidenceConfig::from(&cfg))
        .expect("allowlist-backed evidence client should build");
    let evidence = client.check_mint(mint, &decoded, &classification).await;
    let classification = apply_mayhem_evidence(classification, &evidence);

    assert!(evidence.is_mayhem);
    assert!(classification.has_verified_mayhem_evidence);

    let _ = std::fs::remove_file(path);
}

#[test]
fn metadata_parser_confirms_mayhem_mints_only_on_explicit_flags() {
    let confirmed = parse_mayhem_metadata_json(
        "mint",
        &serde_json::json!({
            "data": {
                "isMayhem": true
            }
        }),
    );
    assert!(confirmed.is_mayhem);
    assert!(confirmed.confidence >= 0.9);

    let unconfirmed = parse_mayhem_metadata_json(
        "mint",
        &serde_json::json!({
            "data": {
                "isMayhem": false,
                "mode": "standard"
            }
        }),
    );
    assert!(!unconfirmed.is_mayhem);
}
