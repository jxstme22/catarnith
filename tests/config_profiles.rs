use catarnith::{config::Config, types::Mode};
use std::path::Path;

fn load_example() -> Config {
    let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
    Config::load_raw(config_path.as_path())
        .unwrap_or_else(|err| panic!("config.example.toml should parse: {err}"))
}

#[test]
fn config_example_is_the_single_paper_safe_template() {
    let cfg = load_example();

    assert_eq!(cfg.mode, Mode::Paper);
    assert!(!cfg.enable_live_trading);
    assert!(cfg.require_manual_live_unlock);
    assert_eq!(cfg.journal_dir, "journals/bot");
    assert_eq!(cfg.sqlite_path, "journals/bot/mayhem.sqlite");
    assert!(cfg.paper_report_path.is_empty());
    assert!(cfg.horizon_report_path.is_empty());
    assert_eq!(cfg.backfill_limit, 0);
    assert!(cfg.max_total_lamports_per_mint >= cfg.base_buy_lamports);
    assert!(cfg.max_total_open_lamports >= cfg.base_buy_lamports);
}

#[test]
fn config_example_can_be_armed_for_live_after_required_local_values() {
    let mut cfg = load_example();
    cfg.mode = Mode::Live;
    cfg.helius_api_key = "test-key".to_string();
    cfg.wallet_keypair_path = "/tmp/catarnith-live-test.json".to_string();
    cfg.enable_live_trading = true;
    cfg.require_manual_live_unlock = false;
    cfg.live_single_lifecycle = true;
    cfg.max_open_positions = 1;
    cfg.max_buys_per_mint = 1;
    cfg.max_total_lamports_per_mint = cfg.base_buy_lamports;
    cfg.max_total_open_lamports = cfg.base_buy_lamports;

    cfg.validate_for_bot()
        .expect("single config should validate once explicitly armed for live");
}

#[test]
fn live_validation_rejects_risk_caps_below_buy_size() {
    let mut cfg = load_example();
    cfg.mode = Mode::Live;
    cfg.helius_api_key = "test-key".to_string();
    cfg.wallet_keypair_path = "/tmp/catarnith-live-test.json".to_string();
    cfg.enable_live_trading = true;
    cfg.require_manual_live_unlock = false;
    cfg.max_total_lamports_per_mint = cfg.base_buy_lamports - 1;
    cfg.max_total_open_lamports = cfg.base_buy_lamports;

    let err = cfg
        .validate_for_bot()
        .expect_err("live config must reject per-mint caps below buy size");
    assert!(err
        .to_string()
        .contains("max_total_lamports_per_mint >= base_buy_lamports"));
}
