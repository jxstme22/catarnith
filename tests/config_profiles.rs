use catarnith::{
    config::{Config, Market},
    types::Mode,
};
use std::{fs, path::Path};

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
    assert_eq!(cfg.sqlite_path, "journals/bot/catarnith.sqlite");
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
        .contains("max_total_sol_per_mint >= base_buy_sol"));
}

#[test]
fn config_supports_market_and_legacy_pair_scope_values() {
    let from_market: Config = toml::from_str(r#"market = "non_mayhem_only""#)
        .expect("market should parse from the current config key");
    assert_eq!(from_market.market, Market::NonMayhemOnly);

    let from_legacy: Config = toml::from_str(r#"pair_scope = "all_pumpfun""#)
        .expect("legacy pair_scope should still parse");
    assert_eq!(from_legacy.market, Market::AllPumpfun);
}

#[test]
fn config_supports_sol_sized_trade_fields() {
    let path = std::env::temp_dir().join(format!(
        "catarnith-config-sol-aliases-{}.toml",
        std::process::id()
    ));
    fs::write(
        &path,
        r#"
base_buy_sol = 0.02
max_total_sol_per_mint = 0.08
max_total_open_sol = 0.21
max_daily_loss_sol = 0.34
copy_trade_max_buy_sol = 0.015
copy_trade_min_source_buy_sol = 0.001

[live]
max_balance_sol = 0.05
jito_tip_sol = 0.0001
"#,
    )
    .expect("write temp SOL config");

    let cfg = Config::load_raw(&path).expect("SOL aliases should parse");
    let _ = fs::remove_file(path);

    assert_eq!(cfg.base_buy_lamports, 20_000_000);
    assert_eq!(cfg.max_total_lamports_per_mint, 80_000_000);
    assert_eq!(cfg.max_total_open_lamports, 210_000_000);
    assert_eq!(cfg.max_daily_loss_lamports, 340_000_000);
    assert_eq!(cfg.copy_trade_max_buy_lamports, 15_000_000);
    assert_eq!(cfg.copy_trade_min_source_buy_lamports, 1_000_000);
    assert_eq!(cfg.live.max_balance_lamports, 50_000_000);
    assert_eq!(cfg.live.jito_tip_lamports, 100_000);
}

#[test]
fn blank_optional_wallets_are_normalized_away() {
    let path = std::env::temp_dir().join(format!(
        "catarnith-config-normalize-{}.toml",
        std::process::id()
    ));
    fs::write(
        &path,
        r#"
target_wallet = ""
watched_wallets = ["", "WatchWallet1111111111111111111111111111111"]
"#,
    )
    .expect("write temp config");

    let cfg = Config::load_raw(&path).expect("blank optional wallets should parse");
    let _ = fs::remove_file(path);

    assert!(cfg.target_wallet.is_none());
    assert_eq!(
        cfg.watched_wallets,
        vec!["WatchWallet1111111111111111111111111111111"]
    );
}
