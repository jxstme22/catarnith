use catarnith::{
    survival::LabTradeEvent,
    types::{DecodedTx, TradeSide},
};

#[allow(dead_code)]
pub fn decoded_buy(program_ids: Vec<String>) -> DecodedTx {
    DecodedTx {
        signature: "test-signature".to_string(),
        slot: 1,
        timestamp_ms: Some(1_700_000_000_000),
        ok: true,
        side: TradeSide::Buy,
        instruction_names: vec!["Buy".to_string()],
        program_ids,
        account_keys: Vec::new(),
        mint: Some("So111111111111111111111111111111111111pump".to_string()),
        signer: None,
        sol_delta_lamports: Some(-13_025_001),
        token_delta_raw: Some(1),
        fee_lamports: Some(5_000),
        logs: Vec::new(),
        err: None,
    }
}

#[allow(dead_code)]
pub fn lab_event(
    mint: &str,
    side: TradeSide,
    timestamp_ms: i64,
    ok: bool,
    sol_delta_lamports: Option<i64>,
) -> LabTradeEvent {
    LabTradeEvent {
        signature: format!("sig-{mint}-{timestamp_ms}"),
        slot: timestamp_ms as u64,
        timestamp_ms: Some(timestamp_ms),
        ok,
        side,
        mint: Some(mint.to_string()),
        token_delta_raw: None,
        token_decimals: Some(6),
        sol_delta_lamports,
        fee_lamports: 5_000,
        compute_units_consumed: None,
        compute_unit_limit: None,
        compute_unit_price_micro_lamports: None,
        instruction_names: Vec::new(),
        program_ids: Vec::new(),
        error: None,
    }
}
