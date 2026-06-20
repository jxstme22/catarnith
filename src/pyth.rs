//! Pyth Network price feed client (off-chain Hermes API).
//!
//! Used for SOL/USD so catarnith can show mcap and position
//! values in USD without relying on CoinGecko.

use anyhow::{Context, Result};
use serde_json::Value;
use std::time::Duration;

const PYTH_HERMES_URL: &str = "https://hermes.pyth.network/api/latest_price_feeds";
const DEFAULT_SOL_USD_FEED_ID: &str =
    "0xef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

/// Fetch SOL/USD from Pyth Hermes. Free, no API key required
/// until the Pyth Core upgrade on 2026-07-31.
pub async fn fetch_sol_price_usd() -> Result<f64> {
    let feed_id = std::env::var("PYTH_SOL_USD_FEED_ID")
        .unwrap_or_else(|_| DEFAULT_SOL_USD_FEED_ID.to_string());
    let url = format!("{}?ids[]={}", PYTH_HERMES_URL, feed_id);

    let client = reqwest::Client::builder()
        .timeout(DEFAULT_TIMEOUT)
        .build()
        .context("build reqwest client for Pyth")?;

    let resp = client
        .get(&url)
        .send()
        .await
        .context("Pyth Hermes request failed")?;

    if !resp.status().is_success() {
        anyhow::bail!("Pyth Hermes returned {}", resp.status());
    }

    let json: Value = resp.json().await.context("parse Pyth Hermes JSON")?;
    let first = json
        .as_array()
        .and_then(|a| a.first())
        .context("Pyth Hermes response empty")?;

    let price_str = first
        .pointer("/price/price")
        .and_then(Value::as_str)
        .context("Pyth price.price missing")?;
    let price: i128 = price_str
        .parse()
        .with_context(|| format!("parse Pyth price string: {price_str}"))?;

    let expo = first
        .pointer("/price/expo")
        .and_then(Value::as_i64)
        .context("Pyth price.expo missing")?;

    let value = price as f64 * 10f64.powi(expo as i32);
    if value <= 0.0 {
        anyhow::bail!("Pyth SOL/USD price non-positive: {}", value);
    }
    Ok(value)
}
