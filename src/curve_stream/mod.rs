use crate::curve::{decode_bonding_curve, BondingCurveState};
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

pub fn spawn_curve_watch(
    ws_url: String,
    commitment: String,
    mint: String,
    account: String,
    out: mpsc::Sender<BondingCurveState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(err) =
                account_subscribe_once(&ws_url, &commitment, &mint, &account, out.clone()).await
            {
                warn!("curve accountSubscribe ended mint={mint}: {err:?}; reconnecting");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}

async fn account_subscribe_once(
    ws_url: &str,
    commitment: &str,
    mint: &str,
    account: &str,
    out: mpsc::Sender<BondingCurveState>,
) -> Result<()> {
    let (mut ws, _) = connect_async(ws_url)
        .await
        .context("connect curve accountSubscribe websocket")?;
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "accountSubscribe",
        "params": [
            account,
            {
                "encoding": "base64",
                "commitment": commitment
            }
        ]
    });
    ws.send(Message::Text(request.to_string().into())).await?;
    info!("subscribed curve account mint={mint} account={account}");

    while let Some(message) = ws.next().await {
        let message = message?;
        if !message.is_text() {
            continue;
        }
        let value: Value = serde_json::from_str(&message.into_text()?)?;
        let Some(state) = parse_account_notification(mint, account, &value)? else {
            continue;
        };
        if out.send(state).await.is_err() {
            break;
        }
    }
    Ok(())
}

pub fn parse_account_notification(
    mint: &str,
    account: &str,
    value: &Value,
) -> Result<Option<BondingCurveState>> {
    if value.get("method").and_then(Value::as_str) != Some("accountNotification") {
        return Ok(None);
    }
    let slot = value
        .pointer("/params/result/context/slot")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let Some(encoded) = value
        .pointer("/params/result/value/data/0")
        .and_then(Value::as_str)
    else {
        return Ok(None);
    };
    let data = STANDARD
        .decode(encoded)
        .context("invalid curve account notification base64")?;
    Ok(Some(decode_bonding_curve(mint, account, slot, &data)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_bonding_curve_account_notification() {
        let mut data = vec![0u8; 115];
        data[..8].copy_from_slice(&[23, 183, 248, 55, 96, 216, 172, 96]);
        data[8..16].copy_from_slice(&1_000u64.to_le_bytes());
        data[16..24].copy_from_slice(&2_000u64.to_le_bytes());
        data[24..32].copy_from_slice(&900u64.to_le_bytes());
        data[32..40].copy_from_slice(&1_500u64.to_le_bytes());
        data[40..48].copy_from_slice(&5_000u64.to_le_bytes());
        data[81] = 1;
        let value = json!({
            "method": "accountNotification",
            "params": {
                "result": {
                    "context": {"slot": 77},
                    "value": {"data": [STANDARD.encode(data), "base64"]}
                }
            }
        });

        let state = parse_account_notification("mint", "curve", &value)
            .expect("notification should decode")
            .expect("notification should contain state");

        assert_eq!(state.slot, 77);
        assert_eq!(state.virtual_quote_reserves, 2_000);
        assert_eq!(state.is_mayhem_mode, Some(true));
        assert!(state.observed_at_ms > 0);
    }
}
