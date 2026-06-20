use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

fn current_time_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    pub source: String,
    pub signature: String,
    pub slot: u64,
    #[serde(default)]
    pub received_at_ms: i64,
    pub logs: Vec<String>,
    pub raw: Value,
}

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub ws_url: String,
    pub rpc_url: String,
    pub commitment: String,
    pub account_include: Vec<String>,
    pub watched_wallets: Vec<String>,
    pub logs_mentions: Vec<String>,
    pub enable_transaction_subscribe: bool,
    pub enable_logs_fallback: bool,
    pub backfill_limit: usize,
}

pub fn spawn_streams(config: StreamConfig) -> mpsc::Receiver<StreamEvent> {
    let (tx, rx) = mpsc::channel(4096);

    if config.backfill_limit > 0 {
        let backfill = config.clone();
        let backfill_tx = tx.clone();
        tokio::spawn(async move {
            if let Err(err) = backfill_recent(&backfill, backfill_tx).await {
                warn!("startup backfill failed: {err:#}");
            }
        });
    }

    let logs_started = Arc::new(AtomicBool::new(false));
    if config.enable_logs_fallback || !config.enable_transaction_subscribe {
        spawn_logs_streams(&config, &tx, &logs_started);
    }

    if config.enable_transaction_subscribe {
        let primary = config.clone();
        let primary_tx = tx.clone();
        let primary_logs_started = logs_started.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = transaction_subscribe_once(&primary, primary_tx.clone()).await {
                    if transaction_subscribe_unavailable(&err) {
                        warn!(
                            "transactionSubscribe is unavailable for this RPC plan; activating logsSubscribe fallback"
                        );
                        spawn_logs_streams(&primary, &primary_tx, &primary_logs_started);
                        break;
                    }
                    warn!("transactionSubscribe ended: {err:?}; reconnecting");
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        });
    }

    rx
}

fn spawn_logs_streams(
    config: &StreamConfig,
    tx: &mpsc::Sender<StreamEvent>,
    started: &Arc<AtomicBool>,
) {
    if started.swap(true, Ordering::AcqRel) {
        return;
    }
    let mentions = config
        .logs_mentions
        .iter()
        .chain(config.watched_wallets.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    if mentions.is_empty() {
        warn!("logsSubscribe fallback requested, but no wallet mentions are configured");
        return;
    }
    for mention in mentions {
        let logs_tx = tx.clone();
        let ws_url = config.ws_url.clone();
        let commitment = config.commitment.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) =
                    logs_subscribe_once(&ws_url, &commitment, &mention, logs_tx.clone()).await
                {
                    warn!("logsSubscribe ended mention={mention}: {err:?}; reconnecting");
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        });
    }
}

fn transaction_subscribe_unavailable(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}").to_ascii_lowercase();
    text.contains("transactionsubscribe")
        && (text.contains("not available")
            || text.contains("unsupported")
            || text.contains("method not found")
            || text.contains("\"code\":-32600")
            || text.contains("\"code\": -32600")
            || text.contains("\"code\":-32601")
            || text.contains("\"code\": -32601"))
}

async fn backfill_recent(config: &StreamConfig, out: mpsc::Sender<StreamEvent>) -> Result<()> {
    let client = reqwest::Client::new();
    let rpc_commitment = if config.commitment == "processed" {
        "confirmed"
    } else {
        config.commitment.as_str()
    };
    let mentions = config
        .account_include
        .iter()
        .chain(config.watched_wallets.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut signatures = BTreeMap::<String, (u64, String)>::new();

    for mention in mentions {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                mention,
                {
                    "limit": config.backfill_limit.min(1_000),
                    "commitment": rpc_commitment
                }
            ]
        });
        let value = rpc_request(&client, &config.rpc_url, body).await?;
        if let Some(items) = value.get("result").and_then(Value::as_array) {
            for item in items {
                if item.get("err").is_some_and(|err| !err.is_null()) {
                    continue;
                }
                let Some(signature) = item.get("signature").and_then(Value::as_str) else {
                    continue;
                };
                let slot = item.get("slot").and_then(Value::as_u64).unwrap_or_default();
                signatures
                    .entry(signature.to_string())
                    .or_insert((slot, mention.clone()));
            }
        }
    }

    let mut ordered = signatures.into_iter().collect::<Vec<_>>();
    ordered.sort_by_key(|(_, (slot, _))| *slot);
    info!("startup backfill signatures={}", ordered.len());

    for (signature, (known_slot, mention)) in ordered {
        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": rpc_commitment,
                    "maxSupportedTransactionVersion": 0
                }
            ]
        });
        let value = rpc_request(&client, &config.rpc_url, body).await?;
        let Some(tx) = value.get("result").filter(|result| !result.is_null()) else {
            continue;
        };
        let logs = tx
            .pointer("/meta/logMessages")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let event = StreamEvent {
            source: format!("backfill:{mention}"),
            signature: signature.clone(),
            slot: tx.get("slot").and_then(Value::as_u64).unwrap_or(known_slot),
            received_at_ms: current_time_ms(),
            logs,
            raw: tx.clone(),
        };
        if out.send(event).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn rpc_request(client: &reqwest::Client, url: &str, body: Value) -> Result<Value> {
    let mut last_error = None;
    for delay_ms in [0u64, 250, 750, 1_500] {
        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        match client.post(url).json(&body).send().await {
            Ok(response) if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                last_error = Some(anyhow::anyhow!("backfill RPC rate limited"));
                continue;
            }
            Ok(response) => {
                if !response.status().is_success() {
                    anyhow::bail!("backfill RPC HTTP status {}", response.status());
                }
                let value: Value = response
                    .json()
                    .await
                    .context("failed to decode backfill RPC response")?;
                if let Some(err) = value.get("error") {
                    anyhow::bail!("backfill RPC error: {err}");
                }
                return Ok(value);
            }
            Err(err) => {
                let kind = if err.is_timeout() {
                    "timeout"
                } else if err.is_connect() {
                    "connection"
                } else {
                    "transport"
                };
                last_error = Some(anyhow::anyhow!("backfill RPC {kind} failure"));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("backfill RPC request failed")))
}

async fn transaction_subscribe_once(
    config: &StreamConfig,
    out: mpsc::Sender<StreamEvent>,
) -> Result<()> {
    let (mut ws, _) = connect_async(&config.ws_url)
        .await
        .context("connect transactionSubscribe websocket")?;

    let sub = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "transactionSubscribe",
        "params": [
            {
                "accountInclude": config.account_include,
                "vote": false,
                "failed": false
            },
            {
                "commitment": config.commitment,
                "encoding": "jsonParsed",
                "transactionDetails": "full"
            }
        ]
    });
    ws.send(Message::Text(sub.to_string().into())).await?;
    let subscription = subscription_ack(&mut ws, "transactionSubscribe").await?;
    info!(
        "confirmed transactionSubscribe subscription={} accounts={:?}",
        subscription, config.account_include
    );

    while let Some(msg) = ws.next().await {
        let msg = msg?;
        if !msg.is_text() {
            continue;
        }
        let value: Value = serde_json::from_str(&msg.into_text()?)?;
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if method != "transactionNotification" && method != "transactionSubscribe" {
            continue;
        }
        if let Some(event) = parse_transaction_event(value, "transactionSubscribe") {
            if out.send(event).await.is_err() {
                break;
            }
        }
    }

    Ok(())
}

async fn logs_subscribe_once(
    ws_url: &str,
    commitment: &str,
    mention: &str,
    out: mpsc::Sender<StreamEvent>,
) -> Result<()> {
    let (mut ws, _) = connect_async(ws_url)
        .await
        .with_context(|| format!("connect logsSubscribe mention={mention}"))?;
    let sub = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [
            { "mentions": [mention] },
            { "commitment": commitment }
        ]
    });
    ws.send(Message::Text(sub.to_string().into())).await?;
    let subscription = subscription_ack(&mut ws, "logsSubscribe").await?;
    info!("confirmed logsSubscribe subscription={subscription} mention={mention}");

    while let Some(msg) = ws.next().await {
        let msg = msg?;
        if !msg.is_text() {
            continue;
        }
        let value: Value = serde_json::from_str(&msg.into_text()?)?;
        if value.get("method").and_then(Value::as_str) != Some("logsNotification") {
            continue;
        }
        if let Some(event) = parse_logs_event(value, mention) {
            if out.send(event).await.is_err() {
                break;
            }
        }
    }

    Ok(())
}

async fn subscription_ack<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    label: &str,
) -> Result<String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let message = tokio::time::timeout(std::time::Duration::from_secs(10), ws.next())
        .await
        .with_context(|| format!("{label} acknowledgement timed out"))?
        .context("websocket closed before subscription acknowledgement")??;
    let text = message
        .into_text()
        .with_context(|| format!("{label} acknowledgement was not text"))?;
    let value: Value =
        serde_json::from_str(&text).with_context(|| format!("invalid {label} acknowledgement"))?;
    if let Some(error) = value.get("error") {
        anyhow::bail!("{label} rejected by RPC: {error}");
    }
    value
        .get("result")
        .map(Value::to_string)
        .context(format!("{label} acknowledgement missing result"))
}

fn parse_transaction_event(value: Value, source: &str) -> Option<StreamEvent> {
    let result = value.pointer("/params/result")?;
    let signature = result
        .pointer("/transaction/signatures/0")
        .or_else(|| result.pointer("/signature"))
        .and_then(Value::as_str)?
        .to_string();
    let slot = result
        .pointer("/context/slot")
        .or_else(|| result.pointer("/slot"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let logs = result
        .pointer("/transaction/meta/logMessages")
        .or_else(|| result.pointer("/meta/logMessages"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    Some(StreamEvent {
        source: source.to_string(),
        signature,
        slot,
        received_at_ms: current_time_ms(),
        logs,
        raw: value,
    })
}

fn parse_logs_event(value: Value, mention: &str) -> Option<StreamEvent> {
    let result = value.pointer("/params/result")?;
    let slot = result
        .pointer("/context/slot")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let event = result.pointer("/value")?;
    if !event.pointer("/err").is_none_or(Value::is_null) {
        return None;
    }
    let signature = event.get("signature")?.as_str()?.to_string();
    let logs = event
        .get("logs")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    Some(StreamEvent {
        source: format!("logsSubscribe:{mention}"),
        signature,
        slot,
        received_at_ms: current_time_ms(),
        logs,
        raw: value,
    })
}

#[cfg(test)]
mod tests {
    use super::transaction_subscribe_unavailable;

    #[test]
    fn classifies_free_plan_rejection_as_permanent() {
        let err = anyhow::anyhow!(
            "transactionSubscribe rejected by RPC: {{\"code\":-32600,\"message\":\"transactionSubscribe is not available on the free plan\"}}"
        );
        assert!(transaction_subscribe_unavailable(&err));
    }

    #[test]
    fn keeps_transient_disconnects_retryable() {
        let err = anyhow::anyhow!("connect transactionSubscribe websocket: connection reset");
        assert!(!transaction_subscribe_unavailable(&err));
    }
}
