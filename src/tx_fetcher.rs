use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::{json, Value};

#[derive(Clone)]
pub struct TxFetcher {
    client: Client,
    rpc_url: String,
}

impl TxFetcher {
    pub fn new(rpc_url: String) -> Self {
        Self {
            client: Client::new(),
            rpc_url,
        }
    }

    pub async fn get_transaction(&self, signature: &str) -> Result<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        });

        let resp = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(|err| {
                let kind = if err.is_timeout() {
                    "timeout"
                } else if err.is_connect() {
                    "connection"
                } else {
                    "transport"
                };
                anyhow::anyhow!("getTransaction HTTP {kind} failure")
            })?;

        let status = resp.status();
        let value: Value = resp
            .json()
            .await
            .context("failed to parse getTransaction JSON")?;

        if !status.is_success() {
            anyhow::bail!("getTransaction HTTP status {} body={}", status, value);
        }

        if let Some(err) = value.get("error") {
            anyhow::bail!("getTransaction RPC error: {}", err);
        }

        Ok(value["result"].clone())
    }

    pub async fn get_slot(&self) -> Result<u64> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": [{ "commitment": "processed" }]
        });

        let resp = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(|err| {
                let kind = if err.is_timeout() {
                    "timeout"
                } else if err.is_connect() {
                    "connection"
                } else {
                    "transport"
                };
                anyhow::anyhow!("getSlot HTTP {kind} failure")
            })?;

        let status = resp.status();
        let value: Value = resp.json().await.context("failed to parse getSlot JSON")?;

        if !status.is_success() {
            anyhow::bail!("getSlot HTTP status {} body={}", status, value);
        }

        if let Some(err) = value.get("error") {
            anyhow::bail!("getSlot RPC error: {}", err);
        }

        value["result"]
            .as_u64()
            .context("getSlot result missing slot")
    }
}

pub fn extract_account_keys(tx: &Value) -> Vec<String> {
    tx.pointer("/transaction/message/accountKeys")
        .and_then(|v| v.as_array())
        .map(|keys| {
            keys.iter()
                .filter_map(|k| {
                    if let Some(s) = k.as_str() {
                        Some(s.to_string())
                    } else {
                        k.get("pubkey")
                            .and_then(|p| p.as_str())
                            .map(|s| s.to_string())
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn extract_token_mints(tx: &Value) -> Vec<String> {
    let mut out = std::collections::BTreeSet::new();

    for field in ["preTokenBalances", "postTokenBalances"] {
        if let Some(items) = tx
            .pointer(&format!("/meta/{}", field))
            .and_then(|v| v.as_array())
        {
            for item in items {
                if let Some(mint) = item.get("mint").and_then(|v| v.as_str()) {
                    out.insert(mint.to_string());
                }
            }
        }
    }

    out.into_iter().collect()
}

pub fn wallet_lamport_delta(tx: &Value, wallet: &str) -> Option<i64> {
    let keys = extract_account_keys(tx);
    let pre = tx.pointer("/meta/preBalances")?.as_array()?;
    let post = tx.pointer("/meta/postBalances")?.as_array()?;

    let mut delta = 0i64;
    let mut found = false;

    for (i, key) in keys.iter().enumerate() {
        if key == wallet {
            let a = pre.get(i)?.as_i64()?;
            let b = post.get(i)?.as_i64()?;
            delta += b - a;
            found = true;
        }
    }

    if found {
        Some(delta)
    } else {
        None
    }
}
