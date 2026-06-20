//! Jupiter swap fallback for selling pump.fun mints.
//!
//! The primary exit is the local `sell_v2` build in `live.rs` (faster:
//! builds from cached curve state, no quote round-trip). When configured,
//! live execution races this sell quote/swap against local `sell_v2` so
//! whichever valid exit lands first can close the position.
//!
//! Requires an authenticated `api.jup.ag` key. The public lite endpoint
//! excludes pre-graduation pump.fun bonding-curve mints; the paid
//! Ultra/Metis endpoint routes them.

#![cfg(feature = "live-executor")]

use anyhow::{bail, Context, Result};
use base64::Engine;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    transaction::VersionedTransaction,
};
use std::time::Duration;

use crate::types::{ExecutionReport, ExecutionStatus};

/// Wrapped SOL mint — the Jupiter swap output.
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const QUOTE_URL: &str = "https://api.jup.ag/swap/v1/quote";
const SWAP_URL: &str = "https://api.jup.ag/swap/v1/swap";
const DEFAULT_PRIORITY_MAX_LAMPORTS: u64 = 1_000_000;
const DEFAULT_MAX_ACCOUNTS: u64 = 64;

/// Sell `amount_raw` base units of `mint` for SOL through Jupiter.
/// Thin wrapper over [`jupiter_swap_sell`] preserving the original
/// fallback-sell report shape.
#[allow(clippy::too_many_arguments)]
pub async fn jupiter_sell(
    rpc: &RpcClient,
    keypair: &Keypair,
    user: Pubkey,
    mint: &str,
    amount_raw: u64,
    api_key: &str,
    slippage_bps: u32,
    timeout: Duration,
) -> Result<ExecutionReport> {
    jupiter_swap_sell(
        rpc,
        keypair,
        user,
        mint,
        amount_raw,
        api_key,
        slippage_bps,
        timeout,
    )
    .await
}

/// Quote, build, sign, broadcast, and best-effort confirm a Jupiter sell
/// from `mint` into WSOL. Returns an `ExecutionReport` shaped like the
/// local execution paths so the caller can journal it uniformly.
///
/// `timeout` bounds each network leg (quote, swap-build, broadcast) and
/// the confirmation wait.
#[allow(clippy::too_many_arguments)]
async fn jupiter_swap_sell(
    rpc: &RpcClient,
    keypair: &Keypair,
    user: Pubkey,
    mint: &str,
    amount_raw: u64,
    api_key: &str,
    slippage_bps: u32,
    timeout: Duration,
) -> Result<ExecutionReport> {
    let started = std::time::Instant::now();
    if amount_raw == 0 {
        bail!("jupiter_sell: refusing to sell 0 tokens");
    }
    let http = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .context("build jupiter http client")?;

    // 1. Quote mint -> WSOL with ExactIn semantics.
    let quote = fetch_quote(&http, mint, WSOL_MINT, amount_raw, slippage_bps, api_key)
        .await
        .context("jupiter quote")?;
    validate_sell_quote(&quote, mint).context("jupiter sell quote validation")?;
    let out_amount =
        parse_out_amount(&quote).context("jupiter quote missing/parsable outAmount")?;
    if out_amount == 0 {
        bail!("jupiter quote returned zero SOL output");
    }

    // 2. Build the swap transaction for our wallet.
    let swap_b64 = fetch_swap_transaction(&http, &quote, user, api_key)
        .await
        .context("jupiter swap build")?;

    // 3. Decode, re-sign with our keypair, broadcast.
    let signed = decode_and_sign(&swap_b64, keypair).context("jupiter sign swap tx")?;
    let signature = send_signed(rpc, &signed, timeout)
        .await
        .context("jupiter broadcast")?;

    // 4. Best-effort confirmation. The signature is already returned, so
    //    even on a confirmation timeout we report LiveSubmitted rather
    //    than losing the in-flight tx.
    let status = confirm(rpc, &signature, timeout).await;
    let (exec_status, error) = match status {
        Ok(()) => (ExecutionStatus::LiveConfirmed, None),
        Err(err) => (
            ExecutionStatus::LiveSubmitted,
            Some(format!("jupiter submitted but confirmation failed: {err}")),
        ),
    };

    Ok(ExecutionReport {
        order_id: format!("jupiter-race-sell-{mint}"),
        signature: Some(signature.to_string()),
        quote_slot: None,
        status: exec_status,
        requested_lamports: 0,
        filled_lamports: Some(out_amount),
        filled_token_amount_raw: Some(u128::from(amount_raw)),
        fee_lamports: None,
        error,
        latency_ms: Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
    })
}

async fn fetch_quote(
    http: &reqwest::Client,
    input_mint: &str,
    output_mint: &str,
    amount_raw: u64,
    slippage_bps: u32,
    api_key: &str,
) -> Result<serde_json::Value> {
    let resp = http
        .get(QUOTE_URL)
        .header("x-api-key", api_key)
        .query(&[
            ("inputMint", input_mint),
            ("outputMint", output_mint),
            ("amount", &amount_raw.to_string()),
            ("slippageBps", &slippage_bps.to_string()),
            ("swapMode", "ExactIn"),
            ("restrictIntermediateTokens", "true"),
            ("onlyDirectRoutes", "false"),
            ("instructionVersion", "V2"),
            ("maxAccounts", &DEFAULT_MAX_ACCOUNTS.to_string()),
        ])
        .send()
        .await
        .context("send quote request")?;
    let status = resp.status();
    let body = resp.text().await.context("read quote body")?;
    if !status.is_success() {
        bail!("jupiter quote HTTP {status}: {}", truncate(&body, 300));
    }
    serde_json::from_str(&body).context("parse quote json")
}

async fn fetch_swap_transaction(
    http: &reqwest::Client,
    quote: &serde_json::Value,
    user: Pubkey,
    api_key: &str,
) -> Result<String> {
    let payload = serde_json::json!({
        "quoteResponse": quote,
        "userPublicKey": user.to_string(),
        "wrapAndUnwrapSol": true,
        "dynamicComputeUnitLimit": true,
        "dynamicSlippage": true,
        "prioritizationFeeLamports": {
            "priorityLevelWithMaxLamports": {
                "maxLamports": jupiter_priority_max_lamports(),
                "priorityLevel": "veryHigh"
            }
        },
    });
    let resp = http
        .post(SWAP_URL)
        .header("x-api-key", api_key)
        .json(&payload)
        .send()
        .await
        .context("send swap request")?;
    let status = resp.status();
    let body = resp.text().await.context("read swap body")?;
    if !status.is_success() {
        bail!("jupiter swap HTTP {status}: {}", truncate(&body, 300));
    }
    let parsed: serde_json::Value = serde_json::from_str(&body).context("parse swap json")?;
    if let Some(sim_error) = parsed.get("simulationError").filter(|v| !v.is_null()) {
        bail!(
            "jupiter swap simulationError: {}",
            truncate(&sim_error.to_string(), 300)
        );
    }
    parsed
        .get("swapTransaction")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .context("swap response missing swapTransaction")
}

fn validate_sell_quote(quote: &serde_json::Value, mint: &str) -> Result<()> {
    let input = quote
        .get("inputMint")
        .and_then(|v| v.as_str())
        .context("quote missing inputMint")?;
    if input != mint {
        bail!("quote inputMint mismatch: expected {mint}, got {input}");
    }
    let output = quote
        .get("outputMint")
        .and_then(|v| v.as_str())
        .context("quote missing outputMint")?;
    if output != WSOL_MINT {
        bail!("quote outputMint mismatch: expected WSOL, got {output}");
    }
    let route_len = quote
        .get("routePlan")
        .and_then(|v| v.as_array())
        .map(|routes| routes.len())
        .unwrap_or(0);
    if route_len == 0 {
        bail!("quote has no routePlan");
    }
    Ok(())
}

/// Parse the quote's `outAmount` (a decimal string of lamports).
fn parse_out_amount(quote: &serde_json::Value) -> Result<u64> {
    quote
        .get("outAmount")
        .and_then(|v| v.as_str())
        .context("no outAmount field")?
        .parse::<u64>()
        .context("outAmount not a u64")
}

fn jupiter_priority_max_lamports() -> u64 {
    crate::config::env_lookup("MAYHEM_LIVE_JUPITER_PRIORITY_MAX_LAMPORTS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_PRIORITY_MAX_LAMPORTS)
}

/// Base64-decode the Jupiter swap tx, deserialize it as a v0
/// `VersionedTransaction`, and re-sign its message with our keypair.
/// Jupiter returns the tx with a placeholder signature; re-signing with
/// `try_new` produces the correct single-signer signature for our wallet.
fn decode_and_sign(swap_b64: &str, keypair: &Keypair) -> Result<VersionedTransaction> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(swap_b64.trim())
        .context("base64 decode swapTransaction")?;
    let unsigned: VersionedTransaction =
        bincode::deserialize(&bytes).context("bincode deserialize VersionedTransaction")?;
    let signed = VersionedTransaction::try_new(unsigned.message, &[keypair])
        .context("re-sign jupiter swap message")?;
    Ok(signed)
}

async fn send_signed(
    rpc: &RpcClient,
    tx: &VersionedTransaction,
    timeout: Duration,
) -> Result<Signature> {
    // Match the panic-sell send pattern: skip preflight, no client-side
    // retries — this is a last resort and we want it out fast.
    let config = RpcSendTransactionConfig {
        skip_preflight: true,
        max_retries: Some(0),
        preflight_commitment: None,
        ..RpcSendTransactionConfig::default()
    };
    match tokio::time::timeout(timeout, rpc.send_transaction_with_config(tx, config)).await {
        Ok(result) => result.context("jupiter send_transaction_with_config"),
        Err(_) => bail!(
            "jupiter broadcast timeout after {}ms; tx state ambiguous",
            timeout.as_millis()
        ),
    }
}

/// Poll signature status until confirmed or the timeout elapses.
async fn confirm(rpc: &RpcClient, signature: &Signature, timeout: Duration) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout.max(Duration::from_millis(1));
    let commitment = CommitmentConfig::confirmed();
    let poll = Duration::from_millis(150);
    loop {
        match rpc
            .get_signature_statuses(std::slice::from_ref(signature))
            .await
        {
            Ok(resp) => {
                if let Some(status) = resp.value.into_iter().next().flatten() {
                    if let Some(err) = status.err {
                        bail!("jupiter swap failed on-chain: {err:?}");
                    }
                    if status.satisfies_commitment(commitment) {
                        return Ok(());
                    }
                }
            }
            Err(err) => {
                if std::time::Instant::now() >= deadline {
                    bail!("jupiter confirmation status error: {err}");
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            bail!(
                "jupiter confirmation timeout after {}ms",
                timeout.as_millis()
            );
        }
        tokio::time::sleep(poll).await;
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Signer;

    #[test]
    fn parses_out_amount_from_quote_json() {
        let quote: serde_json::Value = serde_json::from_str(
            r#"{"inputMint":"abc","outAmount":"123456789","otherAmountThreshold":"1"}"#,
        )
        .unwrap();
        assert_eq!(parse_out_amount(&quote).unwrap(), 123_456_789);
    }

    #[test]
    fn out_amount_missing_is_error() {
        let quote: serde_json::Value = serde_json::from_str(r#"{"inputMint":"abc"}"#).unwrap();
        assert!(parse_out_amount(&quote).is_err());
    }

    #[test]
    fn out_amount_non_numeric_is_error() {
        let quote: serde_json::Value =
            serde_json::from_str(r#"{"outAmount":"not-a-number"}"#).unwrap();
        assert!(parse_out_amount(&quote).is_err());
    }

    #[test]
    fn decode_and_sign_round_trips_a_known_tx() {
        // Build a minimal v0 transaction, serialize it the way Jupiter
        // would (base64 of bincode), then ensure decode_and_sign returns
        // a tx whose message round-trips and that carries one signature.
        use solana_sdk::message::{v0, VersionedMessage};
        let payer = Keypair::new();
        let msg = VersionedMessage::V0(
            v0::Message::try_compile(&payer.pubkey(), &[], &[], Default::default()).unwrap(),
        );
        let unsigned = VersionedTransaction {
            signatures: vec![Signature::default()],
            message: msg.clone(),
        };
        let bytes = bincode::serialize(&unsigned).unwrap();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);

        let signed = decode_and_sign(&b64, &payer).unwrap();
        assert_eq!(signed.message, msg);
        assert_eq!(signed.signatures.len(), 1);
        assert_ne!(signed.signatures[0], Signature::default());
        assert!(signed.verify_with_results().into_iter().all(|ok| ok));
    }

    #[test]
    fn validates_sell_quote_direction_and_route() {
        let quote: serde_json::Value = serde_json::from_str(
            r#"{
                "inputMint":"TargetMint111",
                "outputMint":"So11111111111111111111111111111111111111112",
                "outAmount":"123",
                "routePlan":[{"percent":100}]
            }"#,
        )
        .unwrap();
        validate_sell_quote(&quote, "TargetMint111").unwrap();
    }

    #[test]
    fn parses_zero_out_amount_for_caller_validation() {
        let quote: serde_json::Value = serde_json::from_str(r#"{"outAmount":"0"}"#).unwrap();
        assert_eq!(parse_out_amount(&quote).unwrap(), 0);
    }
}
